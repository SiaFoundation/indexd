package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"syscall"
	"time"

	"go.sia.tech/core/consensus"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/syncer"
	"go.sia.tech/coreutils/wallet"
	"go.sia.tech/indexd/build"
	"go.sia.tech/indexd/config"
	"go.sia.tech/indexd/persist/postgres"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"lukechampine.com/flagg"
	"lukechampine.com/upnp"
)

const (
	indexdAdminPasswordEnvVar = "INDEXD_ADMIN_PASSWORD"
	configFileEnvVar          = "INDEXD_CONFIG_FILE"
	dataDirEnvVar             = "INDEXD_DATA_DIR"
)

var cfg = config.Config{
	Directory: os.Getenv(dataDirEnvVar), // default to env variable
	AdminAPI: config.AdminAPI{
		Address:  "127.0.0.1:9980",
		Password: os.Getenv(indexdAdminPasswordEnvVar),
	},
	ApplicationAPI: config.ApplicationAPI{
		Address: ":9982",
	},
	Syncer: config.Syncer{
		Address:   ":9981",
		Bootstrap: true,
	},
	Consensus: config.Consensus{
		Network:        "mainnet",
		IndexBatchSize: 1000,
	},
	Explorer: config.Explorer{
		Enabled: true,
		URL:     "https://api.siascan.com",
	},
	Database: postgres.ConnectionInfo{
		Host:     "127.0.0.1",
		Port:     5432,
		User:     "postgres",
		Password: "changeme",
		Database: "indexd",
		SSLMode:  "verify-full",
	},
	Log: config.Log{
		File: config.FileLog{
			Level:   zap.NewAtomicLevelAt(zapcore.InfoLevel), // the zero-value is Info, but better to be explicit
			Enabled: true,
			Format:  "json",
		},
		StdOut: config.StdOutLog{
			Level:      zap.NewAtomicLevelAt(zapcore.InfoLevel), // the zero-value is Info, but better to be explicit
			Enabled:    true,
			Format:     "human",
			EnableANSI: runtime.GOOS != "windows",
		},
	},
}

// checkFatalError prints an error message to stderr and exits with a 1 exit code. If err is nil, this is a no-op.
func checkFatalError(context string, err error) {
	if err == nil {
		return
	}
	os.Stderr.WriteString(fmt.Sprintf("%s: %s\n", context, err))
	os.Exit(1)
}

// jsonEncoder returns a zapcore.Encoder that encodes logs as JSON intended for
// parsing.
func jsonEncoder() zapcore.Encoder {
	cfg := zap.NewProductionEncoderConfig()
	cfg.EncodeTime = zapcore.RFC3339TimeEncoder
	return zapcore.NewJSONEncoder(cfg)
}

// humanEncoder returns a zapcore.Encoder that encodes logs as human-readable
// text.
func humanEncoder(showColors bool) zapcore.Encoder {
	cfg := zap.NewProductionEncoderConfig()
	cfg.TimeKey = "" // prevent duplicate timestamps
	cfg.EncodeTime = zapcore.RFC3339TimeEncoder
	cfg.EncodeDuration = zapcore.StringDurationEncoder

	if showColors {
		cfg.EncodeLevel = zapcore.CapitalColorLevelEncoder
	} else {
		cfg.EncodeLevel = zapcore.CapitalLevelEncoder
	}

	cfg.StacktraceKey = ""
	cfg.CallerKey = ""
	return zapcore.NewConsoleEncoder(cfg)
}

func tryConfigPaths() []string {
	if str := os.Getenv(configFileEnvVar); str != "" {
		return []string{str}
	}

	paths := []string{
		"indexd.yml",
	}
	if str := os.Getenv(dataDirEnvVar); str != "" {
		paths = append(paths, filepath.Join(str, "indexd.yml"))
	}

	switch runtime.GOOS {
	case "windows":
		paths = append(paths, filepath.Join(os.Getenv("APPDATA"), "indexd", "indexd.yml"))
	case "darwin":
		paths = append(paths, filepath.Join(os.Getenv("HOME"), "Library", "Application Support", "indexd", "indexd.yml"))
	case "linux", "freebsd", "openbsd":
		paths = append(paths,
			filepath.Join(string(filepath.Separator), "etc", "indexd", "indexd.yml"),
			filepath.Join(string(filepath.Separator), "var", "lib", "indexd", "indexd.yml"), // old default for the Linux service
		)
	}
	return paths
}

func dataDirectory(fp string) string {
	// use the provided path if it's not empty
	if fp != "" {
		return fp
	}

	// default to the operating system's application directory
	switch runtime.GOOS {
	case "windows":
		return filepath.Join(os.Getenv("APPDATA"), "indexd")
	case "darwin":
		return filepath.Join(os.Getenv("HOME"), "Library", "Application Support", "indexd")
	case "linux", "freebsd", "openbsd":
		return filepath.Join(string(filepath.Separator), "var", "lib", "indexd")
	default:
		return "."
	}
}

// tryLoadConfig tries to load the config file. It will try multiple locations
// based on GOOS starting with PWD/indexd.yml. If the file does not exist, it will
// try the next location. If an error occurs while loading the file, it will
// print the error and exit. If the config is successfully loaded, the path to
// the config file is returned.
func tryLoadConfig() string {
	for _, fp := range tryConfigPaths() {
		if err := config.LoadFile(fp, &cfg); err == nil {
			return fp
		} else if !errors.Is(err, os.ErrNotExist) {
			checkFatalError("failed to load config file", err)
		}
	}
	return ""
}

func setupUPNP(ctx context.Context, port uint16, log *zap.Logger) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	d, err := upnp.Discover(ctx)
	if err != nil {
		return "", fmt.Errorf("couldn't discover UPnP router: %w", err)
	} else if !d.IsForwarded(port, "TCP") {
		if err := d.Forward(uint16(port), "TCP", "walletd"); err != nil {
			log.Debug("couldn't forward port", zap.Error(err))
		} else {
			log.Debug("upnp: forwarded p2p port", zap.Uint16("port", port))
		}
	}
	return d.ExternalIP()
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// attempt to load the config file, command line flags will override any
	// values set in the config file
	configPath := tryLoadConfig()

	// determine the data directory
	cfg.Directory = dataDirectory(cfg.Directory)

	var logLevelOverride string

	rootCmd := flagg.Root
	rootCmd.Usage = flagg.SimpleUsage(rootCmd, ``)
	rootCmd.StringVar(&cfg.Directory, "dir", cfg.Directory, "directory to store indexd metadata in")
	rootCmd.StringVar(&cfg.AdminAPI.Address, "api.admin", cfg.AdminAPI.Address, "address to serve admin API on")
	rootCmd.StringVar(&cfg.ApplicationAPI.Address, "api.app", cfg.ApplicationAPI.Address, "address to serve application API on")
	rootCmd.StringVar(&logLevelOverride, "log", "", "overrides the log level for all enabled loggers (debug, info, warn, error)")

	versionCmd := flagg.New("version", ``)
	seedCmd := flagg.New("seed", ``)
	configCmd := flagg.New("config", ``)

	cmd := flagg.Parse(flagg.Tree{
		Cmd: rootCmd,
		Sub: []flagg.Tree{
			{Cmd: versionCmd},
			{Cmd: seedCmd},
			{Cmd: configCmd},
		},
	})

	if logLevelOverride != "" {
		level, err := zap.ParseAtomicLevel(logLevelOverride)
		checkFatalError("failed to parse log level", err)
		cfg.Log.File.Level = level
		cfg.Log.StdOut.Level = level
	}

	switch cmd {
	case versionCmd:
		if len(cmd.Args()) != 0 {
			cmd.Usage()
			return
		}

		fmt.Println("indexd", build.Version())
		fmt.Println("Commit:", build.Commit())
		fmt.Println("Build Date:", build.Time())
	case seedCmd:
		if len(cmd.Args()) != 0 {
			cmd.Usage()
			return
		}

		var seed [32]byte
		phrase := wallet.NewSeedPhrase()
		checkFatalError("failed to generate seed", wallet.SeedFromPhrase(&seed, phrase))
		key := wallet.KeyFromSeed(&seed, 0)
		fmt.Println("Recovery Phrase:", phrase)
		fmt.Println("Address", types.StandardUnlockHash(key.PublicKey()))
	case configCmd:
		if len(cmd.Args()) != 0 {
			cmd.Usage()
			return
		}

		runConfigCmd(configPath)
	case rootCmd:
		if len(cmd.Args()) != 0 {
			cmd.Usage()
			return
		}

		if cfg.AdminAPI.Password == "" {
			os.Stderr.WriteString(fmt.Sprintf("missing admin password - needs to be set either via config file or '%s' env var\n", indexdAdminPasswordEnvVar))
			os.Exit(1)
		}

		var seed [32]byte
		checkFatalError("failed to load wallet seed", wallet.SeedFromPhrase(&seed, cfg.RecoveryPhrase))
		walletKey := wallet.KeyFromSeed(&seed, 0)

		if cfg.Directory != "" {
			// create the data directory if it does not already exist
			if err := os.MkdirAll(cfg.Directory, 0700); err != nil {
				checkFatalError("failed to create data directory", err)
			}
		}

		var logCores []zapcore.Core

		if cfg.Log.StdOut.Enabled {
			var encoder zapcore.Encoder
			switch cfg.Log.StdOut.Format {
			case "json":
				encoder = jsonEncoder()
			default:
				encoder = humanEncoder(cfg.Log.StdOut.EnableANSI)
			}

			// create the stdout logger
			logCores = append(logCores, zapcore.NewCore(encoder, zapcore.Lock(os.Stdout), cfg.Log.StdOut.Level))
		}

		if cfg.Log.File.Enabled {
			// normalize log path
			if cfg.Log.File.Path == "" {
				cfg.Log.File.Path = filepath.Join(cfg.Directory, "indexd.log")
			}

			// configure file logging
			var encoder zapcore.Encoder
			switch cfg.Log.File.Format {
			case "json":
				encoder = jsonEncoder()
			default:
				encoder = humanEncoder(false) // disable colors in file log
			}

			fileWriter, closeFn, err := zap.Open(cfg.Log.File.Path)
			checkFatalError("failed to open log file", err)
			defer closeFn()

			// create the file logger
			logCores = append(logCores, zapcore.NewCore(encoder, zapcore.Lock(fileWriter), cfg.Log.File.Level))
		}

		if cfg.Syncer.Bootstrap {
			switch cfg.Consensus.Network {
			case "mainnet":
				cfg.Syncer.Peers = append(cfg.Syncer.Peers, syncer.MainnetBootstrapPeers...)
			case "zen":
				cfg.Syncer.Peers = append(cfg.Syncer.Peers, syncer.ZenBootstrapPeers...)
			case "anagami":
				cfg.Syncer.Peers = append(cfg.Syncer.Peers, syncer.AnagamiBootstrapPeers...)
			}
		}

		var network *consensus.Network
		var genesis types.Block
		switch cfg.Consensus.Network {
		case "mainnet":
			network, genesis = chain.Mainnet()
		case "zen":
			network, genesis = chain.TestnetZen()
		case "anagami":
			network, genesis = chain.TestnetAnagami()
		default:
			checkFatalError("invalid network", errors.New("must be one of 'mainnet', 'zen' or 'anagami'"))
		}

		var log *zap.Logger
		if len(logCores) == 1 {
			log = zap.New(logCores[0], zap.AddCaller())
		} else {
			log = zap.New(zapcore.NewTee(logCores...), zap.AddCaller())
		}
		defer log.Sync()
		zap.RedirectStdLog(log.Named("stdlib"))

		checkFatalError("daemon startup failed", runRootCmd(ctx, cfg, walletKey, network, genesis, log))
	}
}
