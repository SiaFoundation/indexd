package config

import (
	"bytes"
	"fmt"
	"os"

	"go.sia.tech/indexd/persist/postgres"
	"go.uber.org/zap"
	"gopkg.in/yaml.v3"
)

type (
	// AdminAPI contains the configuration for the HTTP server serving the admin
	// UI and routes.
	AdminAPI struct {
		Address  string `yaml:"address"`
		Password string `yaml:"password"`
	}

	// ApplicationAPI contains the configuration for the HTTP server serving the
	// application API
	ApplicationAPI struct {
		Address string `yaml:"address"`
		// AdvertiseURL can be used to override the URL
		// that is generated for auth requests and
		// the hostname that is valid for signed
		// requests.
		AdvertiseURL string `yaml:"advertiseURL"`
	}

	// Syncer contains the configuration for the p2p syncer.
	Syncer struct {
		Address    string   `yaml:"address"`
		Bootstrap  bool     `yaml:"bootstrap"`
		EnableUPnP bool     `yaml:"enableUPnP"`
		Peers      []string `yaml:"peers"`
	}

	// Consensus contains the configuration for the consensus set.
	Consensus struct {
		Network        string `yaml:"network"`
		IndexBatchSize int    `yaml:"indexBatchSize"`
		// PruneTarget is the target number of blocks to keep when pruning
		// old blocks from the consensus database. A value of 0 disables
		// pruning. Must be at least 6 hours of blocks if enabled.
		PruneTarget uint64 `yaml:"pruneTarget,omitempty"`
	}

	// Explorer contains the configuration for an external explorer.
	Explorer struct {
		Enabled bool   `yaml:"enabled"`
		URL     string `yaml:"url"`
	}

	// Slabs contains the configuration for the slab manager.
	Slabs struct {
		// MigrationWorkers is the number of slabs to migrate in parallel. If
		// zero, defaults to runtime.NumCPU().
		MigrationWorkers int `yaml:"migrationWorkers"`
	}

	// FileLog configures the file output of the logger.
	FileLog struct {
		Enabled bool            `yaml:"enabled"`
		Level   zap.AtomicLevel `yaml:"level"`
		Format  string          `yaml:"format"`
		// Path is the path of the log file.
		Path string `yaml:"path"`
	}

	// StdOutLog configures the standard output of the logger.
	StdOutLog struct {
		Level      zap.AtomicLevel `yaml:"level"`
		Enabled    bool            `yaml:"enabled"`
		Format     string          `yaml:"format"`
		EnableANSI bool            `yaml:"enableANSI"` //nolint:tagliatelle
	}

	// Log contains the configuration for the logger.
	Log struct {
		StdOut StdOutLog `yaml:"stdout"`
		File   FileLog   `yaml:"file"`
	}

	// Config contains the configuration for the indexer
	Config struct {
		AutoOpenWebUI  bool   `yaml:"autoOpenWebUI"`
		Directory      string `yaml:"directory"`
		RecoveryPhrase string `yaml:"recoveryPhrase"`
		Debug          bool   `yaml:"debug"`

		AdminAPI       AdminAPI                `yaml:"adminAPI"`
		ApplicationAPI ApplicationAPI          `yaml:"applicationAPI"`
		Syncer         Syncer                  `yaml:"syncer"`
		Consensus      Consensus               `yaml:"consensus"`
		Explorer       Explorer                `yaml:"explorer"`
		Slabs          Slabs                   `yaml:"slabs"`
		Log            Log                     `yaml:"log"`
		Database       postgres.ConnectionInfo `yaml:"database"`
	}
)

// LoadFile loads the configuration from the provided file path.
// If the file does not exist or cannot be decoded, an error is returned.
func LoadFile(fp string, cfg *Config) error {
	buf, err := os.ReadFile(fp)
	if err != nil {
		return fmt.Errorf("failed to read config file: %w", err)
	}

	r := bytes.NewReader(buf)
	dec := yaml.NewDecoder(r)
	dec.KnownFields(true)

	return dec.Decode(cfg)
}
