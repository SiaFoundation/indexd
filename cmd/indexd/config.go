package main

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/wallet"
	"golang.org/x/term"
	"gopkg.in/yaml.v3"
)

func runConfigCmd(fp string) {
	fmt.Println("indexd Configuration Wizard")
	fmt.Println("This wizard will help you configure indexd for the first time.")
	fmt.Println("You can always change these settings with the config command or by editing the config file.")

	if fp == "" {
		fp = configPath()
	}
	fp, err := filepath.Abs(fp)
	checkFatalError("failed to get absolute path of config file", err)

	fmt.Println("")
	fmt.Printf("Config Location %q\n", fp)

	if _, err := os.Stat(fp); err == nil {
		if !promptYesNo(fmt.Sprintf("%q already exists. Would you like to overwrite it?", fp)) {
			return
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		checkFatalError("failed to check if config file exists", err)
	} else {
		// ensure the config directory exists
		checkFatalError("failed to create config directory", os.MkdirAll(filepath.Dir(fp), 0700))
	}

	fmt.Println("")
	setDataDirectory()

	fmt.Println("")
	if cfg.RecoveryPhrase != "" {
		fmt.Println(ansiStyle("33m", "A wallet seed phrase is already set."))
		fmt.Println("If you change your wallet seed phrase, your indexer will not be able to access Siacoin associated with this wallet.")
		fmt.Println("Ensure that you have backed up your wallet seed phrase before continuing.")
		if promptYesNo("Would you like to change your wallet seed phrase?") {
			setSeedPhrase()
		}
	} else {
		setSeedPhrase()
	}

	fmt.Println("")
	if cfg.AdminAPI.Password != "" {
		fmt.Println(ansiStyle("33m", "An admin password is already set."))
		fmt.Println("If you change your admin password, you will need to update any scripts or applications that use the admin API.")
		if promptYesNo("Would you like to change your admin password?") {
			setAPIPassword()
		}
	} else {
		setAPIPassword()
	}

	setAdvancedConfig()

	// write the config file
	f, err := os.Create(fp)
	checkFatalError("failed to create config file", err)
	defer f.Close()

	enc := yaml.NewEncoder(f)
	defer enc.Close()

	checkFatalError("failed to encode config file", enc.Encode(cfg))
	checkFatalError("failed to sync config file", f.Sync())
}

func configPath() string {
	if str := os.Getenv(configFileEnvVar); str != "" {
		return str
	}

	switch runtime.GOOS {
	case "windows":
		return filepath.Join(os.Getenv("APPDATA"), "indexd", "indexd.yml")
	case "darwin":
		return filepath.Join(os.Getenv("HOME"), "Library", "Application Support", "indexd", "indexd.yml")
	case "linux", "freebsd", "openbsd":
		return filepath.Join(string(filepath.Separator), "etc", "indexd", "indexd.yml")
	default:
		return "indexd.yml"
	}
}

func humanList(s []string, sep string) string {
	if len(s) == 0 {
		return ""
	} else if len(s) == 1 {
		return fmt.Sprintf(`%q`, s[0])
	} else if len(s) == 2 {
		return fmt.Sprintf(`%q %s %q`, s[0], sep, s[1])
	}

	var sb strings.Builder
	for i, v := range s {
		if i != 0 {
			sb.WriteString(", ")
		}
		if i == len(s)-1 {
			sb.WriteString("or ")
		}
		sb.WriteString(`"`)
		sb.WriteString(v)
		sb.WriteString(`"`)
	}
	return sb.String()
}

func promptQuestion(question string, answers []string) string {
	for {
		input := readInput(fmt.Sprintf("%s (%s)", question, strings.Join(answers, "/")))
		for _, answer := range answers {
			if strings.EqualFold(input, answer) {
				return answer
			}
		}
		fmt.Println(ansiStyle("31m", fmt.Sprintf("Answer must be %s", humanList(answers, "or"))))
	}
}

func promptYesNo(question string) bool {
	answer := promptQuestion(question, []string{"yes", "no"})
	return strings.EqualFold(answer, "yes")
}

func readInput(context string) string {
	fmt.Printf("%s: ", context)
	r := bufio.NewReader(os.Stdin)
	input, err := r.ReadString('\n')
	checkFatalError("failed to read input", err)
	return strings.TrimSpace(input)
}

// readPasswordInput reads a password from stdin.
func readPasswordInput(context string) string {
	fmt.Printf("%s: ", context)
	input, err := term.ReadPassword(int(os.Stdin.Fd()))
	checkFatalError("failed to read password input", err)
	fmt.Println("")
	return string(input)
}

// setAPIPassword prompts the user to enter an API password if one is not
// already set via environment variable or config file.
func setAPIPassword() {
	// retry until a valid API password is entered
	for {
		fmt.Println("Please choose a password to unlock indexd.")
		fmt.Println("This password will be required to access the admin UI in your web browser.")
		fmt.Println("(The password must be at least 4 characters.)")
		cfg.AdminAPI.Password = readPasswordInput("Enter password")
		if len(cfg.AdminAPI.Password) >= 4 {
			break
		}

		fmt.Println(ansiStyle("31m", "Password must be at least 4 characters!"))
		fmt.Println("")
	}
}

func setAdvancedConfig() {
	fmt.Println("")
	if !promptYesNo("Would you like to configure advanced settings?") {
		return
	}

	fmt.Println("")
	fmt.Println("Advanced settings are used to configure the indexer's behavior.")
	fmt.Println("You can leave these settings blank to use the defaults.")
	fmt.Println("")

	// http address of Admin API
	fmt.Println("The HTTP address is used to serve the indexer's admin API.")
	fmt.Println("The admin API is used to configure the indexer.")
	fmt.Println("It should only be exposed to the public internet via an https reverse proxy")
	setListenAddress("HTTP Address", &cfg.AdminAPI.Address)

	// http address of Application API
	fmt.Println("The HTTP address is used to serve the indexer's application API.")
	fmt.Println("The application API is used by applications to interact with the indexer.")
	fmt.Println("It should only be exposed to the public internet via an https reverse proxy")
	setListenAddress("HTTP Address", &cfg.ApplicationAPI.Address)

	// syncer address
	fmt.Println("")
	fmt.Println("The syncer address is used to exchange blocks with other nodes in the Sia network.")
	fmt.Println("It should be exposed publicly to improve the indexer's connectivity.")
	setListenAddress("Gateway Address", &cfg.Syncer.Address)

	// database user
	fmt.Println("")
	fmt.Println("The database user for the postgres connection.")
	setDatabaseUser()

	// database name
	fmt.Println("")
	fmt.Println("The database name for the postgres connection.")
	setDatabaseName()

	// database password
	fmt.Println("")
	fmt.Println("The database password for the postgres connection.")
	setDatabasePassword()

	// database address
	fmt.Println("")
	fmt.Println("The database address is used to connect to the indexer's database.")
	setDatabaseAddress()

	// database SSL mode
	fmt.Println("")
	fmt.Println("The database SSL mode is used to configure the database connection's encryption.")
	setDatabaseSSLMode()
}

func setDatabaseAddress() {
	current := fmt.Sprintf("%s:%d", cfg.Database.Host, cfg.Database.Port)
	if !promptYesNo("Would you like to change the database address? (Current: " + current + ")") {
		return
	}

	// will continue to prompt until a valid value is entered
	for {
		input := readInput(fmt.Sprintf("Enter new database address (currently %q)", current))
		if input == "" {
			return
		}

		host, port, err := net.SplitHostPort(input)
		if err != nil {
			stdoutError(fmt.Sprintf("Invalid database address %q: %s", input, err.Error()))
			continue
		}

		n, err := strconv.Atoi(port)
		if err != nil {
			stdoutError(fmt.Sprintf("Invalid database port %q: %s", port, err.Error()))
			continue
		} else if n < 0 || n > 65535 {
			stdoutError(fmt.Sprintf("Invalid database port %q: must be between 0 and 65535", input))
			continue
		}
		cfg.Database.Host = host
		cfg.Database.Port = n
		return
	}
}

func setDatabaseName() {
	if !promptYesNo("Would you like to change the database name? (Current: " + cfg.Database.Database + ")") {
		return
	}
	cfg.Database.Database = readInput("Enter new database name")
}

func setDatabasePassword() {
	if !promptYesNo("Would you like to change the database password?") {
		return
	}

	fmt.Println("The database password is used to connect to the indexer's database.")
	fmt.Println("It should be a strong password that is not used for any other purpose.")
	cfg.Database.Password = readPasswordInput("Enter new database password")
	if len(cfg.Database.Password) < 4 {
		fmt.Println(ansiStyle("31m", "Password must be at least 4 characters!"))
		setDatabasePassword()
		return
	}
}

func setDatabaseSSLMode() {
	if !promptYesNo("Would you like to change the database SSL mode? (Current: " + cfg.Database.SSLMode + ")") {
		return
	}
	cfg.Database.SSLMode = promptQuestion("Enter new database SSL mode", []string{"disable", "allow", "prefer", "require", "verify-ca", "verify-full"})
}

func setDatabaseUser() {
	if !promptYesNo("Would you like to change the database user? (Current: " + cfg.Database.User + ")") {
		return
	}
	cfg.Database.User = readInput("Enter new user")
}

func setDataDirectory() {
	if cfg.Directory == "" {
		cfg.Directory = "."
	}

	dir, err := filepath.Abs(cfg.Directory)
	checkFatalError("failed to get absolute path of data directory", err)

	fmt.Println("The data directory is where indexd will store its metadata and consensus data.")
	fmt.Println("This directory should be on a fast, reliable storage device, preferably an SSD.")
	fmt.Println("")

	_, existsErr := os.Stat(filepath.Join(cfg.Directory, "indexd.db"))
	dataExists := existsErr == nil
	if dataExists {
		fmt.Println(ansiStyle("33m", "There is existing data in the data directory."))
		fmt.Println(ansiStyle("33m", "If you change your data directory, you will need to manually move consensus, gateway, tpool, and indexd.db to the new directory."))
	}

	if !promptYesNo("Would you like to change the data directory? (Current: " + dir + ")") {
		return
	}
	cfg.Directory = readInput("Enter data directory")
}

func setListenAddress(context string, value *string) {
	// will continue to prompt until a valid value is entered
	for {
		input := readInput(fmt.Sprintf("%s (currently %q)", context, *value))
		if input == "" {
			return
		}

		host, port, err := net.SplitHostPort(input)
		if err != nil {
			stdoutError(fmt.Sprintf("Invalid %s port %q: %s", context, input, err.Error()))
			continue
		}

		n, err := strconv.Atoi(port)
		if err != nil {
			stdoutError(fmt.Sprintf("Invalid %s port %q: %s", context, input, err.Error()))
			continue
		} else if n < 0 || n > 65535 {
			stdoutError(fmt.Sprintf("Invalid %s port %q: must be between 0 and 65535", context, input))
			continue
		}
		*value = net.JoinHostPort(host, port)
		return
	}
}

func setSeedPhrase() {
	// retry until a valid seed phrase is entered
	for {
		fmt.Println("")
		fmt.Println("Type in your 12-word seed phrase and press enter. If you do not have a seed phrase yet, type 'seed' to generate one.")
		phrase := readPasswordInput("Enter seed phrase")

		if strings.ToLower(strings.TrimSpace(phrase)) == "seed" {
			// generate a new seed phrase
			var seed [32]byte
			phrase = wallet.NewSeedPhrase()
			if err := wallet.SeedFromPhrase(&seed, phrase); err != nil {
				panic(err)
			}
			key := wallet.KeyFromSeed(&seed, 0)
			fmt.Println("")
			fmt.Println("A new seed phrase has been generated below. " + ansiStyle("1m", "Write it down and keep it safe."))
			fmt.Println("Your seed phrase is the only way to recover your Siacoin. If you lose your seed phrase, you will also lose your Siacoin.")
			fmt.Println("You will need to re-enter this seed phrase every time you start indexd.")
			fmt.Println("")
			fmt.Println(ansiStyle("34;1m", "Seed Phrase:"), phrase)
			fmt.Println(ansiStyle("34;1m", "Wallet Address:"), types.StandardUnlockHash(key.PublicKey()))

			// confirm seed phrase
			for {
				fmt.Println("")
				fmt.Println(ansiStyle("1m", "Please confirm your seed phrase to continue."))
				confirmPhrase := readPasswordInput("Enter seed phrase")
				if confirmPhrase == phrase {
					cfg.RecoveryPhrase = phrase
					return
				}

				fmt.Println(ansiStyle("31m", "Seed phrases do not match!"))
				fmt.Println("You entered:", confirmPhrase)
				fmt.Println("Actual phrase:", phrase)
			}
		}

		var seed [32]byte
		if err := wallet.SeedFromPhrase(&seed, phrase); err != nil {
			fmt.Println(ansiStyle("31m", "Invalid seed phrase:"), err)
			fmt.Println("You entered:", phrase)
			continue
		}

		// valid seed phrase
		cfg.RecoveryPhrase = phrase
		break
	}
}

// stdoutError prints an error message to stdout
func stdoutError(msg string) {
	if cfg.Log.StdOut.EnableANSI {
		fmt.Println(ansiStyle("31m", msg))
	} else {
		fmt.Println(msg)
	}
}

// ansiStyle wraps the output in ANSI escape codes if enabled.
func ansiStyle(style, output string) string {
	if cfg.Log.StdOut.EnableANSI {
		return fmt.Sprintf("\033[%sm%s\033[0m", style, output)
	}
	return output
}
