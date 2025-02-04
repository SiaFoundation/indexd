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
	// Syncer contains the configuration for the p2p syncer.
	Syncer struct {
		Address    string   `yaml:"address,omitempty"`
		Bootstrap  bool     `yaml:"bootstrap,omitempty"`
		EnableUPnP bool     `yaml:"enableUPnP,omitempty"`
		Peers      []string `yaml:"peers,omitempty"`
	}

	// Consensus contains the configuration for the consensus set.
	Consensus struct {
		Network        string `yaml:"network,omitempty"`
		IndexBatchSize int    `yaml:"indexBatchSize,omitempty"`
	}

	// FileLog configures the file output of the logger.
	FileLog struct {
		Enabled bool            `yaml:"enabled,omitempty"`
		Level   zap.AtomicLevel `yaml:"level,omitempty"`
		Format  string          `yaml:"format,omitempty"`
		// Path is the path of the log file.
		Path string `yaml:"path,omitempty"`
	}

	// StdOutLog configures the standard output of the logger.
	StdOutLog struct {
		Level      zap.AtomicLevel `yaml:"level,omitempty"`
		Enabled    bool            `yaml:"enabled,omitempty"`
		Format     string          `yaml:"format,omitempty"`
		EnableANSI bool            `yaml:"enableANSI,omitempty"` //nolint:tagliatelle
	}

	// Log contains the configuration for the logger.
	Log struct {
		StdOut StdOutLog `yaml:"stdout,omitempty"`
		File   FileLog   `yaml:"file,omitempty"`
	}

	// Config contains the configuration for the indexer
	Config struct {
		Directory      string `yaml:"directory,omitempty"`
		RecoveryPhrase string `yaml:"recoveryPhrase,omitempty"`

		Syncer    Syncer                  `yaml:"syncer"`
		Consensus Consensus               `yaml:"consensus"`
		Log       Log                     `yaml:"log"`
		Database  postgres.ConnectionInfo `yaml:"database"`
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
