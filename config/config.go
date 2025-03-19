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
	// HTTP contains the configuration for the HTTP server.
	HTTP struct {
		Address  string `yaml:"address,omitempty"`
		Password string `yaml:"password,omitempty"`
	}

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

	// Explorer contains the configuration for an external explorer.
	Explorer struct {
		Enabled bool   `yaml:"enabled,omitempty"`
		URL     string `yaml:"url,omitempty"`
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

		HTTP      HTTP                    `yaml:"http,omitempty"`
		Syncer    Syncer                  `yaml:"syncer,omitempty"`
		Consensus Consensus               `yaml:"consensus,omitempty"`
		Explorer  Explorer                `yaml:"explorer,omitempty"`
		Log       Log                     `yaml:"log,omitempty"`
		Database  postgres.ConnectionInfo `yaml:"database,omitempty"`
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
