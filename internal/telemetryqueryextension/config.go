package telemetryqueryextension

import (
	"fmt"
)

// Config defines the configuration for the telemetry query extension.
type Config struct {
	// Endpoint is the address and port for the query gRPC server.
	Endpoint string `mapstructure:"endpoint"`
}

// Validate checks if the extension configuration is valid.
func (cfg *Config) Validate() error {
	if cfg.Endpoint == "" {
		return fmt.Errorf("endpoint must be specified")
	}
	return nil
}

// NewConfig creates a new Config with default values.
func NewConfig() *Config {
	return &Config{
		Endpoint: "0.0.0.0:4319",
	}
}
