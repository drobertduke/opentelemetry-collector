package telemetryqueryextension

import (
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configgrpc"
	"go.opentelemetry.io/collector/config/confignet"
)

// Config defines the configuration for the telemetry query extension.
type Config struct {
	// ServerConfig defines the gRPC server settings.
	ServerConfig configgrpc.ServerConfig `mapstructure:",squash"`
}

// Validate checks if the extension configuration is valid.
func (cfg *Config) Validate() error {
	return cfg.ServerConfig.Validate()
}

func createDefaultConfig() component.Config {
	return &Config{
		ServerConfig: configgrpc.ServerConfig{
			NetAddr: confignet.AddrConfig{
				Endpoint:  "0.0.0.0:4319", // Default endpoint
				Transport: confignet.TransportTypeTCP,
			},
		},
	}
}
