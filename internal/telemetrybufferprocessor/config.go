package telemetrybufferprocessor

import (
	"fmt"

	"go.opentelemetry.io/collector/component"
)

// Config defines the configuration for the telemetry buffer processor.
type Config struct {
	// TracesBufferSize is the maximum number of spans to keep in the buffer.
	TracesBufferSize int `mapstructure:"traces_buffer_size"`

	// MetricsBufferSize is the maximum number of metric data points to keep in the buffer.
	MetricsBufferSize int `mapstructure:"metrics_buffer_size"`

	// LogsBufferSize is the maximum number of log records to keep in the buffer.
	LogsBufferSize int `mapstructure:"logs_buffer_size"`
}

// Validate checks if the processor configuration is valid.
func (cfg *Config) Validate() error {
	if cfg.TracesBufferSize <= 0 {
		return fmt.Errorf("traces_buffer_size must be greater than 0")
	}
	if cfg.MetricsBufferSize <= 0 {
		return fmt.Errorf("metrics_buffer_size must be greater than 0")
	}
	if cfg.LogsBufferSize <= 0 {
		return fmt.Errorf("logs_buffer_size must be greater than 0")
	}
	return nil
}

// createDefaultConfig creates the default configuration for the processor.
func createDefaultConfig() component.Config {
	return &Config{
		TracesBufferSize:  10000,
		MetricsBufferSize: 10000,
		LogsBufferSize:    10000,
	}
}
