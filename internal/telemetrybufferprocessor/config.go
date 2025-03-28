package telemetrybufferprocessor

import (
	"fmt"
)

// Config defines the configuration for the telemetry buffer processor.
type Config struct {
	// TracesBufferSize is the size of the traces ring buffer.
	TracesBufferSize int `mapstructure:"traces_buffer_size"`
	// MetricsBufferSize is the size of the metrics ring buffer.
	MetricsBufferSize int `mapstructure:"metrics_buffer_size"`
	// LogsBufferSize is the size of the logs ring buffer.
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

// NewConfig creates a new Config with default values.
func NewConfig() *Config {
	return &Config{
		TracesBufferSize:  10000,
		MetricsBufferSize: 5000,
		LogsBufferSize:    10000,
	}
}
