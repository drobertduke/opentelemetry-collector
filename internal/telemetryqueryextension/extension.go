package telemetryqueryextension

import (
	"context"
	"fmt"
	"net"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/extension"
	"go.opentelemetry.io/collector/internal/telemetrybufferprocessor"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// telemetryQueryExtension implements the extension.Extension interface.
type telemetryQueryExtension struct {
	config      *Config
	logger      *zap.Logger
	server      *grpc.Server
	listener    net.Listener
	queryServer *queryServiceServer
}

// Start starts the gRPC server.
func (tqe *telemetryQueryExtension) Start(ctx context.Context, host component.Host) error {
	// Check if the processor is active
	if !telemetrybufferprocessor.IsProcessorActive() {
		return fmt.Errorf("telemetry buffer processor is not active, make sure it's configured in the pipeline")
	}

	// Get the ring buffers from the processor
	tracesBuffer := telemetrybufferprocessor.GetTracesBuffer()
	metricsBuffer := telemetrybufferprocessor.GetMetricsBuffer()
	logsBuffer := telemetrybufferprocessor.GetLogsBuffer()

	if tracesBuffer == nil || metricsBuffer == nil || logsBuffer == nil {
		return fmt.Errorf("one or more ring buffers are not initialized")
	}

	// Create the gRPC server
	server, err := tqe.config.ServerConfig.ToServer(ctx, host, component.TelemetrySettings{
		Logger: tqe.logger,
	})
	if err != nil {
		return fmt.Errorf("failed to create gRPC server: %w", err)
	}
	tqe.server = server

	// Create and register the query service
	tqe.queryServer = &queryServiceServer{
		tracesBuffer:  tracesBuffer,
		metricsBuffer: metricsBuffer,
		logsBuffer:    logsBuffer,
		logger:        tqe.logger,
	}

	// Note: RegisterTelemetryQueryServiceServer will be available after generating code from the proto file
	// For now, we'll comment this out
	// RegisterTelemetryQueryServiceServer(tqe.server, tqe.queryServer)

	// Start listening
	listener, err := net.Listen(string(tqe.config.ServerConfig.NetAddr.Transport), tqe.config.ServerConfig.NetAddr.Endpoint)
	if err != nil {
		return fmt.Errorf("failed to create listener: %w", err)
	}
	tqe.listener = listener

	tqe.logger.Info("Starting telemetry query gRPC server", zap.String("endpoint", tqe.config.ServerConfig.NetAddr.Endpoint))

	// Start the server in a goroutine
	go func() {
		if err := tqe.server.Serve(listener); err != nil {
			tqe.logger.Error("Telemetry query gRPC server failed", zap.Error(err))
		}
	}()

	return nil
}

// Shutdown stops the gRPC server.
func (tqe *telemetryQueryExtension) Shutdown(ctx context.Context) error {
	if tqe.server != nil {
		tqe.server.GracefulStop()
		tqe.server = nil
	}

	if tqe.listener != nil {
		_ = tqe.listener.Close()
		tqe.listener = nil
	}

	return nil
}

// NewFactory creates a factory for the telemetry query extension.
func NewFactory() extension.Factory {
	typeVal, err := component.NewType("telemetry_query")
	if err != nil {
		// This should never happen for a valid type name
		panic(fmt.Sprintf("failed to create component type: %v", err))
	}

	return extension.NewFactory(
		typeVal,
		createDefaultConfig,
		createExtension,
		component.StabilityLevelDevelopment,
	)
}

// createExtension creates a new telemetry query extension.
func createExtension(
	ctx context.Context,
	set extension.Settings,
	cfg component.Config,
) (extension.Extension, error) {
	config := cfg.(*Config)

	return &telemetryQueryExtension{
		config: config,
		logger: set.Logger,
	}, nil
}
