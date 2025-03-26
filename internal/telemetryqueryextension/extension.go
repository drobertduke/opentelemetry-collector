package telemetryqueryextension

import (
	"context"
	"fmt"
	"net"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

// TelemetryQueryExtension implements a gRPC server for querying telemetry data.
type TelemetryQueryExtension struct {
	config   *Config
	server   *grpc.Server
	listener net.Listener
	wg       sync.WaitGroup
}

// NewTelemetryQueryExtension creates a new TelemetryQueryExtension.
func NewTelemetryQueryExtension(config *Config) (*TelemetryQueryExtension, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}

	return &TelemetryQueryExtension{
		config: config,
	}, nil
}

// Start starts the gRPC server.
func (e *TelemetryQueryExtension) Start(ctx context.Context) error {
	// Create a listener
	listener, err := net.Listen("tcp", e.config.Endpoint)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %v", e.config.Endpoint, err)
	}
	e.listener = listener

	// Create a gRPC server
	e.server = grpc.NewServer()

	// Register the query service
	RegisterTelemetryQueryServiceServer(e.server, NewTelemetryQueryServiceServer())

	// Register reflection service for grpcurl
	reflection.Register(e.server)

	// Start the server in a goroutine
	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		fmt.Printf("Starting telemetry query gRPC server on %s\n", e.config.Endpoint)
		if err := e.server.Serve(listener); err != nil {
			fmt.Printf("gRPC server error: %v\n", err)
		}
	}()

	return nil
}

// Shutdown stops the gRPC server.
func (e *TelemetryQueryExtension) Shutdown(ctx context.Context) error {
	if e.server != nil {
		e.server.GracefulStop()
	}
	if e.listener != nil {
		_ = e.listener.Close()
	}
	e.wg.Wait()
	return nil
}
