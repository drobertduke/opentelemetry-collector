package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"telemetrycollector/internal/telemetrybufferprocessor"
	"telemetrycollector/internal/telemetryqueryextension"

	"google.golang.org/grpc"
)

func main() {
	// Create a new telemetry processor with default configuration
	processor := telemetrybufferprocessor.NewTelemetryProcessor(&telemetrybufferprocessor.Config{
		TracesBufferSize:  10000,
		MetricsBufferSize: 10000,
		LogsBufferSize:    10000,
	})

	// Register the processor globally
	telemetrybufferprocessor.RegisterProcessor(processor)

	// Create a gRPC server for the query service
	grpcServer := grpc.NewServer()

	// Register the query service
	queryService := telemetryqueryextension.NewTelemetryQueryServiceServer()
	telemetryqueryextension.RegisterTelemetryQueryServiceServer(grpcServer, queryService)

	// Start the gRPC server
	listener, err := net.Listen("tcp", "0.0.0.0:4317")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	// Start the server in a goroutine
	go func() {
		fmt.Println("Starting gRPC server on :4317")
		if err := grpcServer.Serve(listener); err != nil {
			log.Fatalf("Failed to serve: %v", err)
		}
	}()

	// Create a separate listener for OTLP data
	otlpListener, err := net.Listen("tcp", "0.0.0.0:4318")
	if err != nil {
		log.Fatalf("Failed to listen for OTLP: %v", err)
	}
	defer otlpListener.Close()

	// Start a simple HTTP server to receive OTLP data
	go func() {
		fmt.Println("Starting OTLP receiver on :4318")
		// In a real implementation, you would use the OTLP receiver from OpenTelemetry
		// For this example, we'll just log that it's started
		fmt.Println("OTLP receiver is ready to accept connections")
		// You would process incoming OTLP data and pass it to the processor
	}()

	// Wait for termination signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	// Gracefully stop the server
	fmt.Println("Shutting down...")
	grpcServer.GracefulStop()
	fmt.Println("Server stopped")
}
