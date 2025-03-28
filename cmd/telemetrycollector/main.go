package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"telemetrycollector/internal/telemetrybufferprocessor"
	"telemetrycollector/internal/telemetryqueryextension"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	// Import OTLP proto packages
	collectorlogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	collectormetrics "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	collectortraces "go.opentelemetry.io/proto/otlp/collector/trace/v1"
)

// OTLPTraceServer implements the OTLP trace service
type OTLPTraceServer struct {
	collectortraces.UnimplementedTraceServiceServer
	processor *telemetrybufferprocessor.TelemetryProcessor
}

// Export implements the OTLP trace service Export method
func (s *OTLPTraceServer) Export(ctx context.Context, req *collectortraces.ExportTraceServiceRequest) (*collectortraces.ExportTraceServiceResponse, error) {
	// Process the traces
	if err := s.processor.ProcessTraces(ctx, req.ResourceSpans); err != nil {
		return nil, err
	}
	return &collectortraces.ExportTraceServiceResponse{}, nil
}

// OTLPMetricsServer implements the OTLP metrics service
type OTLPMetricsServer struct {
	collectormetrics.UnimplementedMetricsServiceServer
	processor *telemetrybufferprocessor.TelemetryProcessor
}

// Export implements the OTLP metrics service Export method
func (s *OTLPMetricsServer) Export(ctx context.Context, req *collectormetrics.ExportMetricsServiceRequest) (*collectormetrics.ExportMetricsServiceResponse, error) {
	// Process the metrics
	if err := s.processor.ProcessMetrics(ctx, req.ResourceMetrics); err != nil {
		return nil, err
	}
	return &collectormetrics.ExportMetricsServiceResponse{}, nil
}

// OTLPLogsServer implements the OTLP logs service
type OTLPLogsServer struct {
	collectorlogs.UnimplementedLogsServiceServer
	processor *telemetrybufferprocessor.TelemetryProcessor
}

// Export implements the OTLP logs service Export method
func (s *OTLPLogsServer) Export(ctx context.Context, req *collectorlogs.ExportLogsServiceRequest) (*collectorlogs.ExportLogsServiceResponse, error) {
	// Process the logs
	if err := s.processor.ProcessLogs(ctx, req.ResourceLogs); err != nil {
		return nil, err
	}
	return &collectorlogs.ExportLogsServiceResponse{}, nil
}

func main() {
	// Parse command line flags
	var configFile string
	flag.StringVar(&configFile, "config", "config.yaml", "Path to the config file")
	flag.Parse()

	// Create a new telemetry processor with default configuration
	processor := telemetrybufferprocessor.NewTelemetryProcessor(&telemetrybufferprocessor.Config{
		TracesBufferSize:  10000,
		MetricsBufferSize: 10000,
		LogsBufferSize:    10000,
	})

	// Register the processor globally
	telemetrybufferprocessor.RegisterProcessor(processor)

	// Create and start the telemetry query extension
	queryConfig := telemetryqueryextension.NewConfig() // This sets endpoint to 0.0.0.0:4319
	queryExtension, err := telemetryqueryextension.NewTelemetryQueryExtension(queryConfig)
	if err != nil {
		log.Fatalf("Failed to create telemetry query extension: %v", err)
	}

	// Start the extension
	if err := queryExtension.Start(context.Background()); err != nil {
		log.Fatalf("Failed to start telemetry query extension: %v", err)
	}

	// Create a gRPC server for the OTLP receiver
	grpcServer := grpc.NewServer()

	// Create the OTLP servers
	traceServer := &OTLPTraceServer{
		processor: processor,
	}
	metricsServer := &OTLPMetricsServer{
		processor: processor,
	}
	logsServer := &OTLPLogsServer{
		processor: processor,
	}

	// Register the OTLP services
	collectortraces.RegisterTraceServiceServer(grpcServer, traceServer)
	collectormetrics.RegisterMetricsServiceServer(grpcServer, metricsServer)
	collectorlogs.RegisterLogsServiceServer(grpcServer, logsServer)

	// Register reflection service for grpcurl
	reflection.Register(grpcServer)

	// Start the OTLP gRPC server
	listener, err := net.Listen("tcp", "0.0.0.0:4317")
	if err != nil {
		log.Fatalf("Failed to listen on OTLP port: %v", err)
	}

	// Start the server in a goroutine
	go func() {
		fmt.Println("Starting OTLP gRPC server on :4317")
		if err := grpcServer.Serve(listener); err != nil {
			log.Fatalf("Failed to serve OTLP gRPC: %v", err)
		}
	}()

	// Create a separate listener for OTLP HTTP data
	otlpListener, err := net.Listen("tcp", "0.0.0.0:4318")
	if err != nil {
		log.Fatalf("Failed to listen for OTLP HTTP: %v", err)
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

	// Gracefully stop the servers
	fmt.Println("Shutting down...")
	grpcServer.GracefulStop()

	// Shutdown the query extension
	if err := queryExtension.Shutdown(context.Background()); err != nil {
		log.Printf("Error shutting down query extension: %v", err)
	}

	fmt.Println("Server stopped")
}
