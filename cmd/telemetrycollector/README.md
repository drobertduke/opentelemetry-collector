# Telemetry Query Collector

This is a custom OpenTelemetry Collector that includes a ring buffer processor and a query extension. It allows you to:

1. Receive telemetry data (traces, metrics, logs) via OTLP
2. Store the data in configurable in-memory ring buffers
3. Query the buffered data via a gRPC API
4. Optionally stream real-time telemetry data

## Features

- **Ring Buffer Storage**: Configurable in-memory storage for traces, metrics, and logs
- **Query API**: gRPC API for querying stored telemetry data
- **Streaming Support**: Real-time streaming of telemetry data as it arrives
- **Standard OTLP Support**: Compatible with standard OpenTelemetry SDKs and tools

## Building

To build the collector:

```bash
cd cmd/telemetrycollector
go mod tidy
go build -o telemetrycollector
```

## Running

Run the collector with the provided configuration:

```bash
./telemetrycollector --config=config.yaml
```

## Configuration

The configuration file (`config.yaml`) includes:

### Telemetry Buffer Processor

```yaml
processors:
  telemetry_buffer:
    traces_buffer_size: 10000  # Number of spans to keep in the buffer
    metrics_buffer_size: 5000  # Number of metric data points to keep in the buffer
    logs_buffer_size: 5000     # Number of log records to keep in the buffer
```

### Telemetry Query Extension

```yaml
extensions:
  telemetry_query:
    endpoint: 0.0.0.0:4319  # gRPC endpoint for the query service
```

## Query API

The query API is exposed as a gRPC service on the configured endpoint. You can use a gRPC client to query the stored telemetry data.

### Proto Definition

The API is defined in `internal/telemetryqueryextension/query_service.proto`. The main methods are:

- `QueryTraces`: Query stored traces with filters
- `QueryMetrics`: Query stored metrics with filters
- `QueryLogs`: Query stored logs with filters
- `SubscribeTraces`: Stream traces in real-time
- `SubscribeMetrics`: Stream metrics in real-time
- `SubscribeLogs`: Stream logs in real-time
- `GetBufferInfo`: Get information about the ring buffers

### Example Client

Here's an example of how to query traces using a gRPC client:

```go
package main

import (
    "context"
    "log"
    "time"

    "google.golang.org/grpc"
    "google.golang.org/protobuf/types/known/timestamppb"
    pb "path/to/generated/proto"
)

func main() {
    conn, err := grpc.Dial("localhost:4319", grpc.WithInsecure())
    if err != nil {
        log.Fatalf("Failed to connect: %v", err)
    }
    defer conn.Close()

    client := pb.NewTelemetryQueryServiceClient(conn)

    // Query traces from the last 5 minutes
    now := time.Now()
    fiveMinutesAgo := now.Add(-5 * time.Minute)
    
    req := &pb.TraceQueryRequest{
        StartTime: timestamppb.New(fiveMinutesAgo),
        EndTime: timestamppb.New(now),
        ServiceName: "my-service", // Optional filter by service name
    }

    resp, err := client.QueryTraces(context.Background(), req)
    if err != nil {
        log.Fatalf("Failed to query traces: %v", err)
    }

    log.Printf("Found %d spans", len(resp.Spans))
    for _, span := range resp.Spans {
        log.Printf("Span: %s, TraceID: %s", span.Name, span.TraceId)
    }
}
```

## Streaming Example

To subscribe to real-time traces:

```go
req := &pb.TraceQueryRequest{
    ServiceName: "my-service",
    ImmediateMode: true, // Only receive new spans, not existing ones
}

stream, err := client.SubscribeTraces(context.Background(), req)
if err != nil {
    log.Fatalf("Failed to subscribe: %v", err)
}

for {
    resp, err := stream.Recv()
    if err != nil {
        log.Fatalf("Stream error: %v", err)
        break
    }

    for _, span := range resp.Spans {
        log.Printf("New span: %s, TraceID: %s", span.Name, span.TraceId)
    }
}
```

## Architecture

The collector consists of two main components:

1. **Telemetry Buffer Processor**: Intercepts telemetry data from the pipeline and stores it in ring buffers
2. **Telemetry Query Extension**: Exposes a gRPC API for querying the stored data

The ring buffers are thread-safe and support concurrent reads and writes. The processor pushes data into the buffers, and the extension reads from them when handling queries.