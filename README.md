# OpenTelemetry Collector with Ring Buffer Query Extension

This project extends the OpenTelemetry Collector with a ring buffer processor and query extension. It allows you to:

1. Receive telemetry data (traces, metrics, logs) via OTLP
2. Store the data in configurable in-memory ring buffers
3. Query the buffered data via a gRPC API
4. Optionally stream real-time telemetry data

## Architecture

The implementation consists of two main components:

1. **Telemetry Buffer Processor**: Intercepts telemetry data from the collector pipeline and stores it in ring buffers.
2. **Telemetry Query Extension**: Exposes a gRPC API for querying the buffered data.

## Features

- **Configurable Buffer Sizes**: Set the maximum number of items to keep for each telemetry type.
- **Query Filters**: Filter by time range, trace ID, or service name.
- **Real-time Streaming**: Subscribe to receive new telemetry data as it arrives.
- **Buffer Info**: Get information about the current state of the buffers.

## Configuration

Example configuration:

```yaml
processors:
  telemetry_buffer:
    traces_buffer_size: 10000
    metrics_buffer_size: 5000
    logs_buffer_size: 10000

extensions:
  telemetry_query:
    endpoint: 0.0.0.0:4319

service:
  extensions: [telemetry_query]
  pipelines:
    traces:
      receivers: [otlp]
      processors: [telemetry_buffer]
      exporters: [otlp]
    metrics:
      receivers: [otlp]
      processors: [telemetry_buffer]
      exporters: [otlp]
    logs:
      receivers: [otlp]
      processors: [telemetry_buffer]
      exporters: [otlp]
```

## Query API

The query API is defined in `query_service.proto` and provides the following methods:

- `Query`: Get telemetry data from the buffer based on filters.
- `Subscribe`: Stream telemetry data as it arrives.
- `GetBufferInfo`: Get information about the current state of the buffers.

### Example Queries

#### Query Traces

```protobuf
// Request
{
  "telemetry_type": "TELEMETRY_TYPE_TRACES",
  "start_time": "2023-01-01T00:00:00Z",
  "end_time": "2023-01-01T01:00:00Z",
  "trace_id": "1234567890abcdef",
  "service_name": "my-service",
  "limit": 100
}
```

#### Subscribe to Metrics

```protobuf
// Request
{
  "telemetry_type": "TELEMETRY_TYPE_METRICS",
  "start_time": "2023-01-01T00:00:00Z",
  "service_name": "my-service"
}
```

## Building and Running

1. Clone the repository
2. Build the collector:
   ```
   go build -o telemetrycollector cmd/telemetrycollector/main.go
   ```
3. Run the collector:
   ```
   ./telemetrycollector
   ```

## Client Usage

You can use any gRPC client to query the telemetry data. Here's an example using `grpcurl`:

```bash
# Query traces
grpcurl -d '{"telemetry_type": "TELEMETRY_TYPE_TRACES"}' \
  -proto internal/telemetryqueryextension/query_service.proto \
  localhost:4319 telemetryquery.TelemetryQueryService/Query

# Get buffer info
grpcurl -proto internal/telemetryqueryextension/query_service.proto \
  localhost:4319 telemetryquery.TelemetryQueryService/GetBufferInfo
```

## Performance Considerations

- The ring buffer implementation uses a mutex for thread safety, which may become a bottleneck under high load.
- For production use, consider implementing a more efficient lock-free ring buffer.
- The buffer size should be configured based on the expected telemetry volume and memory constraints.

## Future Improvements

- Add authentication and authorization
- Implement more advanced filtering capabilities
- Add compression for large telemetry data
- Support for distributed deployment with shared buffers
