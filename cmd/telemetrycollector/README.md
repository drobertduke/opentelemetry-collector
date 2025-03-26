# Telemetry Collector with Ring Buffer Query Extension

This directory contains the main application for the OpenTelemetry Collector with Ring Buffer Query Extension.

## Overview

The telemetry collector is a custom build of the OpenTelemetry Collector that includes:

1. A **Telemetry Buffer Processor** that stores telemetry data in configurable in-memory ring buffers.
2. A **Telemetry Query Extension** that exposes a gRPC API for querying the buffered data.

## Building

To build the collector:

```bash
go build -o telemetrycollector main.go
```

## Running

To run the collector with the default configuration:

```bash
./telemetrycollector
```

To run with a custom configuration file:

```bash
./telemetrycollector --config config.yaml
```

## Configuration

The collector is configured using a YAML file. See `config.yaml` for an example configuration.

Key configuration options:

- `telemetry_buffer.traces_buffer_size`: Maximum number of spans to keep in the buffer.
- `telemetry_buffer.metrics_buffer_size`: Maximum number of metric data points to keep in the buffer.
- `telemetry_buffer.logs_buffer_size`: Maximum number of log records to keep in the buffer.
- `telemetry_query.endpoint`: The address and port for the query gRPC server.

## Usage

### Sending Telemetry Data

Send telemetry data to the collector using the OTLP protocol:

- gRPC: `localhost:4317`
- HTTP: `localhost:4318`

### Querying Telemetry Data

Query the buffered telemetry data using the gRPC API:

```bash
# Using grpcurl to query traces
grpcurl -d '{"telemetry_type": "TELEMETRY_TYPE_TRACES"}' \
  -proto ../../internal/telemetryqueryextension/query_service.proto \
  localhost:4319 telemetryquery.TelemetryQueryService/Query

# Get buffer info
grpcurl -proto ../../internal/telemetryqueryextension/query_service.proto \
  localhost:4319 telemetryquery.TelemetryQueryService/GetBufferInfo
```

### Streaming Telemetry Data

Subscribe to receive telemetry data in real-time:

```bash
# Using grpcurl to subscribe to traces
grpcurl -d '{"telemetry_type": "TELEMETRY_TYPE_TRACES"}' \
  -proto ../../internal/telemetryqueryextension/query_service.proto \
  localhost:4319 telemetryquery.TelemetryQueryService/Subscribe
```

## Troubleshooting

If you encounter issues:

1. Check the collector logs for errors.
2. Verify that the collector is running and listening on the configured ports.
3. Ensure that the telemetry data is being sent in the correct format.
4. Check that the query request is properly formatted.

## Architecture

The collector uses the following components:

- **OTLP Receiver**: Receives telemetry data via gRPC or HTTP.
- **Telemetry Buffer Processor**: Stores telemetry data in ring buffers.
- **Logging Exporter**: Logs telemetry data for debugging.
- **Telemetry Query Extension**: Exposes a gRPC API for querying the buffered data.

The flow of data is:

1. Telemetry data is received by the OTLP receiver.
2. The data is processed by the telemetry buffer processor, which stores it in ring buffers.
3. The data is forwarded to the logging exporter.
4. The telemetry query extension allows clients to query the buffered data.