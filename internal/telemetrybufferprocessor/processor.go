package telemetrybufferprocessor

import (
	"context"
	"sync"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

var (
	globalProcessor     *TelemetryProcessor
	globalProcessorLock sync.Mutex
)

// RegisterProcessor registers a processor as the global processor.
func RegisterProcessor(processor *TelemetryProcessor) {
	globalProcessorLock.Lock()
	defer globalProcessorLock.Unlock()
	globalProcessor = processor
}

// GetGlobalProcessor returns the global processor.
func GetGlobalProcessor() *TelemetryProcessor {
	globalProcessorLock.Lock()
	defer globalProcessorLock.Unlock()
	return globalProcessor
}

// TelemetryProcessor is a processor that buffers telemetry data in memory.
type TelemetryProcessor struct {
	tracesBuffer  *RingBuffer
	metricsBuffer *RingBuffer
	logsBuffer    *RingBuffer
}

// NewTelemetryProcessor creates a new telemetry processor.
func NewTelemetryProcessor(config *Config) *TelemetryProcessor {
	return &TelemetryProcessor{
		tracesBuffer:  NewRingBuffer(config.TracesBufferSize),
		metricsBuffer: NewRingBuffer(config.MetricsBufferSize),
		logsBuffer:    NewRingBuffer(config.LogsBufferSize),
	}
}

// ProcessTraces processes trace data.
func (p *TelemetryProcessor) ProcessTraces(ctx context.Context, resourceSpans []*tracepb.ResourceSpans) error {
	for _, rs := range resourceSpans {
		// Extract resource attributes
		resourceAttrs := extractAttributes(rs.Resource.GetAttributes())
		serviceName := extractServiceName(resourceAttrs)

		// Process each instrumentation library spans
		for _, ils := range rs.ScopeSpans {
			for _, span := range ils.Spans {
				// Create a span item
				spanItem := &SpanItem{
					TraceID:       span.TraceId,
					SpanID:        span.SpanId,
					ParentSpanID:  span.ParentSpanId,
					Name:          span.Name,
					Kind:          int32(span.Kind),
					StartTime:     span.StartTimeUnixNano,
					EndTime:       span.EndTimeUnixNano,
					StatusCode:    int32(span.Status.Code),
					StatusMessage: span.Status.Message,
					ServiceName:   serviceName,
					Attributes:    extractAttributes(span.Attributes),
				}

				// Add the span to the buffer
				p.tracesBuffer.Push(spanItem)
			}
		}
	}
	return nil
}

// ProcessMetrics processes metric data.
func (p *TelemetryProcessor) ProcessMetrics(ctx context.Context, resourceMetrics []*metricspb.ResourceMetrics) error {
	for _, rm := range resourceMetrics {
		// Extract resource attributes
		resourceAttrs := extractAttributes(rm.Resource.GetAttributes())
		serviceName := extractServiceName(resourceAttrs)

		// Process each instrumentation library metrics
		for _, ilm := range rm.ScopeMetrics {
			for _, metric := range ilm.Metrics {
				// Process the metric based on its type
				switch data := metric.Data.(type) {
				case *metricspb.Metric_Gauge:
					for _, point := range data.Gauge.DataPoints {
						metricItem := &MetricItem{
							Name:        metric.Name,
							Description: metric.Description,
							Unit:        metric.Unit,
							Type:        "gauge",
							Timestamp:   point.TimeUnixNano,
							ServiceName: serviceName,
							Attributes:  extractAttributes(point.Attributes),
						}

						// Set the value based on the type
						switch v := point.Value.(type) {
						case *metricspb.NumberDataPoint_AsDouble:
							metricItem.Value = v.AsDouble
						case *metricspb.NumberDataPoint_AsInt:
							metricItem.Value = v.AsInt
						}

						// Add the metric to the buffer
						p.metricsBuffer.Push(metricItem)
					}
				case *metricspb.Metric_Sum:
					for _, point := range data.Sum.DataPoints {
						metricItem := &MetricItem{
							Name:        metric.Name,
							Description: metric.Description,
							Unit:        metric.Unit,
							Type:        "sum",
							Timestamp:   point.TimeUnixNano,
							ServiceName: serviceName,
							Attributes:  extractAttributes(point.Attributes),
						}

						// Set the value based on the type
						switch v := point.Value.(type) {
						case *metricspb.NumberDataPoint_AsDouble:
							metricItem.Value = v.AsDouble
						case *metricspb.NumberDataPoint_AsInt:
							metricItem.Value = v.AsInt
						}

						// Add the metric to the buffer
						p.metricsBuffer.Push(metricItem)
					}
				case *metricspb.Metric_Histogram:
					for _, point := range data.Histogram.DataPoints {
						// Create a histogram data structure
						histogramData := make(map[string]interface{})
						histogramData["count"] = point.Count
						if point.Sum != nil {
							histogramData["sum"] = *point.Sum
						}

						// Process buckets if available
						if len(point.BucketCounts) > 0 {
							buckets := make([]interface{}, 0, len(point.BucketCounts))

							// Create bucket data
							for i, count := range point.BucketCounts {
								bucket := make(map[string]interface{})
								bucket["count"] = count

								// Set upper bound if available
								if i < len(point.ExplicitBounds) {
									bucket["upper_bound"] = point.ExplicitBounds[i]
								} else if i == len(point.ExplicitBounds) {
									// The last bucket has no upper bound (infinity)
									bucket["upper_bound"] = float64(0)
								}

								buckets = append(buckets, bucket)
							}

							histogramData["buckets"] = buckets
						}

						metricItem := &MetricItem{
							Name:        metric.Name,
							Description: metric.Description,
							Unit:        metric.Unit,
							Type:        "histogram",
							Timestamp:   point.TimeUnixNano,
							ServiceName: serviceName,
							Attributes:  extractAttributes(point.Attributes),
							Value:       histogramData,
						}

						// Add the metric to the buffer
						p.metricsBuffer.Push(metricItem)
					}
				case *metricspb.Metric_Summary:
					// Process summary metrics if needed
				}
			}
		}
	}
	return nil
}

// ProcessLogs processes log data.
func (p *TelemetryProcessor) ProcessLogs(ctx context.Context, resourceLogs []*logspb.ResourceLogs) error {
	for _, rl := range resourceLogs {
		// Extract resource attributes
		resourceAttrs := extractAttributes(rl.Resource.GetAttributes())
		serviceName := extractServiceName(resourceAttrs)

		// Process each instrumentation library logs
		for _, ill := range rl.ScopeLogs {
			for _, log := range ill.LogRecords {
				// Create a log item
				logItem := &LogItem{
					TraceID:        log.TraceId,
					SpanID:         log.SpanId,
					Timestamp:      log.TimeUnixNano,
					SeverityText:   log.SeverityText,
					SeverityNumber: int32(log.SeverityNumber),
					Body:           extractBody(log.Body),
					ServiceName:    serviceName,
					Attributes:     extractAttributes(log.Attributes),
				}

				// Add the log to the buffer
				p.logsBuffer.Push(logItem)
			}
		}
	}
	return nil
}

// GetTracesBuffer returns the traces buffer.
func (p *TelemetryProcessor) GetTracesBuffer() *RingBuffer {
	return p.tracesBuffer
}

// GetMetricsBuffer returns the metrics buffer.
func (p *TelemetryProcessor) GetMetricsBuffer() *RingBuffer {
	return p.metricsBuffer
}

// GetLogsBuffer returns the logs buffer.
func (p *TelemetryProcessor) GetLogsBuffer() *RingBuffer {
	return p.logsBuffer
}

// extractAttributes extracts attributes from a slice of KeyValue.
func extractAttributes(kvs []*commonpb.KeyValue) map[string]interface{} {
	if len(kvs) == 0 {
		return nil
	}

	attrs := make(map[string]interface{}, len(kvs))
	for _, kv := range kvs {
		if kv.Key == "" {
			continue
		}

		switch v := kv.Value.Value.(type) {
		case *commonpb.AnyValue_StringValue:
			attrs[kv.Key] = v.StringValue
		case *commonpb.AnyValue_IntValue:
			attrs[kv.Key] = v.IntValue
		case *commonpb.AnyValue_DoubleValue:
			attrs[kv.Key] = v.DoubleValue
		case *commonpb.AnyValue_BoolValue:
			attrs[kv.Key] = v.BoolValue
		case *commonpb.AnyValue_ArrayValue:
			// Process array values if needed
		case *commonpb.AnyValue_KvlistValue:
			// Process key-value list values if needed
		case *commonpb.AnyValue_BytesValue:
			attrs[kv.Key] = v.BytesValue
		}
	}

	return attrs
}

// extractServiceName extracts the service name from attributes.
func extractServiceName(attrs map[string]interface{}) string {
	if attrs == nil {
		return ""
	}

	if serviceName, ok := attrs["service.name"].(string); ok {
		return serviceName
	}

	return ""
}

// extractBody extracts the body from a log record.
func extractBody(body *commonpb.AnyValue) string {
	if body == nil {
		return ""
	}

	switch v := body.Value.(type) {
	case *commonpb.AnyValue_StringValue:
		return v.StringValue
	default:
		return ""
	}
}
