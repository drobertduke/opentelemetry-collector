package telemetrybufferprocessor

import (
	"context"
	"fmt"
	"sync"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

var (
	globalProcessor     *TelemetryProcessor
	globalProcessorLock sync.RWMutex
)

// TelemetryProcessor is a processor that stores telemetry data in ring buffers.
type TelemetryProcessor struct {
	config        *Config
	tracesBuffer  *RingBuffer
	metricsBuffer *RingBuffer
	logsBuffer    *RingBuffer
}

// NewTelemetryProcessor creates a new TelemetryProcessor.
func NewTelemetryProcessor(config *Config) *TelemetryProcessor {
	return &TelemetryProcessor{
		config:        config,
		tracesBuffer:  NewRingBuffer(config.TracesBufferSize),
		metricsBuffer: NewRingBuffer(config.MetricsBufferSize),
		logsBuffer:    NewRingBuffer(config.LogsBufferSize),
	}
}

// RegisterProcessor registers the processor globally.
func RegisterProcessor(processor *TelemetryProcessor) {
	globalProcessorLock.Lock()
	defer globalProcessorLock.Unlock()
	globalProcessor = processor
}

// GetGlobalProcessor returns the globally registered processor.
func GetGlobalProcessor() *TelemetryProcessor {
	globalProcessorLock.RLock()
	defer globalProcessorLock.RUnlock()
	return globalProcessor
}

// ProcessTraces processes trace data and stores it in the ring buffer.
func (p *TelemetryProcessor) ProcessTraces(ctx context.Context, resourceSpans []*tracepb.ResourceSpans) error {
	for _, rs := range resourceSpans {
		resource := rs.Resource
		serviceName := extractServiceName(resource)

		for _, ils := range rs.ScopeSpans {
			for _, span := range ils.Spans {
				// Create a SpanItem from the span
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
					Attributes:    convertAttributesToMap(span.Attributes),
				}

				// Store the span in the ring buffer
				p.tracesBuffer.Push(spanItem)
			}
		}
	}
	return nil
}

// ProcessMetrics processes metric data and stores it in the ring buffer.
func (p *TelemetryProcessor) ProcessMetrics(ctx context.Context, resourceMetrics []*metricspb.ResourceMetrics) error {
	for _, rm := range resourceMetrics {
		resource := rm.Resource
		serviceName := extractServiceName(resource)

		for _, ilm := range rm.ScopeMetrics {
			for _, metric := range ilm.Metrics {
				// Extract the metric value based on the data point type
				var value interface{}
				var timestamp uint64
				var metricType string

				// Handle different metric types
				switch dp := metric.Data.(type) {
				case *metricspb.Metric_Gauge:
					if dp.Gauge != nil && len(dp.Gauge.DataPoints) > 0 {
						metricType = "gauge"
						timestamp = dp.Gauge.DataPoints[0].TimeUnixNano

						switch v := dp.Gauge.DataPoints[0].Value.(type) {
						case *metricspb.NumberDataPoint_AsDouble:
							value = v.AsDouble
						case *metricspb.NumberDataPoint_AsInt:
							value = v.AsInt
						}
					}
				case *metricspb.Metric_Sum:
					if dp.Sum != nil && len(dp.Sum.DataPoints) > 0 {
						metricType = "sum"
						timestamp = dp.Sum.DataPoints[0].TimeUnixNano

						switch v := dp.Sum.DataPoints[0].Value.(type) {
						case *metricspb.NumberDataPoint_AsDouble:
							value = v.AsDouble
						case *metricspb.NumberDataPoint_AsInt:
							value = v.AsInt
						}
					}
				case *metricspb.Metric_Histogram:
					if dp.Histogram != nil && len(dp.Histogram.DataPoints) > 0 {
						metricType = "histogram"
						timestamp = dp.Histogram.DataPoints[0].TimeUnixNano
						value = map[string]interface{}{
							"count": dp.Histogram.DataPoints[0].Count,
							"sum":   dp.Histogram.DataPoints[0].Sum,
						}
					}
				case *metricspb.Metric_ExponentialHistogram:
					if dp.ExponentialHistogram != nil && len(dp.ExponentialHistogram.DataPoints) > 0 {
						metricType = "exponential_histogram"
						timestamp = dp.ExponentialHistogram.DataPoints[0].TimeUnixNano
						value = map[string]interface{}{
							"count": dp.ExponentialHistogram.DataPoints[0].Count,
							"sum":   dp.ExponentialHistogram.DataPoints[0].Sum,
						}
					}
				case *metricspb.Metric_Summary:
					if dp.Summary != nil && len(dp.Summary.DataPoints) > 0 {
						metricType = "summary"
						timestamp = dp.Summary.DataPoints[0].TimeUnixNano
						value = map[string]interface{}{
							"count": dp.Summary.DataPoints[0].Count,
							"sum":   dp.Summary.DataPoints[0].Sum,
						}
					}
				}

				// Create a MetricItem from the metric
				metricItem := &MetricItem{
					Name:        metric.Name,
					Description: metric.Description,
					Unit:        metric.Unit,
					Type:        metricType,
					Timestamp:   timestamp,
					Value:       value,
					ServiceName: serviceName,
					Attributes:  convertAttributesToMap(nil), // Add attributes if needed
				}

				// Store the metric in the ring buffer
				p.metricsBuffer.Push(metricItem)
			}
		}
	}
	return nil
}

// ProcessLogs processes log data and stores it in the ring buffer.
func (p *TelemetryProcessor) ProcessLogs(ctx context.Context, resourceLogs []*logspb.ResourceLogs) error {
	for _, rl := range resourceLogs {
		resource := rl.Resource
		serviceName := extractServiceName(resource)

		for _, ill := range rl.ScopeLogs {
			for _, log := range ill.LogRecords {
				// Create a LogItem from the log
				logItem := &LogItem{
					TraceID:        log.TraceId,
					SpanID:         log.SpanId,
					Timestamp:      log.TimeUnixNano,
					SeverityText:   log.SeverityText,
					SeverityNumber: int32(log.SeverityNumber),
					Body:           extractLogBody(log.Body),
					ServiceName:    serviceName,
					Attributes:     convertAttributesToMap(log.Attributes),
				}

				// Store the log in the ring buffer
				p.logsBuffer.Push(logItem)
			}
		}
	}
	return nil
}

// GetTracesBuffer returns the traces ring buffer.
func (p *TelemetryProcessor) GetTracesBuffer() *RingBuffer {
	return p.tracesBuffer
}

// GetMetricsBuffer returns the metrics ring buffer.
func (p *TelemetryProcessor) GetMetricsBuffer() *RingBuffer {
	return p.metricsBuffer
}

// GetLogsBuffer returns the logs ring buffer.
func (p *TelemetryProcessor) GetLogsBuffer() *RingBuffer {
	return p.logsBuffer
}

// Helper functions

// extractServiceName extracts the service name from a resource.
func extractServiceName(resource *resourcepb.Resource) string {
	if resource == nil || resource.Attributes == nil {
		return ""
	}

	for _, attr := range resource.Attributes {
		if attr.Key == "service.name" {
			return attr.Value.GetStringValue()
		}
	}

	return ""
}

// extractLogBody extracts the body from a log record.
func extractLogBody(body *commonpb.AnyValue) string {
	if body == nil {
		return ""
	}

	switch v := body.Value.(type) {
	case *commonpb.AnyValue_StringValue:
		return v.StringValue
	case *commonpb.AnyValue_IntValue:
		return fmt.Sprintf("%d", v.IntValue)
	case *commonpb.AnyValue_DoubleValue:
		return fmt.Sprintf("%f", v.DoubleValue)
	case *commonpb.AnyValue_BoolValue:
		if v.BoolValue {
			return "true"
		}
		return "false"
	default:
		return "complex value"
	}
}

// convertAttributesToMap converts OTLP attributes to a map.
func convertAttributesToMap(attributes []*commonpb.KeyValue) map[string]interface{} {
	if attributes == nil {
		return nil
	}

	result := make(map[string]interface{})
	for _, attr := range attributes {
		switch v := attr.Value.Value.(type) {
		case *commonpb.AnyValue_StringValue:
			result[attr.Key] = v.StringValue
		case *commonpb.AnyValue_IntValue:
			result[attr.Key] = v.IntValue
		case *commonpb.AnyValue_DoubleValue:
			result[attr.Key] = v.DoubleValue
		case *commonpb.AnyValue_BoolValue:
			result[attr.Key] = v.BoolValue
		}
	}

	return result
}
