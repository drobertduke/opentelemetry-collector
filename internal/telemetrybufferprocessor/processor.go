package telemetrybufferprocessor

import (
	"context"
	"sync"

	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
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
				}

				// Add attributes from the resource
				if resource != nil && resource.Attributes != nil {
					for _, attr := range resource.Attributes {
						if attr.Key == "service.name" {
							spanItem.ServiceName = attr.Value.GetStringValue()
							break
						}
					}
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
		for _, ilm := range rm.ScopeMetrics {
			for _, metric := range ilm.Metrics {
				// Create a MetricItem from the metric
				metricItem := &MetricItem{
					Name:        metric.Name,
					Description: metric.Description,
					Unit:        metric.Unit,
				}

				// Add attributes from the resource
				if resource != nil && resource.Attributes != nil {
					for _, attr := range resource.Attributes {
						if attr.Key == "service.name" {
							metricItem.ServiceName = attr.Value.GetStringValue()
							break
						}
					}
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
		for _, ill := range rl.ScopeLogs {
			for _, log := range ill.LogRecords {
				// Create a LogItem from the log
				logItem := &LogItem{
					TraceID:        log.TraceId,
					SpanID:         log.SpanId,
					Timestamp:      log.TimeUnixNano,
					SeverityText:   log.SeverityText,
					SeverityNumber: int32(log.SeverityNumber),
					Body:           log.Body.GetStringValue(),
				}

				// Add attributes from the resource
				if resource != nil && resource.Attributes != nil {
					for _, attr := range resource.Attributes {
						if attr.Key == "service.name" {
							logItem.ServiceName = attr.Value.GetStringValue()
							break
						}
					}
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
