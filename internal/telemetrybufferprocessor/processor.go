package telemetrybufferprocessor

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor"
	"go.uber.org/zap"
)

// telemetryBufferProcessor is a processor that buffers telemetry data in memory.
type telemetryBufferProcessor struct {
	logger       *zap.Logger
	config       *Config
	nextConsumer consumer.Traces
}

// Capabilities returns the capabilities of the processor.
func (tbp *telemetryBufferProcessor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

// Shutdown stops the processor.
func (tbp *telemetryBufferProcessor) Shutdown(ctx context.Context) error {
	// Nothing to do for shutdown
	return nil
}

// Start starts the processor.
func (tbp *telemetryBufferProcessor) Start(ctx context.Context, host component.Host) error {
	// Nothing to do for start
	return nil
}

// newTracesProcessor creates a new processor for traces.
func newTracesProcessor(logger *zap.Logger, config *Config, nextConsumer consumer.Traces) (*telemetryBufferProcessor, error) {
	if nextConsumer == nil {
		return nil, fmt.Errorf("next consumer cannot be nil")
	}

	// Initialize the ring buffers if they haven't been initialized yet
	if !IsProcessorActive() {
		InitBuffers(config.TracesBufferSize, config.MetricsBufferSize, config.LogsBufferSize)
	}

	return &telemetryBufferProcessor{
		logger:       logger,
		config:       config,
		nextConsumer: nextConsumer,
	}, nil
}

// ConsumeTraces implements the consumer.Traces interface.
func (tbp *telemetryBufferProcessor) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
	// Process the traces and store them in the ring buffer
	resourceSpans := td.ResourceSpans()
	for i := 0; i < resourceSpans.Len(); i++ {
		rs := resourceSpans.At(i)
		resource := rs.Resource()

		// Extract resource attributes
		resourceAttrs := make(map[string]interface{})
		resource.Attributes().Range(func(k string, v pcommon.Value) bool {
			resourceAttrs[k] = v.AsString()
			return true
		})

		scopeSpans := rs.ScopeSpans()
		for j := 0; j < scopeSpans.Len(); j++ {
			ss := scopeSpans.At(j)
			spans := ss.Spans()
			for k := 0; k < spans.Len(); k++ {
				span := spans.At(k)

				// Extract span attributes
				attrs := make(map[string]interface{})
				span.Attributes().Range(func(k string, v pcommon.Value) bool {
					attrs[k] = v.AsString()
					return true
				})

				// Create a simplified span data structure
				spanData := &SpanData{
					TraceID:       span.TraceID().String(),
					SpanID:        span.SpanID().String(),
					ParentSpanID:  span.ParentSpanID().String(),
					Name:          span.Name(),
					StartTime:     span.StartTimestamp().AsTime(),
					EndTime:       span.EndTimestamp().AsTime(),
					Attributes:    attrs,
					ResourceAttrs: resourceAttrs,
					StatusCode:    int32(span.Status().Code()),
					StatusMessage: span.Status().Message(),
				}

				// Add the span to the ring buffer
				GetTracesBuffer().Push(spanData)
			}
		}
	}

	// Forward the traces to the next consumer
	return tbp.nextConsumer.ConsumeTraces(ctx, td)
}

// metricsProcessor is a processor that buffers metrics data in memory.
type metricsProcessor struct {
	logger       *zap.Logger
	config       *Config
	nextConsumer consumer.Metrics
}

// Capabilities returns the capabilities of the processor.
func (mp *metricsProcessor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

// Shutdown stops the processor.
func (mp *metricsProcessor) Shutdown(ctx context.Context) error {
	// Nothing to do for shutdown
	return nil
}

// Start starts the processor.
func (mp *metricsProcessor) Start(ctx context.Context, host component.Host) error {
	// Nothing to do for start
	return nil
}

// newMetricsProcessor creates a new processor for metrics.
func newMetricsProcessor(logger *zap.Logger, config *Config, nextConsumer consumer.Metrics) (*metricsProcessor, error) {
	if nextConsumer == nil {
		return nil, fmt.Errorf("next consumer cannot be nil")
	}

	// Initialize the ring buffers if they haven't been initialized yet
	if !IsProcessorActive() {
		InitBuffers(config.TracesBufferSize, config.MetricsBufferSize, config.LogsBufferSize)
	}

	return &metricsProcessor{
		logger:       logger,
		config:       config,
		nextConsumer: nextConsumer,
	}, nil
}

// ConsumeMetrics implements the consumer.Metrics interface.
func (mp *metricsProcessor) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	// Process the metrics and store them in the ring buffer
	resourceMetrics := md.ResourceMetrics()
	for i := 0; i < resourceMetrics.Len(); i++ {
		rm := resourceMetrics.At(i)
		resource := rm.Resource()

		// Extract resource attributes
		resourceAttrs := make(map[string]interface{})
		resource.Attributes().Range(func(k string, v pcommon.Value) bool {
			resourceAttrs[k] = v.AsString()
			return true
		})

		scopeMetrics := rm.ScopeMetrics()
		for j := 0; j < scopeMetrics.Len(); j++ {
			sm := scopeMetrics.At(j)
			metrics := sm.Metrics()
			for k := 0; k < metrics.Len(); k++ {
				metric := metrics.At(k)

				// Create a simplified metric data structure
				metricData := &MetricData{
					Name:          metric.Name(),
					Description:   metric.Description(),
					Unit:          metric.Unit(),
					Type:          metric.Type().String(),
					Timestamp:     time.Now(), // Use current time as a simplification
					ResourceAttrs: resourceAttrs,
				}

				// Add the metric to the ring buffer
				GetMetricsBuffer().Push(metricData)
			}
		}
	}

	// Forward the metrics to the next consumer
	return mp.nextConsumer.ConsumeMetrics(ctx, md)
}

// logsProcessor is a processor that buffers logs data in memory.
type logsProcessor struct {
	logger       *zap.Logger
	config       *Config
	nextConsumer consumer.Logs
}

// Capabilities returns the capabilities of the processor.
func (lp *logsProcessor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

// Shutdown stops the processor.
func (lp *logsProcessor) Shutdown(ctx context.Context) error {
	// Nothing to do for shutdown
	return nil
}

// Start starts the processor.
func (lp *logsProcessor) Start(ctx context.Context, host component.Host) error {
	// Nothing to do for start
	return nil
}

// newLogsProcessor creates a new processor for logs.
func newLogsProcessor(logger *zap.Logger, config *Config, nextConsumer consumer.Logs) (*logsProcessor, error) {
	if nextConsumer == nil {
		return nil, fmt.Errorf("next consumer cannot be nil")
	}

	// Initialize the ring buffers if they haven't been initialized yet
	if !IsProcessorActive() {
		InitBuffers(config.TracesBufferSize, config.MetricsBufferSize, config.LogsBufferSize)
	}

	return &logsProcessor{
		logger:       logger,
		config:       config,
		nextConsumer: nextConsumer,
	}, nil
}

// ConsumeLogs implements the consumer.Logs interface.
func (lp *logsProcessor) ConsumeLogs(ctx context.Context, ld plog.Logs) error {
	// Process the logs and store them in the ring buffer
	resourceLogs := ld.ResourceLogs()
	for i := 0; i < resourceLogs.Len(); i++ {
		rl := resourceLogs.At(i)
		resource := rl.Resource()

		// Extract resource attributes
		resourceAttrs := make(map[string]interface{})
		resource.Attributes().Range(func(k string, v pcommon.Value) bool {
			resourceAttrs[k] = v.AsString()
			return true
		})

		scopeLogs := rl.ScopeLogs()
		for j := 0; j < scopeLogs.Len(); j++ {
			sl := scopeLogs.At(j)
			logs := sl.LogRecords()
			for k := 0; k < logs.Len(); k++ {
				log := logs.At(k)

				// Extract log attributes
				attrs := make(map[string]interface{})
				log.Attributes().Range(func(k string, v pcommon.Value) bool {
					attrs[k] = v.AsString()
					return true
				})

				// Create a simplified log data structure
				logData := &LogData{
					Timestamp:      log.Timestamp().AsTime(),
					SeverityText:   log.SeverityText(),
					SeverityNumber: int32(log.SeverityNumber()),
					Body:           log.Body().AsString(),
					Attributes:     attrs,
					ResourceAttrs:  resourceAttrs,
				}

				// Add the log to the ring buffer
				GetLogsBuffer().Push(logData)
			}
		}
	}

	// Forward the logs to the next consumer
	return lp.nextConsumer.ConsumeLogs(ctx, ld)
}

// NewFactory creates a factory for the telemetry buffer processor.
func NewFactory() processor.Factory {
	return processor.NewFactory(
		component.MustNewType("telemetry_buffer"),
		createDefaultConfig,
		processor.WithTraces(createTracesProcessor, component.StabilityLevelDevelopment),
		processor.WithMetrics(createMetricsProcessor, component.StabilityLevelDevelopment),
		processor.WithLogs(createLogsProcessor, component.StabilityLevelDevelopment),
	)
}

// createTracesProcessor creates a trace processor based on the config.
func createTracesProcessor(
	ctx context.Context,
	set processor.Settings,
	cfg component.Config,
	nextConsumer consumer.Traces,
) (processor.Traces, error) {
	pCfg := cfg.(*Config)
	return newTracesProcessor(set.Logger, pCfg, nextConsumer)
}

// createMetricsProcessor creates a metrics processor based on the config.
func createMetricsProcessor(
	ctx context.Context,
	set processor.Settings,
	cfg component.Config,
	nextConsumer consumer.Metrics,
) (processor.Metrics, error) {
	pCfg := cfg.(*Config)
	return newMetricsProcessor(set.Logger, pCfg, nextConsumer)
}

// createLogsProcessor creates a logs processor based on the config.
func createLogsProcessor(
	ctx context.Context,
	set processor.Settings,
	cfg component.Config,
	nextConsumer consumer.Logs,
) (processor.Logs, error) {
	pCfg := cfg.(*Config)
	return newLogsProcessor(set.Logger, pCfg, nextConsumer)
}
