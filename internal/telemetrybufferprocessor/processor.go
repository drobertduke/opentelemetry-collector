package telemetrybufferprocessor

import (
	"context"
	"log"
	"sync"
)

// TelemetryProcessor is a processor that stores telemetry data in ring buffers.
type TelemetryProcessor struct {
	tracesBuffer  *RingBuffer
	metricsBuffer *RingBuffer
	logsBuffer    *RingBuffer

	converter OTLPConverter

	mu sync.RWMutex
}

// NewTelemetryProcessor creates a new TelemetryProcessor with the specified configuration.
func NewTelemetryProcessor(cfg *Config) *TelemetryProcessor {
	return &TelemetryProcessor{
		tracesBuffer:  NewRingBuffer(cfg.TracesBufferSize),
		metricsBuffer: NewRingBuffer(cfg.MetricsBufferSize),
		logsBuffer:    NewRingBuffer(cfg.LogsBufferSize),
	}
}

// SetConverter sets the OTLP converter for the processor.
func (tp *TelemetryProcessor) SetConverter(converter OTLPConverter) {
	tp.mu.Lock()
	defer tp.mu.Unlock()
	tp.converter = converter
}

// ProcessTraces processes OTLP trace data and stores it in the traces buffer.
func (tp *TelemetryProcessor) ProcessTraces(ctx context.Context, data []byte) error {
	tp.mu.RLock()
	converter := tp.converter
	tp.mu.RUnlock()

	if converter == nil {
		log.Println("No converter set for TelemetryProcessor")
		return nil
	}

	spans, err := converter.ConvertTraces(data)
	if err != nil {
		return err
	}

	for _, span := range spans {
		tp.tracesBuffer.Push(span)
	}

	return nil
}

// ProcessMetrics processes OTLP metric data and stores it in the metrics buffer.
func (tp *TelemetryProcessor) ProcessMetrics(ctx context.Context, data []byte) error {
	tp.mu.RLock()
	converter := tp.converter
	tp.mu.RUnlock()

	if converter == nil {
		log.Println("No converter set for TelemetryProcessor")
		return nil
	}

	metrics, err := converter.ConvertMetrics(data)
	if err != nil {
		return err
	}

	for _, metric := range metrics {
		tp.metricsBuffer.Push(metric)
	}

	return nil
}

// ProcessLogs processes OTLP log data and stores it in the logs buffer.
func (tp *TelemetryProcessor) ProcessLogs(ctx context.Context, data []byte) error {
	tp.mu.RLock()
	converter := tp.converter
	tp.mu.RUnlock()

	if converter == nil {
		log.Println("No converter set for TelemetryProcessor")
		return nil
	}

	logs, err := converter.ConvertLogs(data)
	if err != nil {
		return err
	}

	for _, logItem := range logs {
		tp.logsBuffer.Push(logItem)
	}

	return nil
}

// GetTracesBuffer returns the traces ring buffer.
func (tp *TelemetryProcessor) GetTracesBuffer() *RingBuffer {
	return tp.tracesBuffer
}

// GetMetricsBuffer returns the metrics ring buffer.
func (tp *TelemetryProcessor) GetMetricsBuffer() *RingBuffer {
	return tp.metricsBuffer
}

// GetLogsBuffer returns the logs ring buffer.
func (tp *TelemetryProcessor) GetLogsBuffer() *RingBuffer {
	return tp.logsBuffer
}

// Global registry to allow the extension to find the processor
var (
	globalProcessor     *TelemetryProcessor
	globalProcessorLock sync.RWMutex
)

// RegisterProcessor registers a processor in the global registry.
func RegisterProcessor(processor *TelemetryProcessor) {
	globalProcessorLock.Lock()
	defer globalProcessorLock.Unlock()
	globalProcessor = processor
}

// GetGlobalProcessor returns the registered processor.
func GetGlobalProcessor() *TelemetryProcessor {
	globalProcessorLock.RLock()
	defer globalProcessorLock.RUnlock()
	return globalProcessor
}
