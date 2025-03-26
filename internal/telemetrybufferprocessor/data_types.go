package telemetrybufferprocessor

import (
	"sync"
	"time"
)

// Global ring buffers and access functions
var (
	tracesRingBuffer  *RingBuffer
	metricsRingBuffer *RingBuffer
	logsRingBuffer    *RingBuffer
	processorActive   bool
	mu                sync.RWMutex
)

// SpanData represents a simplified span for storage in the ring buffer.
type SpanData struct {
	TraceID       string
	SpanID        string
	ParentSpanID  string
	Name          string
	StartTime     time.Time
	EndTime       time.Time
	Attributes    map[string]interface{}
	ResourceAttrs map[string]interface{}
	StatusCode    int32
	StatusMessage string
}

// MetricData represents a simplified metric for storage in the ring buffer.
type MetricData struct {
	Name          string
	Description   string
	Unit          string
	Type          string // gauge, counter, histogram, etc.
	Timestamp     time.Time
	Value         interface{} // Could be a single value or a more complex structure
	Attributes    map[string]interface{}
	ResourceAttrs map[string]interface{}
}

// LogData represents a simplified log record for storage in the ring buffer.
type LogData struct {
	Timestamp      time.Time
	SeverityText   string
	SeverityNumber int32
	Body           string
	Attributes     map[string]interface{}
	ResourceAttrs  map[string]interface{}
}

// InitBuffers initializes the global ring buffers.
func InitBuffers(tracesSize, metricsSize, logsSize int) {
	mu.Lock()
	defer mu.Unlock()

	tracesRingBuffer = NewRingBuffer(tracesSize)
	metricsRingBuffer = NewRingBuffer(metricsSize)
	logsRingBuffer = NewRingBuffer(logsSize)
	processorActive = true
}

// GetTracesBuffer returns the global traces ring buffer.
func GetTracesBuffer() *RingBuffer {
	mu.RLock()
	defer mu.RUnlock()
	return tracesRingBuffer
}

// GetMetricsBuffer returns the global metrics ring buffer.
func GetMetricsBuffer() *RingBuffer {
	mu.RLock()
	defer mu.RUnlock()
	return metricsRingBuffer
}

// GetLogsBuffer returns the global logs ring buffer.
func GetLogsBuffer() *RingBuffer {
	mu.RLock()
	defer mu.RUnlock()
	return logsRingBuffer
}

// IsProcessorActive returns whether the processor is active.
func IsProcessorActive() bool {
	mu.RLock()
	defer mu.RUnlock()
	return processorActive
}

// SetProcessorActive sets whether the processor is active.
func SetProcessorActive(active bool) {
	mu.Lock()
	defer mu.Unlock()
	processorActive = active
}
