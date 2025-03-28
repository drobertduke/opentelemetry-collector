package telemetrybufferprocessor

import (
	"sync"
	"time"
)

// RingBuffer is a thread-safe ring buffer for storing telemetry data.
type RingBuffer struct {
	mu       sync.RWMutex
	data     []interface{}
	capacity int
	head     int
	size     int
}

// NewRingBuffer creates a new RingBuffer with the specified capacity.
func NewRingBuffer(capacity int) *RingBuffer {
	return &RingBuffer{
		data:     make([]interface{}, capacity),
		capacity: capacity,
	}
}

// Push adds an item to the ring buffer, overwriting the oldest item if the buffer is full.
func (rb *RingBuffer) Push(item interface{}) {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	rb.data[rb.head] = item
	rb.head = (rb.head + 1) % rb.capacity
	if rb.size < rb.capacity {
		rb.size++
	}
}

// GetAll returns all items in the ring buffer in chronological order.
func (rb *RingBuffer) GetAll() []interface{} {
	rb.mu.RLock()
	defer rb.mu.RUnlock()

	if rb.size == 0 {
		return nil
	}

	result := make([]interface{}, rb.size)
	start := (rb.head - rb.size + rb.capacity) % rb.capacity
	for i := 0; i < rb.size; i++ {
		idx := (start + i) % rb.capacity
		result[i] = rb.data[idx]
	}
	return result
}

// Size returns the number of items in the ring buffer.
func (rb *RingBuffer) Size() int {
	rb.mu.RLock()
	defer rb.mu.RUnlock()
	return rb.size
}

// Capacity returns the capacity of the ring buffer.
func (rb *RingBuffer) Capacity() int {
	return rb.capacity
}

// GetByTraceID returns all items with the specified trace ID.
func (rb *RingBuffer) GetByTraceID(traceID string) []interface{} {
	items := rb.GetAll()
	if items == nil {
		return nil
	}

	var result []interface{}
	for _, item := range items {
		if telemetryItem, ok := item.(TelemetryItem); ok {
			if telemetryItem.GetTraceID() == traceID {
				result = append(result, item)
			}
		}
	}
	return result
}

// GetByServiceName returns all items with the specified service name.
func (rb *RingBuffer) GetByServiceName(serviceName string) []interface{} {
	items := rb.GetAll()
	if items == nil {
		return nil
	}

	var result []interface{}
	for _, item := range items {
		if telemetryItem, ok := item.(TelemetryItem); ok {
			if telemetryItem.GetServiceName() == serviceName {
				result = append(result, item)
			}
		}
	}
	return result
}

// GetByTimeRange returns all items within the specified time range.
func (rb *RingBuffer) GetByTimeRange(startTime, endTime time.Time) []interface{} {
	items := rb.GetAll()
	if items == nil {
		return nil
	}

	var result []interface{}
	for _, item := range items {
		if telemetryItem, ok := item.(TelemetryItem); ok {
			timestamp := telemetryItem.GetTimestamp()
			if !startTime.IsZero() && timestamp.Before(startTime) {
				continue
			}
			if !endTime.IsZero() && timestamp.After(endTime) {
				continue
			}
			result = append(result, item)
		}
	}
	return result
}
