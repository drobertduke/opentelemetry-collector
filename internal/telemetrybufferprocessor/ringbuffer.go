package telemetrybufferprocessor

import (
	"sync"
	"time"
)

// RingBuffer is a thread-safe fixed-size buffer that overwrites the oldest data when full.
type RingBuffer struct {
	mu       sync.RWMutex
	data     []interface{}
	capacity int
	head     int
	size     int
}

// NewRingBuffer creates a new ring buffer with the specified capacity.
func NewRingBuffer(capacity int) *RingBuffer {
	return &RingBuffer{
		data:     make([]interface{}, capacity),
		capacity: capacity,
		head:     0,
		size:     0,
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

// GetAll returns all items in the ring buffer in chronological order (oldest first).
func (rb *RingBuffer) GetAll() []interface{} {
	rb.mu.RLock()
	defer rb.mu.RUnlock()

	if rb.size == 0 {
		return []interface{}{}
	}

	result := make([]interface{}, rb.size)

	// Calculate the index of the oldest item
	oldest := rb.head
	if rb.size == rb.capacity {
		oldest = rb.head
	} else {
		oldest = 0
	}

	// Copy items in chronological order
	for i := 0; i < rb.size; i++ {
		idx := (oldest + i) % rb.capacity
		result[i] = rb.data[idx]
	}

	return result
}

// GetByTimeRange returns items within the specified time range.
func (rb *RingBuffer) GetByTimeRange(startTime, endTime time.Time) []interface{} {
	rb.mu.RLock()
	defer rb.mu.RUnlock()

	if rb.size == 0 {
		return []interface{}{}
	}

	var result []interface{}

	// Calculate the index of the oldest item
	oldest := rb.head
	if rb.size == rb.capacity {
		oldest = rb.head
	} else {
		oldest = 0
	}

	// Copy items in chronological order that fall within the time range
	for i := 0; i < rb.size; i++ {
		idx := (oldest + i) % rb.capacity
		item := rb.data[idx]

		// Check if the item implements the TelemetryItem interface
		if telemetryItem, ok := item.(TelemetryItem); ok {
			timestamp := telemetryItem.GetTimestamp()
			if (startTime.IsZero() || !timestamp.Before(startTime)) &&
				(endTime.IsZero() || !timestamp.After(endTime)) {
				result = append(result, item)
			}
		}
	}

	return result
}

// GetByTraceID returns items with the specified trace ID.
func (rb *RingBuffer) GetByTraceID(traceID string) []interface{} {
	rb.mu.RLock()
	defer rb.mu.RUnlock()

	if rb.size == 0 || traceID == "" {
		return []interface{}{}
	}

	var result []interface{}

	// Calculate the index of the oldest item
	oldest := rb.head
	if rb.size == rb.capacity {
		oldest = rb.head
	} else {
		oldest = 0
	}

	// Copy items that match the trace ID
	for i := 0; i < rb.size; i++ {
		idx := (oldest + i) % rb.capacity
		item := rb.data[idx]

		// Check if the item implements the TelemetryItem interface
		if telemetryItem, ok := item.(TelemetryItem); ok {
			if telemetryItem.GetTraceID() == traceID {
				result = append(result, item)
			}
		}
	}

	return result
}

// GetByServiceName returns items with the specified service name.
func (rb *RingBuffer) GetByServiceName(serviceName string) []interface{} {
	rb.mu.RLock()
	defer rb.mu.RUnlock()

	if rb.size == 0 || serviceName == "" {
		return []interface{}{}
	}

	var result []interface{}

	// Calculate the index of the oldest item
	oldest := rb.head
	if rb.size == rb.capacity {
		oldest = rb.head
	} else {
		oldest = 0
	}

	// Copy items that match the service name
	for i := 0; i < rb.size; i++ {
		idx := (oldest + i) % rb.capacity
		item := rb.data[idx]

		// Check if the item implements the TelemetryItem interface
		if telemetryItem, ok := item.(TelemetryItem); ok {
			if telemetryItem.GetServiceName() == serviceName {
				result = append(result, item)
			}
		}
	}

	return result
}

// Size returns the current number of items in the ring buffer.
func (rb *RingBuffer) Size() int {
	rb.mu.RLock()
	defer rb.mu.RUnlock()
	return rb.size
}

// Capacity returns the maximum capacity of the ring buffer.
func (rb *RingBuffer) Capacity() int {
	return rb.capacity
}

// Clear removes all items from the ring buffer.
func (rb *RingBuffer) Clear() {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	rb.head = 0
	rb.size = 0
	// Optionally clear references to help GC
	for i := range rb.data {
		rb.data[i] = nil
	}
}
