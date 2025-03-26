package telemetrybufferprocessor

import (
	"sync"
)

// RingBuffer is a thread-safe ring buffer implementation for storing telemetry data.
type RingBuffer struct {
	mu       sync.RWMutex
	data     []interface{}
	capacity int
	head     int
	size     int
	// For notifying subscribers about new data
	subscribers []chan interface{}
}

// NewRingBuffer creates a new ring buffer with the specified capacity.
func NewRingBuffer(capacity int) *RingBuffer {
	return &RingBuffer{
		data:        make([]interface{}, capacity),
		capacity:    capacity,
		subscribers: make([]chan interface{}, 0),
	}
}

// Push adds an item to the ring buffer, overwriting the oldest item if the buffer is full.
func (rb *RingBuffer) Push(item interface{}) {
	rb.mu.Lock()

	// Store the item
	rb.data[rb.head] = item
	rb.head = (rb.head + 1) % rb.capacity
	if rb.size < rb.capacity {
		rb.size++
	}

	// Notify subscribers (non-blocking)
	for _, ch := range rb.subscribers {
		select {
		case ch <- item:
			// Item sent
		default:
			// Channel full, skip notification
		}
	}

	rb.mu.Unlock()
}

// Snapshot returns a copy of all items currently in the buffer in chronological order.
func (rb *RingBuffer) Snapshot() []interface{} {
	rb.mu.RLock()
	defer rb.mu.RUnlock()

	if rb.size == 0 {
		return nil
	}

	result := make([]interface{}, rb.size)

	// Calculate the index of the oldest item
	start := (rb.head - rb.size + rb.capacity) % rb.capacity

	// Copy items in chronological order
	for i := 0; i < rb.size; i++ {
		idx := (start + i) % rb.capacity
		result[i] = rb.data[idx]
	}

	return result
}

// Size returns the current number of items in the buffer.
func (rb *RingBuffer) Size() int {
	rb.mu.RLock()
	defer rb.mu.RUnlock()
	return rb.size
}

// Capacity returns the maximum capacity of the buffer.
func (rb *RingBuffer) Capacity() int {
	rb.mu.RLock()
	defer rb.mu.RUnlock()
	return rb.capacity
}

// Subscribe creates a new subscription channel for receiving new items.
// The returned channel will receive new items as they are pushed to the buffer.
// The caller is responsible for closing the channel when done.
func (rb *RingBuffer) Subscribe(bufferSize int) chan interface{} {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	ch := make(chan interface{}, bufferSize)
	rb.subscribers = append(rb.subscribers, ch)
	return ch
}

// Unsubscribe removes a subscription channel.
func (rb *RingBuffer) Unsubscribe(ch chan interface{}) {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	for i, sub := range rb.subscribers {
		if sub == ch {
			// Remove the channel from the slice
			rb.subscribers = append(rb.subscribers[:i], rb.subscribers[i+1:]...)
			break
		}
	}
}
