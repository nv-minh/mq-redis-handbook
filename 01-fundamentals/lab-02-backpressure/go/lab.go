// Package lab is a bounded in-memory queue with backpressure: when the queue is full, Publish
// blocks until a consumer has made room or the context is done. The producer is slowed to the
// consumer's pace instead of the queue growing without limit.
package lab

import (
	"context"
	"errors"
	"sync"
)

// ErrClosed is returned by Publish after Close.
var ErrClosed = errors.New("lab: queue closed")

// BoundedQueue holds at most capacity messages. It is safe for concurrent use.
type BoundedQueue[T any] struct {
	items     chan T
	done      chan struct{}
	closeOnce sync.Once
}

// NewBoundedQueue returns a queue that holds at most capacity messages. capacity must be >= 1.
func NewBoundedQueue[T any](capacity int) *BoundedQueue[T] {
	if capacity < 1 {
		panic("lab: capacity must be >= 1")
	}
	return &BoundedQueue[T]{items: make(chan T, capacity), done: make(chan struct{})}
}

// Publish blocks while the queue is full. It returns nil once the message is queued,
// ctx.Err() if ctx is done first, or ErrClosed if the queue was closed.
func (q *BoundedQueue[T]) Publish(ctx context.Context, msg T) error {
	select {
	case <-q.done:
		return ErrClosed
	default:
	}
	select {
	case q.items <- msg:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-q.done:
		return ErrClosed
	}
}

// Consume starts one sequential consumer goroutine that runs until Close. The message being
// handled has already left the queue, so it does not count towards Size.
func (q *BoundedQueue[T]) Consume(handler func(T)) {
	go func() {
		for {
			select {
			case msg := <-q.items:
				handler(msg)
			case <-q.done:
				return
			}
		}
	}()
}

// Size is the number of messages waiting in the queue, never more than the capacity.
func (q *BoundedQueue[T]) Size() int { return len(q.items) }

// Close stops the consumer and fails pending and future Publish calls with ErrClosed.
// It is safe to call more than once.
func (q *BoundedQueue[T]) Close() { q.closeOnce.Do(func() { close(q.done) }) }
