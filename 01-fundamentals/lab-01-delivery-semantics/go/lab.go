// Package lab is an in-memory broker that shows what delivery semantics mean, with no network.
// A message goes pending -> delivered to the handler -> (maybe) acked. Loss is injected through
// Options.LossRate and Options.Rand, so every failure in the lab is reproducible.
package lab

import (
	"math/rand"
	"sync"
)

// Mode selects the delivery guarantee.
type Mode string

const (
	AtMostOnce  Mode = "at-most-once"
	AtLeastOnce Mode = "at-least-once"
)

const defaultMaxDeliveries = 5

// Options configures a Queue.
type Options struct {
	Mode Mode
	// LossRate is the probability (0..1) that something goes wrong in transit.
	// AtMostOnce: the delivery is lost, the handler never sees the message.
	// AtLeastOnce: the consumer's ack is lost, so the broker redelivers (duplicates).
	// 1 means it always happens, 0 means it never does.
	LossRate float64
	// Rand is the source of randomness in [0, 1). Defaults to rand.Float64.
	Rand func() float64
	// MaxDeliveries (AtLeastOnce only) is the number of deliveries before the message is parked
	// as dead. Defaults to 5.
	MaxDeliveries int
}

// Handler processes one message. Returning an error models a consumer crash:
// AtMostOnce has already forgotten the message, AtLeastOnce never gets the ack and retries.
type Handler func(id string) error

// Queue is an in-memory broker. It is safe for concurrent use.
type Queue struct {
	opts Options

	mu         sync.Mutex
	cond       *sync.Cond
	pending    []string
	deliveries map[string]int
	dead       []string
	handler    Handler
	running    bool
}

// NewQueue returns an empty Queue.
func NewQueue(opts Options) *Queue {
	if opts.Rand == nil {
		opts.Rand = rand.Float64
	}
	if opts.MaxDeliveries <= 0 {
		opts.MaxDeliveries = defaultMaxDeliveries
	}
	q := &Queue{opts: opts, deliveries: map[string]int{}}
	q.cond = sync.NewCond(&q.mu)
	return q
}

// Publish enqueues a message. Delivery happens asynchronously once a consumer is registered.
func (q *Queue) Publish(id string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.pending = append(q.pending, id)
	q.kick()
}

// Consume registers the consumer and starts delivering pending messages.
func (q *Queue) Consume(h Handler) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.handler = h
	q.kick()
}

// Drain blocks until every message that can be delivered has been settled (acked, lost or dead).
// With no consumer registered it returns immediately.
func (q *Queue) Drain() {
	q.mu.Lock()
	defer q.mu.Unlock()
	for q.running {
		q.cond.Wait()
	}
}

// DeadLetters returns the ids that used up MaxDeliveries without an ack.
func (q *Queue) DeadLetters() []string {
	q.mu.Lock()
	defer q.mu.Unlock()
	return append([]string(nil), q.dead...)
}

// kick starts the delivery goroutine if there is work and none is running. Caller holds q.mu.
func (q *Queue) kick() {
	if !q.running && q.handler != nil && len(q.pending) > 0 {
		q.running = true
		go q.run()
	}
}

func (q *Queue) run() {
	q.mu.Lock()
	defer q.mu.Unlock()
	for q.handler != nil && len(q.pending) > 0 {
		id := q.pending[0]
		q.pending = q.pending[1:]
		h := q.handler
		q.mu.Unlock()
		q.deliver(id, h)
		q.mu.Lock()
	}
	q.running = false
	q.cond.Broadcast()
}

// deliver is only ever called from the single run goroutine, so it may use rand unguarded,
// but it must take q.mu before touching shared state.
func (q *Queue) deliver(id string, h Handler) {
	if q.opts.Mode == AtMostOnce {
		// Fire and forget: the broker drops the message before or while handing it over.
		if q.opts.Rand() < q.opts.LossRate {
			return
		}
		_ = h(id) // a crash changes nothing: the message is already gone
		return
	}

	err := h(id)
	acked := err == nil && q.opts.Rand() >= q.opts.LossRate // the ack travels back and may be lost

	q.mu.Lock()
	defer q.mu.Unlock()
	attempt := q.deliveries[id] + 1
	switch {
	case acked:
		delete(q.deliveries, id)
	case attempt >= q.opts.MaxDeliveries:
		delete(q.deliveries, id)
		q.dead = append(q.dead, id)
	default:
		q.deliveries[id] = attempt
		q.pending = append(q.pending, id) // redeliver, behind the messages already waiting
	}
}

// NewIdempotentHandler wraps apply so redelivered ids are skipped. The id is recorded before
// apply runs and removed again if apply fails, otherwise a failed attempt would be mistaken for
// a duplicate forever. In-memory only: a real consumer must store the marker atomically with
// the side effect.
func NewIdempotentHandler(apply Handler) Handler {
	var mu sync.Mutex
	seen := map[string]struct{}{}
	return func(id string) error {
		mu.Lock()
		if _, dup := seen[id]; dup {
			mu.Unlock()
			return nil
		}
		seen[id] = struct{}{}
		mu.Unlock()

		if err := apply(id); err != nil {
			mu.Lock()
			delete(seen, id)
			mu.Unlock()
			return err
		}
		return nil
	}
}
