package lab

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nv-minh/mq-redis-handbook/internal/testkit"
)

// gatedConsumer parks on every message until the test releases it.
type gatedConsumer struct {
	mu       sync.Mutex
	taken    []int
	took     chan struct{} // one signal per message taken
	releases chan struct{}
}

func newGatedConsumer() *gatedConsumer {
	return &gatedConsumer{took: make(chan struct{}, 64), releases: make(chan struct{})}
}

func (g *gatedConsumer) handle(msg int) {
	g.mu.Lock()
	g.taken = append(g.taken, msg)
	g.mu.Unlock()
	g.took <- struct{}{}
	<-g.releases
}

func (g *gatedConsumer) takenSnapshot() []int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return slices.Clone(g.taken)
}

func TestPublishBlocksWhenQueueFull(t *testing.T) {
	for _, capacity := range []int{1, 3} {
		q := NewBoundedQueue[int](capacity)
		t.Cleanup(q.Close)
		for i := 0; i < capacity; i++ {
			if err := q.Publish(context.Background(), i); err != nil { // none of these block
				t.Fatalf("capacity %d: publish %d: %v", capacity, i, err)
			}
		}
		if q.Size() != capacity {
			t.Fatalf("capacity %d: size = %d", capacity, q.Size())
		}

		// A full queue holds the publisher until its context gives up: it did not return early.
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		err := q.Publish(ctx, capacity)
		cancel()
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("capacity %d: blocked publish returned %v, want DeadlineExceeded", capacity, err)
		}
		if q.Size() != capacity {
			t.Fatalf("capacity %d: size after abandoned publish = %d", capacity, q.Size())
		}

		// Consuming one message makes room, and a waiting publish completes.
		var published atomic.Bool
		var publishErr atomic.Value
		go func() {
			if err := q.Publish(context.Background(), capacity); err != nil {
				publishErr.Store(err)
			}
			published.Store(true)
		}()
		consumer := newGatedConsumer()
		q.Consume(consumer.handle)
		testkit.Eventually(t, 2*time.Second, func() (bool, bool) { return published.Load(), published.Load() })
		if err, _ := publishErr.Load().(error); err != nil {
			t.Fatalf("capacity %d: unblocked publish failed: %v", capacity, err)
		}
		if got := consumer.takenSnapshot(); !slices.Equal(got, []int{0}) { // FIFO: oldest first
			t.Fatalf("capacity %d: taken = %v, want [0]", capacity, got)
		}
		if q.Size() != capacity { // the freed slot was refilled by the blocked publish
			t.Fatalf("capacity %d: size = %d, want full", capacity, q.Size())
		}
		q.Close() // unblocks the parked handler's queue goroutine; release it too
		close(consumer.releases)
	}
}

func TestPublishReturnsErrClosedAfterClose(t *testing.T) {
	q := NewBoundedQueue[int](1)
	q.Close()
	if err := q.Publish(context.Background(), 1); !errors.Is(err, ErrClosed) {
		t.Fatalf("err = %v, want ErrClosed", err)
	}
}

func TestSizeNeverExceedsCapacityWithSlowConsumer(t *testing.T) {
	const capacity, total = 3, 20
	q := NewBoundedQueue[int](capacity)
	t.Cleanup(q.Close)

	var maxSize atomic.Int64
	observe := func() {
		n := int64(q.Size())
		for {
			cur := maxSize.Load()
			if n <= cur || maxSize.CompareAndSwap(cur, n) {
				return
			}
		}
	}

	// A fast producer: 20 publishes back to back, each one waits for room.
	var produced atomic.Int64
	go func() {
		for i := 0; i < total; i++ {
			if err := q.Publish(context.Background(), i); err != nil {
				t.Errorf("publish %d: %v", i, err)
				return
			}
			observe()
			produced.Add(1)
		}
	}()

	// A slow consumer: it holds each message until the test lets it go.
	consumer := newGatedConsumer()
	q.Consume(func(msg int) {
		observe()
		consumer.handle(msg)
	})

	for done := 0; done < total; done++ {
		select {
		case <-consumer.took:
		case <-time.After(2 * time.Second):
			t.Fatalf("consumer did not take message %d in time", done)
		}
		observe()
		consumer.releases <- struct{}{}
	}
	testkit.Eventually(t, 2*time.Second, func() (bool, bool) {
		ok := produced.Load() == total
		return ok, ok
	})

	want := make([]int, total)
	for i := range want {
		want[i] = i
	}
	if got := consumer.takenSnapshot(); !slices.Equal(got, want) {
		t.Fatalf("taken = %v, want %v", got, want)
	}
	if got := maxSize.Load(); got > capacity {
		t.Fatalf("max size = %d, exceeds capacity %d", got, capacity)
	} else if got != capacity { // the bound was actually reached, so the check has teeth
		t.Fatalf("max size = %d, expected the producer to fill the queue to %d", got, capacity)
	}
}
