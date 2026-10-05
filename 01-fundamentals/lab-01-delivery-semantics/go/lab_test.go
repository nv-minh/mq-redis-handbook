package lab

import (
	"errors"
	"slices"
	"sync"
	"testing"

	"github.com/nv-minh/mq-redis-handbook/internal/testkit"
)

// scripted returns a rand() that replays a fixed script, so a "random" loss is decided by the test.
func scripted(t *testing.T, values ...float64) func() float64 {
	i := 0
	return func() float64 {
		if i >= len(values) {
			// Called from the queue goroutine, where Fatalf is not allowed.
			t.Errorf("scripted rand exhausted after %d values", len(values))
			return 1
		}
		v := values[i]
		i++
		return v
	}
}

func count(ids []string, want string) int {
	n := 0
	for _, id := range ids {
		if id == want {
			n++
		}
	}
	return n
}

func TestAtMostOnceLosesMessagesWhenConsumerCrashes(t *testing.T) {
	m1, m2, m3 := testkit.UniqueName("m1"), testkit.UniqueName("m2"), testkit.UniqueName("m3")
	q := NewQueue(Options{Mode: AtMostOnce, LossRate: 0})
	var attempts, processed []string
	q.Consume(func(id string) error {
		attempts = append(attempts, id)
		// The consumer crashes while handling m2: the handler returns an error before finishing.
		if id == m2 {
			return errors.New("consumer crashed")
		}
		processed = append(processed, id)
		return nil
	})

	for _, id := range []string{m1, m2, m3} {
		q.Publish(id)
	}
	q.Drain()

	// m2 was handed over once and never redelivered, so its work is lost for good.
	if !slices.Equal(attempts, []string{m1, m2, m3}) {
		t.Fatalf("attempts = %v", attempts)
	}
	if !slices.Equal(processed, []string{m1, m3}) {
		t.Fatalf("processed = %v", processed)
	}
	if dead := q.DeadLetters(); len(dead) != 0 {
		t.Fatalf("dead = %v, want none", dead)
	}
}

func TestAtMostOnceDropsEveryDeliveryWhenLossRateIsOne(t *testing.T) {
	q := NewQueue(Options{Mode: AtMostOnce, LossRate: 1})
	var seen []string
	q.Consume(func(id string) error { seen = append(seen, id); return nil })
	for _, id := range []string{"a", "b", "c"} {
		q.Publish(id)
	}
	q.Drain()
	if len(seen) != 0 {
		t.Fatalf("seen = %v, want none", seen)
	}
}

func TestAtLeastOnceRedeliversWhenConsumerCrashes(t *testing.T) {
	id := testkit.UniqueName("m")
	q := NewQueue(Options{Mode: AtLeastOnce, LossRate: 0})
	attempts := 0
	var processed []string
	q.Consume(func(got string) error {
		attempts++
		if attempts == 1 {
			return errors.New("consumer crashed") // no ack, so the broker redelivers
		}
		processed = append(processed, got)
		return nil
	})
	q.Publish(id)
	q.Drain()
	if attempts != 2 || !slices.Equal(processed, []string{id}) {
		t.Fatalf("attempts = %d, processed = %v", attempts, processed)
	}
}

func TestAtLeastOnceDuplicatesWhenAckIsLost(t *testing.T) {
	// LossRate = 1: every ack is lost, so every delivery is retried until MaxDeliveries.
	q := NewQueue(Options{Mode: AtLeastOnce, LossRate: 1, MaxDeliveries: 3})
	a, b := testkit.UniqueName("a"), testkit.UniqueName("b")
	var seen []string
	q.Consume(func(id string) error { seen = append(seen, id); return nil })
	q.Publish(a)
	q.Publish(b)
	q.Drain()

	if got := count(seen, a); got != 3 {
		t.Fatalf("deliveries of a = %d, want 3", got)
	}
	if got := count(seen, b); got != 3 {
		t.Fatalf("deliveries of b = %d, want 3", got)
	}
	// The handler did run to completion each time; only the ack went missing.
	// After MaxDeliveries the broker gives up and parks the message as dead.
	dead := q.DeadLetters()
	slices.Sort(dead)
	want := []string{a, b}
	slices.Sort(want)
	if !slices.Equal(dead, want) {
		t.Fatalf("dead = %v, want %v", dead, want)
	}
}

func TestAtLeastOnceStopsRedeliveringOnceAnAckGetsThrough(t *testing.T) {
	// First ack lost (0 < 0.5), second ack arrives (0.9 >= 0.5): exactly one duplicate.
	q := NewQueue(Options{Mode: AtLeastOnce, LossRate: 0.5, Rand: scripted(t, 0, 0.9)})
	id := testkit.UniqueName("m")
	var seen []string
	q.Consume(func(got string) error { seen = append(seen, got); return nil })
	q.Publish(id)
	q.Drain()
	if !slices.Equal(seen, []string{id, id}) {
		t.Fatalf("seen = %v", seen)
	}
	if dead := q.DeadLetters(); len(dead) != 0 {
		t.Fatalf("dead = %v, want none", dead)
	}
}

func TestIdempotentConsumerAppliesEachIDOnce(t *testing.T) {
	q := NewQueue(Options{Mode: AtLeastOnce, LossRate: 1, MaxDeliveries: 4})
	ids := []string{testkit.UniqueName("a"), testkit.UniqueName("b"), testkit.UniqueName("c")}
	var applied []string
	deliveries := 0
	handler := NewIdempotentHandler(func(id string) error {
		applied = append(applied, id)
		return nil
	})
	q.Consume(func(id string) error {
		deliveries++
		return handler(id)
	})
	for _, id := range ids {
		q.Publish(id)
	}
	q.Drain()

	if deliveries != len(ids)*4 { // duplicates really arrived
		t.Fatalf("deliveries = %d, want %d", deliveries, len(ids)*4)
	}
	slices.Sort(applied) // yet each id was applied once
	want := slices.Clone(ids)
	slices.Sort(want)
	if !slices.Equal(applied, want) {
		t.Fatalf("applied = %v, want %v", applied, want)
	}
}

func TestIdempotentConsumerRetriesAnIDWhoseApplyFailed(t *testing.T) {
	calls := 0
	var applied []string
	handler := NewIdempotentHandler(func(id string) error {
		calls++
		if calls == 1 {
			return errors.New("side effect failed")
		}
		applied = append(applied, id)
		return nil
	})
	if err := handler("x"); err == nil {
		t.Fatal("first call should fail")
	}
	_ = handler("x") // not a duplicate: the first attempt never completed
	_ = handler("x") // now it is a duplicate
	if !slices.Equal(applied, []string{"x"}) || calls != 2 {
		t.Fatalf("applied = %v, calls = %d", applied, calls)
	}
}

// Publishing from many goroutines while draining must be race-free (run with -race).
func TestQueueIsSafeForConcurrentPublish(t *testing.T) {
	q := NewQueue(Options{Mode: AtLeastOnce, LossRate: 0})
	var mu sync.Mutex
	got := 0
	q.Consume(func(string) error { mu.Lock(); got++; mu.Unlock(); return nil })
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); q.Publish(testkit.UniqueName("m")) }()
	}
	wg.Wait()
	q.Drain()
	mu.Lock()
	defer mu.Unlock()
	if got != 50 {
		t.Fatalf("got = %d, want 50", got)
	}
}
