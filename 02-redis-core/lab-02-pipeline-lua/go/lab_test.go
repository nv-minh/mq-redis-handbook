package lab

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/nv-minh/mq-redis-handbook/internal/testkit"
	"github.com/redis/go-redis/v9"
)

const callers = 50

func redisURL() string {
	if url := os.Getenv("REDIS_URL"); url != "" {
		return url
	}
	return "redis://127.0.0.1:6379"
}

func options(t *testing.T) *redis.Options {
	t.Helper()
	opts, err := redis.ParseURL(redisURL())
	if err != nil {
		t.Fatalf("parse REDIS_URL: %v", err)
	}
	return opts
}

// newKeys returns a registrar: every key it hands out is deleted when the test ends.
func newKeys(t *testing.T, rdb *redis.Client) func(label string) string {
	t.Helper()
	var keys []string
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if len(keys) > 0 {
			rdb.Del(ctx, keys...)
		}
	})
	return func(label string) string {
		key := testkit.UniqueName("lab02-" + label)
		keys = append(keys, key)
		return key
	}
}

func newClient(t *testing.T) *redis.Client {
	t.Helper()
	rdb := redis.NewClient(options(t))
	t.Cleanup(func() { _ = rdb.Close() })
	return rdb
}

// hammer runs n DecrIfPositive calls that all start at the same moment and returns how many succeeded.
func hammer(t *testing.T, rdb *redis.Client, key string, n int) int {
	t.Helper()
	ctx := context.Background()
	var (
		wg        sync.WaitGroup
		mu        sync.Mutex
		successes int
	)
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start // release every goroutine at once
			ok, err := DecrIfPositive(ctx, rdb, key)
			if err != nil {
				t.Errorf("DecrIfPositive: %v", err)
				return
			}
			if ok {
				mu.Lock()
				successes++
				mu.Unlock()
			}
		}()
	}
	close(start)
	wg.Wait()
	return successes
}

func value(t *testing.T, rdb *redis.Client, key string) string {
	t.Helper()
	v, err := rdb.Get(context.Background(), key).Result()
	if err != nil {
		t.Fatalf("GET %s: %v", key, err)
	}
	return v
}

func TestPipelineUsesFewerRoundTripsThanSequential(t *testing.T) {
	ctx := context.Background()
	rdb, counter := NewCountingClient(options(t))
	t.Cleanup(func() { _ = rdb.Close() })
	newKey := newKeys(t, rdb)
	prefix := newKey("pipe")
	keys := make([]string, 100)
	for i := range keys {
		keys[i] = fmt.Sprintf("%s:%d", prefix, i)
	}
	t.Cleanup(func() { rdb.Del(context.Background(), keys...) })

	// Warm up both connection pools (HELLO and friends are writes too), then count from zero.
	if err := SetSequential(ctx, rdb, prefix, 1); err != nil {
		t.Fatal(err)
	}
	if err := SetPipelined(ctx, rdb, prefix, 1); err != nil {
		t.Fatal(err)
	}

	counter.Reset()
	if err := SetSequential(ctx, rdb, prefix, 100); err != nil {
		t.Fatal(err)
	}
	sequentialTrips := counter.Writes()

	counter.Reset()
	if err := SetPipelined(ctx, rdb, prefix, 100); err != nil {
		t.Fatal(err)
	}
	pipelinedTrips := counter.Writes()

	// Each command is one write + one reply; the pipeline sends all 100 in one write.
	if sequentialTrips != 100 {
		t.Fatalf("sequential round trips = %d, want 100", sequentialTrips)
	}
	if pipelinedTrips != 1 {
		t.Fatalf("pipelined round trips = %d, want 1", pipelinedTrips)
	}
	// Fewer round trips, same effect: all 100 keys exist.
	n, err := rdb.Exists(ctx, keys...).Result()
	if err != nil || n != 100 {
		t.Fatalf("EXISTS = %d, %v; want 100", n, err)
	}
}

func TestDecrIfPositiveNeverGoesBelowZeroUnder50ConcurrentCallers(t *testing.T) {
	rdb := newClient(t)
	key := newKeys(t, rdb)("never-negative")
	if err := rdb.Set(context.Background(), key, 10, 0).Err(); err != nil {
		t.Fatal(err)
	}

	successes := hammer(t, rdb, key, callers)

	// 50 callers raced for 10 units: the counter stops at 0 and never turns negative.
	if got := value(t, rdb, key); got != "0" {
		t.Fatalf("counter = %s, want 0", got)
	}
	if successes > 10 {
		t.Fatalf("successes = %d, want <= 10", successes)
	}
}

func TestExactlyNCallersSucceedWhenCounterIsN(t *testing.T) {
	rdb := newClient(t)
	newKey := newKeys(t, rdb)
	for _, n := range []int{1, 20, callers} {
		key := newKey("exact-" + strconv.Itoa(n))
		if err := rdb.Set(context.Background(), key, n, 0).Err(); err != nil {
			t.Fatal(err)
		}
		if got := hammer(t, rdb, key, callers); got != n {
			t.Fatalf("counter = %d: %d callers succeeded, want exactly %d", n, got, n)
		}
		if got := value(t, rdb, key); got != "0" {
			t.Fatalf("counter = %s after draining %d, want 0", got, n)
		}
	}
}

func TestDecrIfPositiveReturnsFalseAndCreatesNothingForAMissingKey(t *testing.T) {
	ctx := context.Background()
	rdb := newClient(t)
	key := newKeys(t, rdb)("missing")
	ok, err := DecrIfPositive(ctx, rdb, key)
	if err != nil || ok {
		t.Fatalf("DecrIfPositive = %v, %v; want false, nil", ok, err)
	}
	if n, _ := rdb.Exists(ctx, key).Result(); n != 0 {
		t.Fatalf("key was created by a failed decrement")
	}
}

func TestDecrIfPositiveReturnsFalseAtZero(t *testing.T) {
	ctx := context.Background()
	rdb := newClient(t)
	key := newKeys(t, rdb)("zero")
	if err := rdb.Set(ctx, key, 0, 0).Err(); err != nil {
		t.Fatal(err)
	}
	ok, err := DecrIfPositive(ctx, rdb, key)
	if err != nil || ok {
		t.Fatalf("DecrIfPositive = %v, %v; want false, nil", ok, err)
	}
	if got := value(t, rdb, key); got != "0" {
		t.Fatalf("counter = %s, want 0", got)
	}
}

func TestNoscriptAfterScriptFlushIsRecoveredByTheClient(t *testing.T) {
	ctx := context.Background()
	rdb := newClient(t)
	key := newKeys(t, rdb)("noscript")
	if err := rdb.Set(ctx, key, 2, 0).Err(); err != nil {
		t.Fatal(err)
	}
	if ok, err := DecrIfPositive(ctx, rdb, key); err != nil || !ok {
		t.Fatalf("first DecrIfPositive = %v, %v; want true, nil", ok, err)
	}

	// The server forgets every cached script (this also happens on restart or failover).
	if err := rdb.ScriptFlush(ctx).Err(); err != nil {
		t.Fatal(err)
	}

	// EVALSHA fails with NOSCRIPT inside redis.Script.Run, which falls back to EVAL.
	if ok, err := DecrIfPositive(ctx, rdb, key); err != nil || !ok {
		t.Fatalf("DecrIfPositive after SCRIPT FLUSH = %v, %v; want true, nil", ok, err)
	}
	if got := value(t, rdb, key); got != "0" {
		t.Fatalf("counter = %s, want 0", got)
	}
}
