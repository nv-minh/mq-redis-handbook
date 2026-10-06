package lab

import (
	"context"
	"os"
	"regexp"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/nv-minh/mq-redis-handbook/internal/testkit"
	"github.com/redis/go-redis/v9"
)

func redisOptions(t *testing.T) *redis.Options {
	t.Helper()
	url := os.Getenv("REDIS_URL")
	if url == "" {
		url = "redis://127.0.0.1:6379"
	}
	opts, err := redis.ParseURL(url)
	if err != nil {
		t.Fatalf("parse REDIS_URL: %v", err)
	}
	return opts
}

// env holds the shared commands client and a cleanup list for one test.
type env struct {
	t        *testing.T
	commands *redis.Client
	keys     []string
}

func newEnv(t *testing.T) *env {
	t.Helper()
	e := &env{t: t, commands: redis.NewClient(redisOptions(t))}
	t.Cleanup(func() {
		if len(e.keys) > 0 {
			e.commands.Del(context.Background(), e.keys...)
		}
		_ = e.commands.Close()
	})
	return e
}

func (e *env) queueKey() string {
	key := testkit.UniqueName("lab03-list")
	e.keys = append(e.keys, key)
	return key
}

// newQueue builds one consumer-side queue: shared commands client, its own blocking client.
func (e *env) newQueue(queueKey string, blockTimeout time.Duration, consumerIDs ...string) (*ReliableQueue, *redis.Client) {
	e.t.Helper()
	blocking := redis.NewClient(redisOptions(e.t))
	e.t.Cleanup(func() { _ = blocking.Close() })
	queue := NewReliableQueue(Config{Commands: e.commands, Blocking: blocking, Queue: queueKey, BlockTimeout: blockTimeout})
	for _, id := range consumerIDs {
		e.keys = append(e.keys, queue.ProcessingKey(id))
	}
	return queue, blocking
}

func (e *env) list(key string) []string {
	e.t.Helper()
	items, err := e.commands.LRange(context.Background(), key, 0, -1).Result()
	if err != nil {
		e.t.Fatalf("LRANGE %s: %v", key, err)
	}
	return items
}

func mustDequeue(t *testing.T, q *ReliableQueue, consumerID string) string {
	t.Helper()
	msg, ok, err := q.Dequeue(context.Background(), consumerID)
	if err != nil || !ok {
		t.Fatalf("Dequeue(%s) = %q, %v, %v; want a message", consumerID, msg, ok, err)
	}
	return msg
}

func assertList(t *testing.T, what string, got, want []string) {
	t.Helper()
	if !slices.Equal(got, want) {
		t.Fatalf("%s = %v, want %v", what, got, want)
	}
}

var blockedRe = regexp.MustCompile(`blocked_clients:(\d+)`)

// blockedClients reads blocked_clients from INFO: clients waiting in a blocking command.
func blockedClients(t *testing.T, rdb *redis.Client) int {
	t.Helper()
	info, err := rdb.Info(context.Background(), "clients").Result()
	if err != nil {
		t.Fatalf("INFO clients: %v", err)
	}
	match := blockedRe.FindStringSubmatch(info)
	if match == nil {
		return 0
	}
	n, _ := strconv.Atoi(match[1])
	return n
}

func TestMessageStaysInProcessingListUntilAck(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t)
	queueKey := e.queueKey()
	queue, _ := e.newQueue(queueKey, time.Second, "worker-1")
	if err := queue.Enqueue(ctx, "job-1"); err != nil {
		t.Fatal(err)
	}
	assertList(t, "queue", e.list(queueKey), []string{"job-1"})

	if got := mustDequeue(t, queue, "worker-1"); got != "job-1" {
		t.Fatalf("Dequeue = %q, want job-1", got)
	}

	// BLMOVE moved the message atomically: gone from the queue, parked in the processing list.
	assertList(t, "queue", e.list(queueKey), []string{})
	assertList(t, "processing", e.list(queue.ProcessingKey("worker-1")), []string{"job-1"})

	// Only the ack removes it from the processing list.
	acked, err := queue.Ack(ctx, "worker-1", "job-1")
	if err != nil || !acked {
		t.Fatalf("Ack = %v, %v; want true", acked, err)
	}
	assertList(t, "processing", e.list(queue.ProcessingKey("worker-1")), []string{})
}

func TestMessageIsRecoveredAfterConsumerCrash(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t)
	queueKey := e.queueKey()
	crashing, crashingConn := e.newQueue(queueKey, time.Second)
	survivor, _ := e.newQueue(queueKey, time.Second, "worker-a", "worker-b")
	if err := survivor.Enqueue(ctx, "job-1"); err != nil {
		t.Fatal(err)
	}

	// worker-a takes the message and "crashes": it never acks, and its connection is dropped.
	if got := mustDequeue(t, crashing, "worker-a"); got != "job-1" {
		t.Fatalf("Dequeue = %q, want job-1", got)
	}
	_ = crashingConn.Close()
	assertList(t, "processing of worker-a", e.list(survivor.ProcessingKey("worker-a")), []string{"job-1"})
	assertList(t, "queue", e.list(queueKey), []string{})

	// A recovery pass puts the dead consumer's message back on the queue.
	recovered, err := survivor.RecoverStale(ctx, "worker-a")
	if err != nil || recovered != 1 {
		t.Fatalf("RecoverStale = %d, %v; want 1", recovered, err)
	}
	assertList(t, "processing of worker-a", e.list(survivor.ProcessingKey("worker-a")), []string{})

	// Another consumer now receives the very same message: at-least-once delivery.
	if got := mustDequeue(t, survivor, "worker-b"); got != "job-1" {
		t.Fatalf("Dequeue after recovery = %q, want job-1", got)
	}
	acked, err := survivor.Ack(ctx, "worker-b", "job-1")
	if err != nil || !acked {
		t.Fatalf("Ack = %v, %v; want true", acked, err)
	}
	assertList(t, "queue", e.list(queueKey), []string{})
	assertList(t, "processing of worker-b", e.list(survivor.ProcessingKey("worker-b")), []string{})
}

func TestDequeueOnEmptyQueueReturnsNilAfterTheBlockTimeout(t *testing.T) {
	e := newEnv(t)
	queue, _ := e.newQueue(e.queueKey(), time.Second, "worker-1")

	started := time.Now()
	msg, ok, err := queue.Dequeue(context.Background(), "worker-1")
	waited := time.Since(started)

	if err != nil || ok || msg != "" {
		t.Fatalf("Dequeue on empty queue = %q, %v, %v; want no message and no error", msg, ok, err)
	}
	// BLMOVE really blocked for about the timeout (it cannot return earlier on an empty list),
	// and an empty result leaves nothing in the processing list.
	if waited < 900*time.Millisecond {
		t.Fatalf("Dequeue returned after %v, want about the 1s block timeout", waited)
	}
	assertList(t, "processing", e.list(queue.ProcessingKey("worker-1")), []string{})
}

func TestBlockedDequeueWakesUpWhenAMessageArrives(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t)
	queue, _ := e.newQueue(e.queueKey(), 10*time.Second, "worker-1")
	blockedBefore := blockedClients(t, e.commands)

	type result struct {
		msg string
		ok  bool
		err error
	}
	done := make(chan result, 1)
	go func() {
		msg, ok, err := queue.Dequeue(ctx, "worker-1")
		done <- result{msg, ok, err}
	}()
	// Wait until the server reports one more blocked client: BLMOVE is parked on the empty list.
	testkit.Eventually(t, 5*time.Second, func() (struct{}, bool) {
		return struct{}{}, blockedClients(t, e.commands) > blockedBefore
	})

	if err := queue.Enqueue(ctx, "late-job"); err != nil {
		t.Fatal(err)
	}
	// The push wakes the blocked client right away, long before the 10 second timeout.
	select {
	case r := <-done:
		if r.err != nil || !r.ok || r.msg != "late-job" {
			t.Fatalf("Dequeue = %q, %v, %v; want late-job", r.msg, r.ok, r.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("blocked Dequeue did not wake up after Enqueue")
	}
}

func TestMessagesAreDeliveredInFifoOrder(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t)
	queue, _ := e.newQueue(e.queueKey(), time.Second, "worker-1")
	for _, msg := range []string{"a", "b", "c"} {
		if err := queue.Enqueue(ctx, msg); err != nil {
			t.Fatal(err)
		}
	}
	for _, want := range []string{"a", "b", "c"} {
		if got := mustDequeue(t, queue, "worker-1"); got != want {
			t.Fatalf("Dequeue = %q, want %q", got, want)
		}
	}
}

func TestRecoverStaleOnEmptyProcessingListReturnsZero(t *testing.T) {
	e := newEnv(t)
	queue, _ := e.newQueue(e.queueKey(), time.Second)
	n, err := queue.RecoverStale(context.Background(), "nobody")
	if err != nil || n != 0 {
		t.Fatalf("RecoverStale = %d, %v; want 0", n, err)
	}
}

func TestRecoveredMessagesAreRedeliveredInTheirOriginalOrder(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t)
	queueKey := e.queueKey()
	crashing, crashingConn := e.newQueue(queueKey, time.Second)
	survivor, _ := e.newQueue(queueKey, time.Second, "worker-a", "worker-b")
	for _, msg := range []string{"a", "b", "c"} {
		if err := survivor.Enqueue(ctx, msg); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 3; i++ {
		mustDequeue(t, crashing, "worker-a")
	}
	_ = crashingConn.Close()

	recovered, err := survivor.RecoverStale(ctx, "worker-a")
	if err != nil || recovered != 3 {
		t.Fatalf("RecoverStale = %d, %v; want 3", recovered, err)
	}
	for _, want := range []string{"a", "b", "c"} {
		if got := mustDequeue(t, survivor, "worker-b"); got != want {
			t.Fatalf("Dequeue after recovery = %q, want %q", got, want)
		}
	}
}

func TestAckOfAMessageNotInTheProcessingListReturnsFalse(t *testing.T) {
	e := newEnv(t)
	queue, _ := e.newQueue(e.queueKey(), time.Second, "worker-1")
	acked, err := queue.Ack(context.Background(), "worker-1", "never-dequeued")
	if err != nil || acked {
		t.Fatalf("Ack = %v, %v; want false", acked, err)
	}
}

// Go only: the typed BLMove command of go-redis extends the socket read deadline by the block
// time, so a client ReadTimeout shorter than the block time does not break it.
func TestTypedBlockingCommandOutlivesClientReadTimeout(t *testing.T) {
	e := newEnv(t)
	opts := redisOptions(t)
	opts.ReadTimeout = 300 * time.Millisecond
	blocking := redis.NewClient(opts)
	t.Cleanup(func() { _ = blocking.Close() })
	queue := NewReliableQueue(Config{Commands: e.commands, Blocking: blocking, Queue: e.queueKey(), BlockTimeout: time.Second})
	e.keys = append(e.keys, queue.ProcessingKey("worker-1"))

	msg, ok, err := queue.Dequeue(context.Background(), "worker-1")
	if err != nil || ok || msg != "" {
		t.Fatalf("Dequeue = %q, %v, %v; want a clean empty result after 1s despite ReadTimeout 300ms", msg, ok, err)
	}
}
