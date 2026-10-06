package lab

import (
	"context"
	"os"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nv-minh/mq-redis-handbook/internal/testkit"
	"github.com/redis/go-redis/v9"
)

const blockTime = 200 * time.Millisecond

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

// env holds the shared commands client and the stream keys to delete when the test ends.
type env struct {
	t        *testing.T
	commands *redis.Client
	streams  []string
}

func newEnv(t *testing.T) *env {
	t.Helper()
	e := &env{t: t, commands: redis.NewClient(redisOptions(t))}
	t.Cleanup(func() {
		if len(e.streams) > 0 {
			// DEL removes the stream together with its consumer groups and their pending lists.
			e.commands.Del(context.Background(), e.streams...)
		}
		_ = e.commands.Close()
	})
	return e
}

func (e *env) streamKey() string {
	key := testkit.UniqueName("lab03-stream")
	e.streams = append(e.streams, key)
	return key
}

// newQueue builds one consumer-side object: shared commands client, its own blocking client.
func (e *env) newQueue(stream string) (*StreamQueue, *redis.Client) {
	e.t.Helper()
	blocking := redis.NewClient(redisOptions(e.t))
	e.t.Cleanup(func() { _ = blocking.Close() })
	return NewStreamQueue(Config{Commands: e.commands, Blocking: blocking, Stream: stream, BlockTime: blockTime}), blocking
}

func mustPublish(t *testing.T, q *StreamQueue, fields map[string]string) string {
	t.Helper()
	id, err := q.Publish(context.Background(), fields)
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	return id
}

func mustConsume(t *testing.T, q *StreamQueue, group, consumer string, count int) []Message {
	t.Helper()
	messages, err := q.Consume(context.Background(), group, consumer, count)
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	return messages
}

func mustPendingCount(t *testing.T, q *StreamQueue, group string) int64 {
	t.Helper()
	n, err := q.PendingCount(context.Background(), group)
	if err != nil {
		t.Fatalf("PendingCount: %v", err)
	}
	return n
}

func mustPending(t *testing.T, q *StreamQueue, group string) []PendingEntry {
	t.Helper()
	entries, err := q.PendingEntries(context.Background(), group)
	if err != nil {
		t.Fatalf("PendingEntries: %v", err)
	}
	return entries
}

func mustCreateGroup(t *testing.T, q *StreamQueue, group string) {
	t.Helper()
	if err := q.CreateGroup(context.Background(), group); err != nil {
		t.Fatalf("CreateGroup: %v", err)
	}
}

func ids(messages []Message) []string {
	out := make([]string, len(messages))
	for i, m := range messages {
		out[i] = m.ID
	}
	return out
}

// compareIDs orders stream ids by millisecond part, then by sequence part.
func compareIDs(a, b string) int {
	split := func(id string) (int64, int64) {
		ms, seq, _ := strings.Cut(id, "-")
		m, _ := strconv.ParseInt(ms, 10, 64)
		s, _ := strconv.ParseInt(seq, 10, 64)
		return m, s
	}
	am, as := split(a)
	bm, bs := split(b)
	if am != bm {
		return int(am - bm)
	}
	return int(as - bs)
}

func TestEachMessageGoesToOneConsumerInGroup(t *testing.T) {
	e := newEnv(t)
	stream := e.streamKey()
	group := testkit.UniqueName("workers")
	c1, _ := e.newQueue(stream)
	c2, _ := e.newQueue(stream)
	mustCreateGroup(t, c1, group)
	const total = 20
	published := make([]string, 0, total)
	for i := 0; i < total; i++ {
		published = append(published, mustPublish(t, c1, map[string]string{"n": strconv.Itoa(i)}))
	}

	// Two consumers of the same group read at the same time until everything has been delivered.
	var (
		mu       sync.Mutex
		received = map[string][]string{}
		wg       sync.WaitGroup
	)
	count := func() int {
		mu.Lock()
		defer mu.Unlock()
		return len(received["c1"]) + len(received["c2"])
	}
	readAll := func(q *StreamQueue, name string) {
		defer wg.Done()
		for count() < total {
			messages, err := q.Consume(context.Background(), group, name, 2)
			if err != nil {
				t.Errorf("Consume(%s): %v", name, err)
				return
			}
			mu.Lock()
			received[name] = append(received[name], ids(messages)...)
			mu.Unlock()
		}
	}
	wg.Add(2)
	go readAll(c1, "c1")
	go readAll(c2, "c2")
	wg.Wait()

	// Each id was delivered exactly once, and together the consumers got every message.
	all := append(append([]string{}, received["c1"]...), received["c2"]...)
	if len(all) != total {
		t.Fatalf("delivered %d messages, want %d", len(all), total)
	}
	sort.Slice(all, func(i, j int) bool { return compareIDs(all[i], all[j]) < 0 })
	if !slices.Equal(all, published) {
		t.Fatalf("delivered ids = %v, want each of %v exactly once", all, published)
	}

	// The server agrees: the pending list holds each id once, owned by the consumer that received it.
	pending := mustPending(t, c1, group)
	if len(pending) != total {
		t.Fatalf("pending entries = %d, want %d", len(pending), total)
	}
	for _, entry := range pending {
		if !slices.Contains(received[entry.Consumer], entry.ID) {
			t.Fatalf("entry %s is owned by %s but that consumer did not receive it", entry.ID, entry.Consumer)
		}
		if entry.DeliveryCount != 1 {
			t.Fatalf("entry %s delivery count = %d, want 1", entry.ID, entry.DeliveryCount)
		}
	}
}

func TestXackRemovesEntryFromPendingList(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t)
	q, _ := e.newQueue(e.streamKey())
	group := testkit.UniqueName("workers")
	mustCreateGroup(t, q, group)
	for i := 0; i < 3; i++ {
		mustPublish(t, q, map[string]string{"n": strconv.Itoa(i)})
	}

	messages := mustConsume(t, q, group, "c1", 10)
	if len(messages) != 3 {
		t.Fatalf("Consume returned %d messages, want 3", len(messages))
	}
	// Delivered but not acked: all three sit in the pending list (PEL).
	if n := mustPendingCount(t, q, group); n != 3 {
		t.Fatalf("pending before ack = %d, want 3", n)
	}

	acked, err := q.Ack(ctx, group, messages[0].ID)
	if err != nil || acked != 1 {
		t.Fatalf("Ack = %d, %v; want 1", acked, err)
	}
	if n := mustPendingCount(t, q, group); n != 2 {
		t.Fatalf("pending after one ack = %d, want 2", n)
	}
	// Acking the same id again removes nothing.
	acked, err = q.Ack(ctx, group, messages[0].ID)
	if err != nil || acked != 0 {
		t.Fatalf("second Ack = %d, %v; want 0", acked, err)
	}
	if n := mustPendingCount(t, q, group); n != 2 {
		t.Fatalf("pending after repeated ack = %d, want 2", n)
	}

	for _, m := range messages[1:] {
		if _, err := q.Ack(ctx, group, m.ID); err != nil {
			t.Fatal(err)
		}
	}
	if n := mustPendingCount(t, q, group); n != 0 {
		t.Fatalf("pending after all acks = %d, want 0", n)
	}
}

// waitUntilIdle polls (never sleeps) until the first pending entry has been idle for idleAtLeast.
// It returns the idle time it observed.
func waitUntilIdle(t *testing.T, q *StreamQueue, group string, idleAtLeast time.Duration) time.Duration {
	t.Helper()
	return testkit.Eventually(t, 10*time.Second, func() (time.Duration, bool) {
		entries, err := q.PendingEntries(context.Background(), group)
		if err != nil || len(entries) == 0 {
			return 0, false
		}
		return entries[0].Idle, entries[0].Idle >= idleAtLeast
	})
}

func TestXautoclaimRecoversPendingFromDeadConsumer(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t)
	stream := e.streamKey()
	group := testkit.UniqueName("workers")
	const minIdle = 200 * time.Millisecond
	dead, deadConn := e.newQueue(stream)
	rescuer, _ := e.newQueue(stream)
	mustCreateGroup(t, dead, group)
	id := mustPublish(t, dead, map[string]string{"job": "send-email"})

	// Consumer A reads the entry and "crashes": it never acks and its client is closed.
	delivered := mustConsume(t, dead, group, "consumer-a", 1)
	if !slices.Equal(ids(delivered), []string{id}) {
		t.Fatalf("delivered = %v, want [%s]", ids(delivered), id)
	}
	_ = deadConn.Close()

	// Wait (no sleep) until the entry has been idle for at least twice minIdle, so the claim
	// below is far from the boundary of "idle time greater than min-idle-time".
	idleBeforeClaim := waitUntilIdle(t, rescuer, group, 2*minIdle)

	claimed, err := rescuer.ClaimStale(ctx, group, "consumer-b", minIdle)
	if err != nil {
		t.Fatalf("ClaimStale: %v", err)
	}
	if len(claimed.Messages) != 1 || claimed.Messages[0].ID != id || claimed.Messages[0].Fields["job"] != "send-email" {
		t.Fatalf("claimed messages = %+v, want the one entry %s", claimed.Messages, id)
	}
	if len(claimed.DeletedIDs) != 0 {
		t.Fatalf("deleted ids = %v, want none", claimed.DeletedIDs)
	}

	// The entry moved to consumer B's pending list, and the claim counted as a second delivery.
	pending := mustPending(t, rescuer, group)
	if len(pending) != 1 || pending[0].ID != id || pending[0].Consumer != "consumer-b" || pending[0].DeliveryCount != 2 {
		t.Fatalf("pending after claim = %+v, want %s owned by consumer-b with delivery count 2", pending, id)
	}
	if pending[0].Idle >= idleBeforeClaim { // the claim reset the idle time
		t.Fatalf("idle after claim = %v, want less than the %v before it", pending[0].Idle, idleBeforeClaim)
	}

	// B finishes the job.
	if n, err := rescuer.Ack(ctx, group, id); err != nil || n != 1 {
		t.Fatalf("Ack = %d, %v; want 1", n, err)
	}
	if n := mustPendingCount(t, rescuer, group); n != 0 {
		t.Fatalf("pending after ack = %d, want 0", n)
	}
}

func TestClaimStaleWithNothingPendingReturnsEmptyResult(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t)
	q, _ := e.newQueue(e.streamKey())
	group := testkit.UniqueName("workers")
	mustCreateGroup(t, q, group)

	// Group exists but nothing was ever delivered.
	claimed, err := q.ClaimStale(ctx, group, "consumer-b", 100*time.Millisecond)
	if err != nil || len(claimed.Messages) != 0 || len(claimed.DeletedIDs) != 0 {
		t.Fatalf("ClaimStale on empty PEL = %+v, %v; want empty and no error", claimed, err)
	}

	// Delivered and acked: the pending list is empty again.
	id := mustPublish(t, q, map[string]string{"n": "1"})
	mustConsume(t, q, group, "consumer-a", 1)
	if _, err := q.Ack(ctx, group, id); err != nil {
		t.Fatal(err)
	}
	claimed, err = q.ClaimStale(ctx, group, "consumer-b", 100*time.Millisecond)
	if err != nil || len(claimed.Messages) != 0 || len(claimed.DeletedIDs) != 0 {
		t.Fatalf("ClaimStale after ack = %+v, %v; want empty and no error", claimed, err)
	}
}

func TestXautoclaimSkipsEntriesThatAreNotIdleLongEnough(t *testing.T) {
	e := newEnv(t)
	q, _ := e.newQueue(e.streamKey())
	group := testkit.UniqueName("workers")
	mustCreateGroup(t, q, group)
	id := mustPublish(t, q, map[string]string{"n": "1"})
	mustConsume(t, q, group, "consumer-a", 1)

	// One minute of min idle against an entry that was delivered milliseconds ago.
	claimed, err := q.ClaimStale(context.Background(), group, "consumer-b", time.Minute)
	if err != nil || len(claimed.Messages) != 0 {
		t.Fatalf("ClaimStale = %+v, %v; want nothing claimed", claimed, err)
	}
	pending := mustPending(t, q, group)
	if len(pending) != 1 || pending[0].ID != id || pending[0].Consumer != "consumer-a" || pending[0].DeliveryCount != 1 {
		t.Fatalf("pending = %+v, want %s still owned by consumer-a with delivery count 1", pending, id)
	}
}

func TestXautoclaimReportsIDsDeletedFromTheStream(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t)
	stream := e.streamKey()
	q, _ := e.newQueue(stream)
	group := testkit.UniqueName("workers")
	const minIdle = 100 * time.Millisecond
	mustCreateGroup(t, q, group)
	id := mustPublish(t, q, map[string]string{"n": "1"})
	mustConsume(t, q, group, "consumer-a", 1)
	// The entry leaves the stream (XDEL, or trimming) while it is still in the pending list.
	if n, err := e.commands.XDel(ctx, stream, id).Result(); err != nil || n != 1 {
		t.Fatalf("XDEL = %d, %v; want 1", n, err)
	}
	_ = waitUntilIdle(t, q, group, 2*minIdle)

	// Redis 7.0+: XAUTOCLAIM does not claim it, drops it from the PEL and returns its id as the
	// third element of the reply. The typed go-redis XAutoClaim drops that element, which is why
	// the lab parses the raw reply.
	claimed, err := q.ClaimStale(ctx, group, "consumer-b", minIdle)
	if err != nil {
		t.Fatalf("ClaimStale: %v", err)
	}
	if len(claimed.Messages) != 0 || !slices.Equal(claimed.DeletedIDs, []string{id}) {
		t.Fatalf("ClaimStale = %+v, want no messages and deleted ids [%s]", claimed, id)
	}
	if n := mustPendingCount(t, q, group); n != 0 {
		t.Fatalf("pending = %d, want 0", n)
	}
}

func TestConsumeReturnsEmptyListWhenNoNewMessageArrivesWithinTheBlockTime(t *testing.T) {
	e := newEnv(t)
	q, _ := e.newQueue(e.streamKey())
	group := testkit.UniqueName("workers")
	mustCreateGroup(t, q, group)

	started := time.Now()
	messages := mustConsume(t, q, group, "c1", 5)

	if len(messages) != 0 {
		t.Fatalf("Consume = %+v, want empty", messages)
	}
	if waited := time.Since(started); waited < blockTime-50*time.Millisecond {
		t.Fatalf("Consume returned after %v, want about the %v block time", waited, blockTime)
	}
}

func TestCreateGroupIsIdempotent(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t)
	q, _ := e.newQueue(e.streamKey())
	group := testkit.UniqueName("workers")
	mustCreateGroup(t, q, group)
	// The second XGROUP CREATE fails with BUSYGROUP on the server, and the lab swallows exactly that.
	if err := q.CreateGroup(ctx, group); err != nil {
		t.Fatalf("second CreateGroup = %v, want nil", err)
	}
	// Other errors are not swallowed: a group on a key of the wrong type is a WRONGTYPE error.
	notAStream := e.streamKey()
	if err := e.commands.Set(ctx, notAStream, "x", 0).Err(); err != nil {
		t.Fatal(err)
	}
	wrongType, _ := e.newQueue(notAStream)
	err := wrongType.CreateGroup(ctx, group)
	if err == nil || !strings.Contains(err.Error(), "WRONGTYPE") {
		t.Fatalf("CreateGroup on a string key = %v, want a WRONGTYPE error", err)
	}
}
