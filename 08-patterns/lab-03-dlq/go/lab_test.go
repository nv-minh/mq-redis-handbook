package lab

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"os/exec"
	"reflect"
	"slices"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/nv-minh/mq-redis-handbook/internal/testkit"
	"github.com/redis/go-redis/v9"
)

const (
	blockTime = 100 * time.Millisecond
	minIdle   = 200 * time.Millisecond
	// helperEnv bật chế độ helper process: test binary được chạy lại để đóng vai consumer bị kill.
	helperEnv = "LAB03_HELPER"
)

func redisOptions(t testing.TB) *redis.Options {
	t.Helper()
	url := os.Getenv("REDIS_URL")
	if url == "" {
		url = "redis://127.0.0.1:6379"
	}
	opts, err := redis.ParseURL(url)
	if err != nil {
		t.Fatalf("REDIS_URL không hợp lệ: %v", err)
	}
	return opts
}

// names gom các key của một test. Chúng được đăng ký để xóa trước khi dùng.
type names struct {
	stream, group, dlq, marker string
}

type env struct {
	t        *testing.T
	commands *redis.Client
}

func newEnv(t *testing.T) *env {
	t.Helper()
	e := &env{t: t, commands: redis.NewClient(redisOptions(t))}
	t.Cleanup(func() { _ = e.commands.Close() })
	return e
}

func (e *env) names() names {
	stream := testkit.UniqueName("lab03-stream")
	n := names{stream: stream, group: testkit.UniqueName("lab03-group"), dlq: stream + ":dlq", marker: stream + ":started"}
	e.t.Cleanup(func() {
		// DEL xóa stream cùng với các consumer group và pending list của chúng.
		e.commands.Del(context.Background(), n.stream, n.dlq, n.marker)
	})
	return n
}

func (e *env) queue(n names, consumer string) *Queue {
	e.t.Helper()
	blocking := redis.NewClient(redisOptions(e.t))
	e.t.Cleanup(func() { _ = blocking.Close() })
	return NewQueue(Config{
		Commands: e.commands, Blocking: blocking,
		Stream: n.stream, Group: n.group, DLQStream: n.dlq,
		Consumer: consumer, MinIdle: minIdle, BlockTime: blockTime,
	})
}

func mustEnsureGroup(t *testing.T, q *Queue) {
	t.Helper()
	if err := q.EnsureGroup(context.Background()); err != nil {
		t.Fatalf("EnsureGroup lỗi: %v", err)
	}
}

func mustPublish(t *testing.T, q *Queue, fields map[string]string) string {
	t.Helper()
	id, err := q.Publish(context.Background(), fields)
	if err != nil {
		t.Fatalf("Publish lỗi: %v", err)
	}
	return id
}

func mustConsume(t *testing.T, q *Queue, maxAttempts int, h Handler) Stats {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	stats, err := q.ConsumeWithDLQ(ctx, Options{MaxAttempts: maxAttempts}, h)
	if err != nil {
		t.Fatalf("ConsumeWithDLQ lỗi: %v", err)
	}
	return stats
}

func mustDLQ(t *testing.T, q *Queue) []Message {
	t.Helper()
	entries, err := q.DLQEntries(context.Background())
	if err != nil {
		t.Fatalf("DLQEntries lỗi: %v", err)
	}
	return entries
}

func mustPending(t *testing.T, q *Queue) []PendingEntry {
	t.Helper()
	entries, err := q.PendingEntries(context.Background())
	if err != nil {
		t.Fatalf("PendingEntries lỗi: %v", err)
	}
	return entries
}

func TestPoisonMessageMovesToDlqAfterMaxAttemptsWithErrorMetadata(t *testing.T) {
	e := newEnv(t)
	n := e.names()
	q := e.queue(n, "consumer-1")
	mustEnsureGroup(t, q)
	id := mustPublish(t, q, map[string]string{"order": "42", "note": "poison"})

	var attempts []int64
	before := time.Now()
	stats := mustConsume(t, q, 3, func(_ context.Context, d Delivery) error {
		attempts = append(attempts, d.DeliveryCount)
		return errors.New("cannot parse order 42")
	})
	after := time.Now()

	// Handler chạy đúng maxAttempts lần, delivery count (từ XPENDING) tăng 1, 2, 3.
	if !slices.Equal(attempts, []int64{1, 2, 3}) {
		t.Fatalf("attempts = %v, mong đợi [1 2 3]", attempts)
	}
	if want := (Stats{Succeeded: 0, Failed: 3, DeadLettered: 1}); stats != want {
		t.Fatalf("stats = %+v, mong đợi %+v", stats, want)
	}

	dlq := mustDLQ(t, q)
	if len(dlq) != 1 {
		t.Fatalf("DLQ có %d entry, mong đợi 1", len(dlq))
	}
	fields := dlq[0].Fields
	keys := slices.Sorted(maps.Keys(fields))
	if !slices.Equal(keys, []string{"attempts", "error", "failed_at", "original_id", "payload"}) {
		t.Fatalf("các field của DLQ entry = %v", keys)
	}
	if fields["original_id"] != id || fields["error"] != "cannot parse order 42" || fields["attempts"] != "3" {
		t.Fatalf("metadata sai: %v", fields)
	}
	failedAt, err := time.Parse("2006-01-02T15:04:05.000Z07:00", fields["failed_at"])
	if err != nil {
		t.Fatalf("failed_at %q không đúng định dạng ISO 8601 có mili giây: %v", fields["failed_at"], err)
	}
	if failedAt.Before(before.Add(-time.Second)) || failedAt.After(after.Add(time.Second)) {
		t.Fatalf("failed_at %s ngoài khoảng [%s, %s]", failedAt, before, after)
	}
	var payload map[string]string
	if err := json.Unmarshal([]byte(fields["payload"]), &payload); err != nil {
		t.Fatalf("payload không phải JSON: %v", err)
	}
	if !reflect.DeepEqual(payload, map[string]string{"order": "42", "note": "poison"}) {
		t.Fatalf("payload = %v", payload)
	}

	// Entry gốc đã được XACK: pending list sạch.
	if p := mustPending(t, q); len(p) != 0 {
		t.Fatalf("pending list còn %d entry, mong đợi 0", len(p))
	}
}

func TestHandlerThatRecoversBeforeMaxAttemptsIsNotDeadLettered(t *testing.T) {
	e := newEnv(t)
	q := e.queue(e.names(), "consumer-1")
	mustEnsureGroup(t, q)
	mustPublish(t, q, map[string]string{"order": "7"})

	var attempts []int64
	stats := mustConsume(t, q, 5, func(_ context.Context, d Delivery) error {
		attempts = append(attempts, d.DeliveryCount)
		if d.DeliveryCount < 3 {
			return errors.New("lỗi tạm thời")
		}
		return nil
	})

	if !slices.Equal(attempts, []int64{1, 2, 3}) {
		t.Fatalf("attempts = %v, mong đợi [1 2 3]", attempts)
	}
	if want := (Stats{Succeeded: 1, Failed: 2, DeadLettered: 0}); stats != want {
		t.Fatalf("stats = %+v, mong đợi %+v", stats, want)
	}
	if len(mustDLQ(t, q)) != 0 || len(mustPending(t, q)) != 0 {
		t.Fatal("DLQ và pending list phải rỗng")
	}
}

func TestMaxAttemptsOneSendsFirstFailureStraightToDlq(t *testing.T) {
	e := newEnv(t)
	q := e.queue(e.names(), "consumer-1")
	mustEnsureGroup(t, q)
	id := mustPublish(t, q, map[string]string{"order": "1"})

	calls := 0
	stats := mustConsume(t, q, 1, func(context.Context, Delivery) error {
		calls++
		return errors.New("lỗi ngay lần đầu")
	})

	if calls != 1 {
		t.Fatalf("handler chạy %d lần, mong đợi 1", calls)
	}
	if want := (Stats{Succeeded: 0, Failed: 1, DeadLettered: 1}); stats != want {
		t.Fatalf("stats = %+v, mong đợi %+v", stats, want)
	}
	dlq := mustDLQ(t, q)
	if len(dlq) != 1 || dlq[0].Fields["original_id"] != id || dlq[0].Fields["attempts"] != "1" {
		t.Fatalf("DLQ = %+v", dlq)
	}
	if len(mustPending(t, q)) != 0 {
		t.Fatal("pending list phải rỗng")
	}
}

func TestEntryDeliveredMoreThanMaxAttemptsBeforeHandlerRunsGoesToDlq(t *testing.T) {
	// Consumer chết ở lần giao cuối cùng (không kịp báo lỗi): delivery count vượt maxAttempts,
	// và message phải vào DLQ thay vì làm consumer kế tiếp chết lặp lại mãi.
	e := newEnv(t)
	n := e.names()
	q := e.queue(n, "consumer-1")
	mustEnsureGroup(t, q)
	id := mustPublish(t, q, map[string]string{"order": "9"})
	// Lần giao thứ nhất: đọc thẳng bằng XREADGROUP rồi bỏ đó, như một consumer đã chết.
	if err := e.commands.Do(context.Background(), "XREADGROUP", "GROUP", n.group, "dead-consumer", "STREAMS", n.stream, ">").Err(); err != nil {
		t.Fatalf("XREADGROUP lỗi: %v", err)
	}

	calls := 0
	stats := mustConsume(t, q, 1, func(context.Context, Delivery) error {
		calls++
		return nil
	})

	if calls != 0 {
		t.Fatalf("handler chạy %d lần, mong đợi 0", calls)
	}
	if want := (Stats{DeadLettered: 1}); stats != want {
		t.Fatalf("stats = %+v, mong đợi %+v", stats, want)
	}
	dlq := mustDLQ(t, q)
	if len(dlq) != 1 || dlq[0].Fields["original_id"] != id || dlq[0].Fields["attempts"] != "2" {
		t.Fatalf("DLQ = %+v", dlq)
	}
	if len(mustPending(t, q)) != 0 {
		t.Fatal("pending list phải rỗng")
	}
}

func TestEmptyStreamConsumeReturnsWithoutError(t *testing.T) {
	e := newEnv(t)
	q := e.queue(e.names(), "consumer-1")
	mustEnsureGroup(t, q)

	calls := 0
	stats := mustConsume(t, q, 3, func(context.Context, Delivery) error {
		calls++
		return nil
	})

	if calls != 0 || stats != (Stats{}) {
		t.Fatalf("calls = %d, stats = %+v, mong đợi toàn 0", calls, stats)
	}
	if len(mustDLQ(t, q)) != 0 {
		t.Fatal("DLQ phải rỗng")
	}
}

func TestMaxAttemptsMustBeAPositiveInteger(t *testing.T) {
	e := newEnv(t)
	q := e.queue(e.names(), "consumer-1")
	for _, maxAttempts := range []int{0, -1} {
		_, err := q.ConsumeWithDLQ(context.Background(), Options{MaxAttempts: maxAttempts}, func(context.Context, Delivery) error { return nil })
		if !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("maxAttempts %d: err = %v, mong đợi ErrInvalidArgument", maxAttempts, err)
		}
	}
}

func TestReplayDlqReturnsMessagesToMainStream(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t)
	n := e.names()
	q := e.queue(n, "consumer-1")
	mustEnsureGroup(t, q)
	// Replay một DLQ rỗng: 0, không lỗi.
	if got, err := q.ReplayDLQ(ctx, 5); err != nil || got != 0 {
		t.Fatalf("replay DLQ rỗng: got %d, err %v", got, err)
	}

	originals := []map[string]string{{"order": "1"}, {"order": "2"}, {"order": "3"}}
	for _, fields := range originals {
		mustPublish(t, q, fields)
	}
	// maxAttempts 1: mỗi message lỗi một lần là vào thẳng DLQ.
	failed := mustConsume(t, q, 1, func(context.Context, Delivery) error { return errors.New("lỗi do bug đã được sửa sau đó") })
	if failed.DeadLettered != 3 || len(mustDLQ(t, q)) != 3 {
		t.Fatalf("stats = %+v, DLQ = %d entry", failed, len(mustDLQ(t, q)))
	}

	// limit nhỏ hơn kích thước DLQ: chỉ chuyển đúng limit entry, cũ nhất trước.
	steps := []struct{ limit, want, left int }{
		{2, 2, 1},
		// limit lớn hơn kích thước DLQ: trả số entry thực sự đã chuyển.
		{100, 1, 0},
		// DLQ đã rỗng.
		{100, 0, 0},
	}
	for _, s := range steps {
		got, err := q.ReplayDLQ(ctx, s.limit)
		if err != nil || got != s.want {
			t.Fatalf("ReplayDLQ(%d) = %d, %v; mong đợi %d", s.limit, got, err, s.want)
		}
		if left := len(mustDLQ(t, q)); left != s.left {
			t.Fatalf("sau ReplayDLQ(%d) DLQ còn %d, mong đợi %d", s.limit, left, s.left)
		}
	}

	// Main stream nhận lại đúng field gốc (3 entry gốc, rồi 3 entry replay theo cùng thứ tự).
	main, err := e.commands.XRange(ctx, n.stream, "-", "+").Result()
	if err != nil {
		t.Fatalf("XRANGE lỗi: %v", err)
	}
	var mainFields []map[string]string
	for _, m := range main {
		fields := map[string]string{}
		for k, v := range m.Values {
			fields[k] = fmt.Sprint(v)
		}
		mainFields = append(mainFields, fields)
	}
	if want := append(slices.Clone(originals), originals...); !reflect.DeepEqual(mainFields, want) {
		t.Fatalf("main stream = %v, mong đợi %v", mainFields, want)
	}

	// Bug đã sửa: consumer xử lý thành công các message vừa replay.
	var seen []map[string]string
	ok := mustConsume(t, q, 3, func(_ context.Context, d Delivery) error {
		seen = append(seen, d.Fields)
		return nil
	})
	if want := (Stats{Succeeded: 3}); ok != want {
		t.Fatalf("stats = %+v, mong đợi %+v", ok, want)
	}
	if !reflect.DeepEqual(seen, originals) {
		t.Fatalf("handler thấy %v, mong đợi %v", seen, originals)
	}
}

func TestReplayDlqRejectsANonPositiveLimit(t *testing.T) {
	e := newEnv(t)
	q := e.queue(e.names(), "consumer-1")
	for _, limit := range []int{0, -1} {
		if _, err := q.ReplayDLQ(context.Background(), limit); !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("limit %d: err = %v, mong đợi ErrInvalidArgument", limit, err)
		}
	}
}

// safeBuffer cho phép đọc stderr của child trong khi exec còn ghi vào nó.
type safeBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *safeBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *safeBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func TestChaosConsumerKilledMidProcessingMessageIsReclaimed(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t)
	n := e.names()
	parent := e.queue(n, "rescuer")
	mustEnsureGroup(t, parent)
	id := mustPublish(t, parent, map[string]string{"job": "send-invoice"})

	// Helper process: chạy lại chính test binary này, chỉ chạy TestHelperProcess.
	childConsumer := testkit.UniqueName("victim")
	cmd := exec.Command(os.Args[0], "-test.run=^TestHelperProcess$")
	cmd.Env = append(os.Environ(),
		helperEnv+"=1",
		"STREAM="+n.stream, "GROUP="+n.group, "DLQ_STREAM="+n.dlq,
		"CONSUMER="+childConsumer, "MARKER_KEY="+n.marker,
	)
	stderr := &safeBuffer{}
	cmd.Stdout, cmd.Stderr = stderr, stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("không khởi động được helper process: %v", err)
	}
	// Luôn kill và reap child, kể cả khi test fail: không để process mồ côi.
	waited := make(chan error, 1)
	go func() { waited <- cmd.Wait() }()
	reaped := false
	reap := func() error {
		if reaped {
			return nil
		}
		reaped = true
		_ = cmd.Process.Kill() // SIGKILL, bỏ qua lỗi "process đã thoát"
		select {
		case err := <-waited:
			return err
		case <-time.After(10 * time.Second):
			return errors.New("child không thoát sau SIGKILL")
		}
	}
	t.Cleanup(func() { _ = reap() })
	fail := func(format string, args ...any) {
		t.Helper()
		t.Fatalf(format+"\nstderr của child:\n%s", append(args, stderr.String())...)
	}

	// Chờ tới khi child báo đã nhận message (không sleep, không đoán thời gian khởi động).
	startedID := testkit.Eventually(t, 30*time.Second, func() (string, bool) {
		v, err := e.commands.Get(ctx, n.marker).Result()
		return v, err == nil
	})
	if startedID != id {
		fail("child báo bắt đầu message %q, mong đợi %q", startedID, id)
	}
	pending := mustPending(t, parent)
	if len(pending) != 1 || pending[0].ID != id || pending[0].Consumer != childConsumer || pending[0].DeliveryCount != 1 {
		fail("pending = %+v, mong đợi một entry của %s với delivery count 1", pending, childConsumer)
	}

	// SIGKILL: process chết ngay giữa handler, không kịp XACK hay dọn gì.
	err := reap()
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) {
		fail("Wait trả %v, mong đợi *exec.ExitError", err)
	}
	if ws, ok := exitErr.Sys().(syscall.WaitStatus); !ok || !ws.Signaled() || ws.Signal() != syscall.SIGKILL {
		fail("child không chết vì SIGKILL: %v", exitErr)
	}

	// Message vẫn nằm trong PEL của consumer đã chết. Chờ tới khi idle gấp đôi minIdle,
	// để lần claim cách xa ranh giới min-idle-time.
	testkit.Eventually(t, 10*time.Second, func() (struct{}, bool) {
		entries, err := parent.PendingEntries(ctx)
		return struct{}{}, err == nil && len(entries) == 1 && entries[0].Idle >= 2*minIdle
	})

	var handled []Delivery
	stats := mustConsume(t, parent, 3, func(_ context.Context, d Delivery) error {
		handled = append(handled, d)
		return nil
	})

	// Message được consumer thứ hai tiếp quản (lần giao thứ hai) và xử lý thành công.
	want := []Delivery{{ID: id, Fields: map[string]string{"job": "send-invoice"}, DeliveryCount: 2}}
	if !reflect.DeepEqual(handled, want) {
		t.Fatalf("handled = %+v, mong đợi %+v", handled, want)
	}
	if (stats != Stats{Succeeded: 1}) {
		t.Fatalf("stats = %+v", stats)
	}
	if len(mustPending(t, parent)) != 0 || len(mustDLQ(t, parent)) != 0 {
		t.Fatal("pending list và DLQ phải rỗng")
	}
}

// TestHelperProcess không phải test thật: nó chỉ chạy khi test binary được chạy lại với LAB03_HELPER=1.
// Khi đó nó đóng vai một consumer: đọc message, ghi marker "đã bắt đầu xử lý" vào Redis rồi treo ở handler
// cho tới khi bị SIGKILL. Có thời hạn cứng 60 giây để không bao giờ thành process mồ côi.
func TestHelperProcess(t *testing.T) {
	if os.Getenv(helperEnv) != "1" {
		t.Skip("chỉ chạy như helper process của test chaos")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	opts := redisOptions(t)
	commands := redis.NewClient(opts)
	blocking := redis.NewClient(opts)
	defer func() { _ = commands.Close() }()
	defer func() { _ = blocking.Close() }()
	q := NewQueue(Config{
		Commands: commands, Blocking: blocking,
		Stream: os.Getenv("STREAM"), Group: os.Getenv("GROUP"), DLQStream: os.Getenv("DLQ_STREAM"),
		Consumer: os.Getenv("CONSUMER"), MinIdle: minIdle, BlockTime: blockTime,
	})
	marker := os.Getenv("MARKER_KEY")
	_, err := q.ConsumeWithDLQ(ctx, Options{MaxAttempts: 3}, func(ctx context.Context, d Delivery) error {
		if err := commands.Set(ctx, marker, d.ID, 0).Err(); err != nil {
			return err
		}
		<-ctx.Done() // treo cho tới khi bị kill
		return ctx.Err()
	})
	if err != nil && !errors.Is(err, context.DeadlineExceeded) {
		fmt.Fprintln(os.Stderr, "helper:", err)
		t.Fatal(err)
	}
}
