package lab

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
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
	// helperEnv bật chế độ helper process: test binary được chạy lại để đóng vai worker bị kill.
	helperEnv = "LAB04_HELPER"
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

type names struct{ stream, group, marker string }

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

// names đăng ký mọi key để xóa trước khi dùng, nên test fail giữa chừng vẫn dọn sạch.
func (e *env) names() names {
	stream := testkit.UniqueName("lab04-stream")
	n := names{stream: stream, group: testkit.UniqueName("lab04-group"), marker: stream + ":started"}
	e.t.Cleanup(func() {
		// DEL xóa stream cùng với các consumer group và pending list của chúng.
		e.commands.Del(context.Background(), n.stream, n.marker)
	})
	return n
}

func (e *env) group(n names, minIdle time.Duration) *WorkerGroup {
	return NewWorkerGroup(Config{
		Commands: e.commands, Options: redisOptions(e.t),
		Stream: n.stream, Group: n.group, MinIdle: minIdle, BlockTime: blockTime,
	})
}

// start khởi động worker và đăng ký Stop vào Cleanup (chạy cả khi test fail).
func (e *env) start(g *WorkerGroup, workers int, h Handler) *Handle {
	e.t.Helper()
	handle, err := g.StartWorkers(workers, h)
	if err != nil {
		e.t.Fatalf("StartWorkers lỗi: %v", err)
	}
	e.t.Cleanup(func() {
		if err := handle.Stop(); err != nil {
			e.t.Errorf("Stop trong cleanup lỗi: %v", err)
		}
	})
	return handle
}

func mustEnsureGroup(t *testing.T, g *WorkerGroup) {
	t.Helper()
	if err := g.EnsureGroup(context.Background()); err != nil {
		t.Fatalf("EnsureGroup lỗi: %v", err)
	}
}

func mustPublish(t *testing.T, g *WorkerGroup, fields map[string]string) string {
	t.Helper()
	id, err := g.Publish(context.Background(), fields)
	if err != nil {
		t.Fatalf("Publish lỗi: %v", err)
	}
	return id
}

func pendingCount(g *WorkerGroup) (int, bool) {
	entries, err := g.PendingEntries(context.Background())
	return len(entries), err == nil
}

// recorder gom các lần giao mà handler thấy, an toàn khi nhiều worker ghi đồng thời.
type recorder struct {
	mu         sync.Mutex
	deliveries []Delivery
}

func (r *recorder) add(d Delivery) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.deliveries = append(r.deliveries, d)
}

func (r *recorder) snapshot() []Delivery {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.deliveries)
}

func TestFourWorkersProcessEachMessageExactlyOnceWithoutFailures(t *testing.T) {
	e := newEnv(t)
	n := e.names()
	g := e.group(n, 10*time.Second) // minIdle lớn: không có lần claim nào xảy ra khi không ai lỗi
	mustEnsureGroup(t, g)
	const total = 40
	published := make([]string, 0, total)
	for i := 0; i < total; i++ {
		published = append(published, mustPublish(t, g, map[string]string{"n": fmt.Sprint(i)}))
	}

	rec := &recorder{}
	handle := e.start(g, 4, func(_ context.Context, d Delivery) error {
		rec.add(d)
		return nil
	})
	if got := len(slices.Compact(slices.Sorted(slices.Values(handle.ConsumerNames())))); got != 4 {
		t.Fatalf("có %d tên consumer khác nhau, mong đợi 4", got)
	}

	testkit.Eventually(t, 20*time.Second, func() (struct{}, bool) {
		pending, ok := pendingCount(g)
		return struct{}{}, ok && len(rec.snapshot()) >= total && pending == 0
	})
	if err := handle.Stop(); err != nil {
		t.Fatalf("Stop lỗi: %v", err)
	}

	// Mỗi id được xử lý đúng một lần, không có id nào thiếu hay thừa. Không assert cách chia việc giữa các worker.
	counts := map[string]int{}
	for _, d := range rec.snapshot() {
		counts[d.ID]++
		if !slices.Contains(handle.ConsumerNames(), d.Consumer) {
			t.Errorf("consumer %q không thuộc các worker đã khởi động", d.Consumer)
		}
	}
	if len(counts) != total {
		t.Fatalf("xử lý %d id khác nhau, mong đợi %d", len(counts), total)
	}
	for _, id := range published {
		if counts[id] != 1 {
			t.Errorf("id %s được xử lý %d lần, mong đợi 1", id, counts[id])
		}
	}
	if pending, _ := pendingCount(g); pending != 0 {
		t.Fatalf("PEL còn %d entry, mong đợi 0", pending)
	}
}

func TestFailedHandlerLeavesMessagePendingUntilReclaimed(t *testing.T) {
	e := newEnv(t)
	g := e.group(e.names(), 300*time.Millisecond)
	mustEnsureGroup(t, g)
	id := mustPublish(t, g, map[string]string{"job": "flaky"})

	rec := &recorder{}
	handle := e.start(g, 2, func(_ context.Context, d Delivery) error {
		rec.add(d)
		if d.DeliveryCount == 1 {
			return errors.New("lỗi tạm thời ở lần giao đầu")
		}
		return nil
	})

	// Lần đầu lỗi nên không ack, message nằm lại PEL, một worker claim lại sau minIdle và lần hai thành công.
	testkit.Eventually(t, 20*time.Second, func() (struct{}, bool) {
		pending, ok := pendingCount(g)
		return struct{}{}, ok && len(rec.snapshot()) >= 2 && pending == 0
	})
	if err := handle.Stop(); err != nil {
		t.Fatalf("Stop lỗi: %v", err)
	}
	var attempts []int64
	for _, d := range rec.snapshot() {
		if d.ID != id {
			t.Fatalf("handler thấy id %s, mong đợi %s", d.ID, id)
		}
		attempts = append(attempts, d.DeliveryCount)
	}
	if !slices.Equal(attempts, []int64{1, 2}) {
		t.Fatalf("attempts = %v, mong đợi [1 2]", attempts)
	}
}

func TestStopWaitsForInFlightHandlerAndAcksIt(t *testing.T) {
	e := newEnv(t)
	g := e.group(e.names(), 10*time.Second)
	mustEnsureGroup(t, g)
	mustPublish(t, g, map[string]string{"job": "slow"})

	started := make(chan struct{})
	release := make(chan struct{})
	handle := e.start(g, 1, func(context.Context, Delivery) error {
		close(started)
		<-release
		return nil
	})
	select {
	case <-started:
	case <-time.After(10 * time.Second):
		close(release)
		t.Fatal("handler không bắt đầu")
	}

	stopped := make(chan error, 1)
	go func() { stopped <- handle.Stop() }()
	// Handler còn đang chạy nên Stop chưa được xong: sau một round trip tới Redis nó vẫn chưa trả về.
	if pending, ok := pendingCount(g); !ok || pending != 1 {
		close(release)
		t.Fatalf("pending = %d, ok = %v, mong đợi 1", pending, ok)
	}
	select {
	case err := <-stopped:
		close(release)
		t.Fatalf("Stop trả về (%v) khi handler còn đang chạy", err)
	default:
	}

	close(release)
	select {
	case err := <-stopped:
		if err != nil {
			t.Fatalf("Stop lỗi: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Stop không trả về sau khi handler xong")
	}
	// Sau Stop: handler đã xong và message đã được ack.
	if pending, _ := pendingCount(g); pending != 0 {
		t.Fatalf("PEL còn %d entry, mong đợi 0", pending)
	}
}

func TestStartWorkersRejectsANonPositiveWorkerCount(t *testing.T) {
	e := newEnv(t)
	g := e.group(e.names(), time.Second)
	for _, n := range []int{0, -1} {
		if _, err := g.StartWorkers(n, func(context.Context, Delivery) error { return nil }); !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("n = %d: err = %v, mong đợi ErrInvalidArgument", n, err)
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

func TestChaosWorkerKilledMidProcessingMessageIsProcessedByAnother(t *testing.T) {
	ctx := context.Background()
	const minIdle = 300 * time.Millisecond
	e := newEnv(t)
	n := e.names()
	g := e.group(n, minIdle)
	mustEnsureGroup(t, g)
	id := mustPublish(t, g, map[string]string{"job": "send-invoice"})

	// Helper process: chạy lại chính test binary này, chỉ chạy TestHelperProcess.
	cmd := exec.Command(os.Args[0], "-test.run=^TestHelperProcess$")
	cmd.Env = append(os.Environ(),
		helperEnv+"=1", "STREAM="+n.stream, "GROUP="+n.group, "MARKER_KEY="+n.marker,
		"MIN_IDLE_MS="+fmt.Sprint(minIdle.Milliseconds()),
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
	pending, err := g.PendingEntries(ctx)
	if err != nil || len(pending) != 1 || pending[0].ID != id || pending[0].DeliveryCount != 1 {
		fail("pending = %+v, err = %v, mong đợi một entry với delivery count 1", pending, err)
	}
	victim := pending[0].Consumer

	// SIGKILL: process chết ngay giữa handler, không kịp XACK hay dọn gì.
	var exitErr *exec.ExitError
	if err := reap(); !errors.As(err, &exitErr) {
		fail("Wait trả %v, mong đợi *exec.ExitError", err)
	}
	if ws, ok := exitErr.Sys().(syscall.WaitStatus); !ok || !ws.Signaled() || ws.Signal() != syscall.SIGKILL {
		fail("child không chết vì SIGKILL: %v", exitErr)
	}

	// Chờ tới khi idle gấp đôi minIdle, để lần claim cách xa ranh giới min-idle-time.
	testkit.Eventually(t, 10*time.Second, func() (struct{}, bool) {
		entries, err := g.PendingEntries(ctx)
		return struct{}{}, err == nil && len(entries) == 1 && entries[0].Idle >= 2*minIdle
	})

	// Hai worker trong process này cạnh tranh nhau: một trong hai claim message của worker đã chết.
	rec := &recorder{}
	handle := e.start(g, 2, func(_ context.Context, d Delivery) error {
		rec.add(d)
		return nil
	})
	testkit.Eventually(t, 20*time.Second, func() (struct{}, bool) {
		pending, ok := pendingCount(g)
		return struct{}{}, ok && len(rec.snapshot()) >= 1 && pending == 0
	})
	if err := handle.Stop(); err != nil {
		t.Fatalf("Stop lỗi: %v", err)
	}

	// At-least-once: message được xử lý ít nhất một lần và đã ack (PEL cạn). Không assert "đúng một lần".
	var got *Delivery
	for _, d := range rec.snapshot() {
		if d.ID == id {
			got = &d
			break
		}
	}
	if got == nil {
		t.Fatalf("message %s chưa được xử lý bởi consumer nào của process này", id)
	}
	if got.DeliveryCount < 2 || got.Consumer == victim || got.Fields["job"] != "send-invoice" {
		t.Fatalf("delivery = %+v, mong đợi delivery count >= 2, consumer khác %s, field job", *got, victim)
	}
	if pending, _ := pendingCount(g); pending != 0 {
		t.Fatalf("PEL còn %d entry, mong đợi 0", pending)
	}
}

// TestHelperProcess không phải test thật: nó chỉ chạy khi test binary được chạy lại với LAB04_HELPER=1.
// Khi đó nó đóng vai một worker: đọc message, ghi marker "đã bắt đầu xử lý" vào Redis rồi treo ở handler
// cho tới khi bị SIGKILL. Có thời hạn cứng 60 giây để không bao giờ thành process mồ côi.
func TestHelperProcess(t *testing.T) {
	if os.Getenv(helperEnv) != "1" {
		t.Skip("chỉ chạy như helper process của test chaos")
	}
	helperMinIdle, _ := time.ParseDuration(os.Getenv("MIN_IDLE_MS") + "ms")
	commands := redis.NewClient(redisOptions(t))
	defer func() { _ = commands.Close() }()
	g := NewWorkerGroup(Config{
		Commands: commands, Options: redisOptions(t),
		Stream: os.Getenv("STREAM"), Group: os.Getenv("GROUP"), MinIdle: helperMinIdle, BlockTime: blockTime,
	})
	marker := os.Getenv("MARKER_KEY")
	hang := make(chan struct{})
	if _, err := g.StartWorkers(1, func(ctx context.Context, d Delivery) error {
		if err := commands.Set(ctx, marker, d.ID, 0).Err(); err != nil {
			return err
		}
		<-hang // treo cho tới khi bị kill
		return nil
	}); err != nil {
		fmt.Fprintln(os.Stderr, "helper:", err)
		t.Fatal(err)
	}
	// Không gọi Stop (handler không bao giờ xong): hết 60 giây thì thoát, process cha lẽ ra đã kill từ lâu.
	<-time.After(60 * time.Second)
}
