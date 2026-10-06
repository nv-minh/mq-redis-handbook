package lab

import (
	"context"
	"os"
	"slices"
	"strings"
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
		t.Fatalf("REDIS_URL không hợp lệ: %v", err)
	}
	return opts
}

// env giữ client lệnh dùng chung và danh sách cần dọn dẹp cho một test.
type env struct {
	t        *testing.T
	commands *redis.Client
	keys     []string
	// connectionNames ánh xạ một queue tới CLIENT SETNAME duy nhất của các connection blocking của nó.
	connectionNames map[*ReliableQueue]string
}

func newEnv(t *testing.T) *env {
	t.Helper()
	e := &env{t: t, commands: redis.NewClient(redisOptions(t)), connectionNames: map[*ReliableQueue]string{}}
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

// newQueue dựng một queue phía consumer: client lệnh dùng chung, client blocking riêng.
func (e *env) newQueue(queueKey string, blockTimeout time.Duration, consumerIDs ...string) (*ReliableQueue, *redis.Client) {
	e.t.Helper()
	// Tên connection duy nhất giúp test tìm đúng connection này trong CLIENT LIST.
	// Mọi connection của pool đều chạy CLIENT SETNAME khi được mở.
	name := testkit.UniqueName("lab03-blocking")
	opts := redisOptions(e.t)
	opts.OnConnect = func(ctx context.Context, conn *redis.Conn) error {
		return conn.ClientSetName(ctx, name).Err()
	}
	blocking := redis.NewClient(opts)
	e.t.Cleanup(func() { _ = blocking.Close() })
	queue := NewReliableQueue(Config{Commands: e.commands, Blocking: blocking, Queue: queueKey, BlockTimeout: blockTimeout})
	e.connectionNames[queue] = name
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
		t.Fatalf("Dequeue(%s) = %q, %v, %v; mong đợi một message", consumerID, msg, ok, err)
	}
	return msg
}

func assertList(t *testing.T, what string, got, want []string) {
	t.Helper()
	if !slices.Equal(got, want) {
		t.Fatalf("%s = %v, mong đợi %v", what, got, want)
	}
}

// isBlockedInBlmove cho biết client tên connectionName có đang bị chặn trong BLMOVE ngay lúc này
// hay không, theo CLIENT LIST (flags chứa "b", cmd là blmove).
func isBlockedInBlmove(t *testing.T, rdb *redis.Client, connectionName string) bool {
	t.Helper()
	list, err := rdb.ClientList(context.Background()).Result()
	if err != nil {
		t.Fatalf("CLIENT LIST: %v", err)
	}
	for _, line := range strings.Split(list, "\n") {
		fields := map[string]string{}
		for _, field := range strings.Fields(line) {
			if key, value, ok := strings.Cut(field, "="); ok {
				fields[key] = value
			}
		}
		if fields["name"] == connectionName && strings.Contains(fields["flags"], "b") && fields["cmd"] == "blmove" {
			return true
		}
	}
	return false
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
		t.Fatalf("Dequeue = %q, mong đợi job-1", got)
	}

	// BLMOVE chuyển message một cách atomic: biến khỏi queue, được cất trong processing list.
	assertList(t, "queue", e.list(queueKey), []string{})
	assertList(t, "processing", e.list(queue.ProcessingKey("worker-1")), []string{"job-1"})

	// Chỉ có ack mới xóa nó khỏi processing list.
	acked, err := queue.Ack(ctx, "worker-1", "job-1")
	if err != nil || !acked {
		t.Fatalf("Ack = %v, %v; mong đợi true", acked, err)
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

	// worker-a lấy message rồi "crash": nó không bao giờ ack, và connection của nó bị cắt.
	if got := mustDequeue(t, crashing, "worker-a"); got != "job-1" {
		t.Fatalf("Dequeue = %q, mong đợi job-1", got)
	}
	_ = crashingConn.Close()
	assertList(t, "processing of worker-a", e.list(survivor.ProcessingKey("worker-a")), []string{"job-1"})
	assertList(t, "queue", e.list(queueKey), []string{})

	// Một lượt recovery đưa message của consumer đã chết về lại queue.
	recovered, err := survivor.RecoverStale(ctx, "worker-a")
	if err != nil || recovered != 1 {
		t.Fatalf("RecoverStale = %d, %v; mong đợi 1", recovered, err)
	}
	assertList(t, "processing of worker-a", e.list(survivor.ProcessingKey("worker-a")), []string{})

	// Một consumer khác giờ nhận đúng message đó: at-least-once delivery.
	if got := mustDequeue(t, survivor, "worker-b"); got != "job-1" {
		t.Fatalf("Dequeue sau recovery = %q, mong đợi job-1", got)
	}
	acked, err := survivor.Ack(ctx, "worker-b", "job-1")
	if err != nil || !acked {
		t.Fatalf("Ack = %v, %v; mong đợi true", acked, err)
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
		t.Fatalf("Dequeue trên queue rỗng = %q, %v, %v; mong đợi không có message và không có lỗi", msg, ok, err)
	}
	// BLMOVE thực sự đã chặn khoảng bằng timeout (nó không thể return sớm hơn trên list rỗng),
	// và kết quả rỗng không để lại gì trong processing list.
	if waited < 900*time.Millisecond {
		t.Fatalf("Dequeue return sau %v, mong đợi khoảng block timeout 1s", waited)
	}
	assertList(t, "processing", e.list(queue.ProcessingKey("worker-1")), []string{})
}

func TestBlockedDequeueWakesUpWhenAMessageArrives(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t)
	queue, _ := e.newQueue(e.queueKey(), 10*time.Second, "worker-1")

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
	// Chờ tới khi server báo CHÍNH connection này đang bị chặn trong BLMOVE (flags=b trong CLIENT LIST).
	testkit.Eventually(t, 5*time.Second, func() (struct{}, bool) {
		return struct{}{}, isBlockedInBlmove(t, e.commands, e.connectionNames[queue])
	})

	if err := queue.Enqueue(ctx, "late-job"); err != nil {
		t.Fatal(err)
	}
	// Lệnh push đánh thức client đang bị chặn ngay lập tức, rất lâu trước timeout 10 giây.
	select {
	case r := <-done:
		if r.err != nil || !r.ok || r.msg != "late-job" {
			t.Fatalf("Dequeue = %q, %v, %v; mong đợi late-job", r.msg, r.ok, r.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Dequeue đang bị chặn không được đánh thức sau Enqueue")
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
			t.Fatalf("Dequeue = %q, mong đợi %q", got, want)
		}
	}
}

func TestRecoverStaleOnEmptyProcessingListReturnsZero(t *testing.T) {
	e := newEnv(t)
	queue, _ := e.newQueue(e.queueKey(), time.Second)
	n, err := queue.RecoverStale(context.Background(), "nobody")
	if err != nil || n != 0 {
		t.Fatalf("RecoverStale = %d, %v; mong đợi 0", n, err)
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
		t.Fatalf("RecoverStale = %d, %v; mong đợi 3", recovered, err)
	}
	for _, want := range []string{"a", "b", "c"} {
		if got := mustDequeue(t, survivor, "worker-b"); got != want {
			t.Fatalf("Dequeue sau recovery = %q, mong đợi %q", got, want)
		}
	}
}

func TestAckOfAMessageNotInTheProcessingListReturnsFalse(t *testing.T) {
	e := newEnv(t)
	queue, _ := e.newQueue(e.queueKey(), time.Second, "worker-1")
	acked, err := queue.Ack(context.Background(), "worker-1", "never-dequeued")
	if err != nil || acked {
		t.Fatalf("Ack = %v, %v; mong đợi false", acked, err)
	}
}

// Chỉ có ở Go: lệnh BLMove có kiểu của go-redis kéo dài read deadline của socket thêm đúng thời gian
// block, nên ReadTimeout của client ngắn hơn thời gian block cũng không làm nó hỏng.
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
		t.Fatalf("Dequeue = %q, %v, %v; mong đợi kết quả rỗng sạch sau 1s dù ReadTimeout là 300ms", msg, ok, err)
	}
}
