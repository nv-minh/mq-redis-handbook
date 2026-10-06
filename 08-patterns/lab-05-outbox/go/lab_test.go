package lab

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/redis/go-redis/v9"

	"github.com/nv-minh/mq-redis-handbook/internal/testkit"
)

// helperEnv bật chế độ helper process: test binary được chạy lại để đóng vai relay bị kill.
const helperEnv = "LAB05_HELPER"

func redisURL() string {
	if url := os.Getenv("REDIS_URL"); url != "" {
		return url
	}
	return "redis://127.0.0.1:6379"
}

// newPool tạo schema riêng (tên duy nhất) và một pool có search_path trỏ vào schema đó.
// Cleanup chạy ngược thứ tự đăng ký: pool đóng trước, rồi mới DROP SCHEMA, kể cả khi test fail.
func newPool(t *testing.T) (*pgxpool.Pool, string) {
	t.Helper()
	ctx := context.Background()
	schema := testkit.UniqueName("lab05")
	if err := CreateSchema(ctx, schema); err != nil {
		t.Fatalf("CreateSchema lỗi: %v", err)
	}
	t.Cleanup(func() {
		if err := DropSchema(context.Background(), schema); err != nil {
			t.Errorf("DropSchema lỗi: %v", err)
		}
	})
	pool, err := OpenPool(ctx, schema)
	if err != nil {
		t.Fatalf("OpenPool lỗi: %v", err)
	}
	t.Cleanup(pool.Close)
	if err := CreateTables(ctx, pool); err != nil {
		t.Fatalf("CreateTables lỗi: %v", err)
	}
	return pool, schema
}

func count(t *testing.T, pool *pgxpool.Pool, sql string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), sql).Scan(&n); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	return n
}

func outboxIDs(t *testing.T, pool *pgxpool.Pool) []int64 {
	t.Helper()
	rows, err := pool.Query(context.Background(), "select id from outbox order by id")
	if err != nil {
		t.Fatalf("đọc outbox lỗi: %v", err)
	}
	ids, err := pgx.CollectRows(rows, pgx.RowTo[int64])
	if err != nil {
		t.Fatalf("đọc outbox lỗi: %v", err)
	}
	return ids
}

func mustPlace(t *testing.T, pool *pgxpool.Pool, ids ...string) {
	t.Helper()
	for i, id := range ids {
		if err := PlaceOrder(context.Background(), pool, Order{ID: id, Amount: float64(10 + i)}); err != nil {
			t.Fatalf("PlaceOrder %s lỗi: %v", id, err)
		}
	}
}

// recorder là consumer dedupe theo event id: nó đếm số lần broker giao mỗi id và chỉ "áp dụng effect"
// (ghi vào applied) ở lần giao đầu tiên của id đó.
type recorder struct {
	mu         sync.Mutex
	deliveries map[string]int
	applied    []string
}

func (r *recorder) onDelivery(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.deliveries[id]++
	if r.deliveries[id] == 1 {
		r.applied = append(r.applied, id)
	}
}

func (r *recorder) deliveriesOf(id string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.deliveries[id]
}

func (r *recorder) appliedIDs() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.applied)
}

func (r *recorder) distinct() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.deliveries)
}

type broker struct {
	queue     string
	publish   PublishFn
	recording *recorder
}

// newBroker tạo queue quorum duy nhất, một confirm channel cho publisher và một consumer ack thủ công.
// Queue bị xóa trong Cleanup bằng connection riêng, nên vẫn dọn được khi connection của test đã đóng.
func newBroker(t *testing.T) *broker {
	t.Helper()
	queue := testkit.UniqueName("lab05-outbox")
	t.Cleanup(func() {
		conn, err := amqp.Dial(AMQPURL())
		if err != nil {
			t.Errorf("không mở được connection để xóa queue %s: %v", queue, err)
			return
		}
		defer func() { _ = conn.Close() }()
		ch, err := conn.Channel()
		if err != nil {
			t.Errorf("mở channel để xóa queue lỗi: %v", err)
			return
		}
		if _, err := ch.QueueDelete(queue, false, false, false); err != nil {
			t.Errorf("xóa queue %s lỗi: %v", queue, err)
		}
	})
	conn, err := amqp.Dial(AMQPURL())
	if err != nil {
		t.Fatalf("amqp.Dial lỗi: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	pubCh, err := conn.Channel()
	if err != nil {
		t.Fatalf("mở channel lỗi: %v", err)
	}
	if err := DeclareQueue(pubCh, queue); err != nil {
		t.Fatalf("DeclareQueue lỗi: %v", err)
	}
	publish, err := NewPublisher(pubCh, queue)
	if err != nil {
		t.Fatalf("NewPublisher lỗi: %v", err)
	}
	consCh, err := conn.Channel()
	if err != nil {
		t.Fatalf("mở channel consumer lỗi: %v", err)
	}
	rec := &recorder{deliveries: map[string]int{}}
	if err := StartConsumer(consCh, queue, testkit.UniqueName("lab05-consumer"), func(e ReceivedEvent) {
		rec.onDelivery(e.EventID)
	}); err != nil {
		t.Fatalf("StartConsumer lỗi: %v", err)
	}
	return &broker{queue: queue, publish: publish, recording: rec}
}

func TestOrderAndOutboxRowCommitAtomically(t *testing.T) {
	ctx := context.Background()
	pool, _ := newPool(t)
	mustPlace(t, pool, "o-1")

	// Hai dòng có cùng xmin (cùng transaction id) và cùng created_at (now() cố định trong một transaction).
	var sameTx, sameTs bool
	var aggregateID, typ string
	var payload map[string]any
	var publishedAt *time.Time
	err := pool.QueryRow(ctx, `
		select o.xmin::text = ob.xmin::text, o.created_at = ob.created_at,
		       ob.aggregate_id, ob.type, ob.payload, ob.published_at
		  from orders o join outbox ob on ob.aggregate_id = o.id`,
	).Scan(&sameTx, &sameTs, &aggregateID, &typ, &payload, &publishedAt)
	if err != nil {
		t.Fatalf("đọc order và outbox lỗi: %v", err)
	}
	if !sameTx || !sameTs || aggregateID != "o-1" || typ != "OrderPlaced" || publishedAt != nil {
		t.Fatalf("sameTx=%v sameTs=%v aggregate=%q type=%q publishedAt=%v", sameTx, sameTs, aggregateID, typ, publishedAt)
	}
	if payload["orderId"] != "o-1" || payload["amount"] != 10.0 {
		t.Fatalf("payload = %v", payload)
	}
	if n := count(t, pool, "select count(*) from orders"); n != 1 {
		t.Fatalf("orders = %d, mong đợi 1", n)
	}
	if n := count(t, pool, "select count(*) from outbox"); n != 1 {
		t.Fatalf("outbox = %d, mong đợi 1", n)
	}
}

func TestRollbackLeavesNoOutboxRow(t *testing.T) {
	ctx := context.Background()
	pool, _ := newPool(t)
	// Ép lỗi ở câu INSERT thứ hai: câu INSERT order đã chạy xong trong transaction, rồi outbox từ chối dòng này.
	if _, err := pool.Exec(ctx, "alter table outbox add constraint outbox_no_poison check (aggregate_id <> 'poison')"); err != nil {
		t.Fatalf("thêm constraint lỗi: %v", err)
	}

	err := PlaceOrder(ctx, pool, Order{ID: "poison", Amount: 1})
	if err == nil || !strings.Contains(err.Error(), "outbox_no_poison") {
		t.Fatalf("PlaceOrder err = %v, mong đợi lỗi constraint outbox_no_poison", err)
	}
	if n := count(t, pool, "select count(*) from orders"); n != 0 {
		t.Fatalf("orders = %d, mong đợi 0", n)
	}
	if n := count(t, pool, "select count(*) from outbox"); n != 0 {
		t.Fatalf("outbox = %d, mong đợi 0", n)
	}
	// Pool vẫn dùng được sau lỗi (connection đã ROLLBACK và được trả về pool, không kẹt trong transaction).
	mustPlace(t, pool, "ok")
	if n := count(t, pool, "select count(*) from orders"); n != 1 {
		t.Fatalf("orders = %d, mong đợi 1", n)
	}
}

func TestRelayPublishesEachRowAndMarksItPublished(t *testing.T) {
	ctx := context.Background()
	pool, _ := newPool(t)
	b := newBroker(t)
	mustPlace(t, pool, "o-1", "o-2", "o-3")
	var want []string
	for _, id := range outboxIDs(t, pool) {
		want = append(want, strconv.FormatInt(id, 10))
	}

	published, err := RunRelay(ctx, pool, b.publish, 10)
	if err != nil || published != 3 {
		t.Fatalf("RunRelay = %d, %v, mong đợi 3, nil", published, err)
	}

	// Mỗi event tới consumer đúng một lần, đúng thứ tự chèn (một relay duy nhất giữ thứ tự).
	testkit.Eventually(t, 10*time.Second, func() (struct{}, bool) {
		return struct{}{}, len(b.recording.appliedIDs()) == 3
	})
	if got := b.recording.appliedIDs(); !slices.Equal(got, want) {
		t.Fatalf("thứ tự nhận = %v, mong đợi %v", got, want)
	}
	for _, id := range want {
		if n := b.recording.deliveriesOf(id); n != 1 {
			t.Errorf("event %s được giao %d lần, mong đợi 1", id, n)
		}
	}
	if n := count(t, pool, "select count(*) from outbox where published_at is null"); n != 0 {
		t.Fatalf("còn %d dòng chưa đánh dấu", n)
	}

	// Lần chạy thứ hai không còn gì để publish.
	if again, err := RunRelay(ctx, pool, b.publish, 10); err != nil || again != 0 {
		t.Fatalf("RunRelay lần hai = %d, %v, mong đợi 0, nil", again, err)
	}
	if got := b.recording.distinct(); got != 3 {
		t.Fatalf("consumer thấy %d event khác nhau, mong đợi 3", got)
	}
}

func TestEmptyOutboxRelayReturnsZeroWithoutPublishing(t *testing.T) {
	pool, _ := newPool(t)
	calls := 0
	published, err := RunRelay(context.Background(), pool, func(context.Context, OutboxEvent) error {
		calls++
		return nil
	}, 5)
	if err != nil || published != 0 || calls != 0 {
		t.Fatalf("RunRelay = %d, %v, publish được gọi %d lần, mong đợi 0, nil, 0", published, err, calls)
	}
}

func TestBatchSizeLimitsTheRowsPublishedPerCall(t *testing.T) {
	ctx := context.Background()
	pool, _ := newPool(t)
	mustPlace(t, pool, "o-1", "o-2", "o-3", "o-4", "o-5")
	ids := outboxIDs(t, pool)
	var seen []int64
	publish := func(_ context.Context, e OutboxEvent) error {
		seen = append(seen, e.ID)
		return nil
	}

	// Backlog 5, batchSize 2: 2 + 2 + 1 + 0, mỗi lần theo thứ tự id.
	var batches []int
	for range 4 {
		n, err := RunRelay(ctx, pool, publish, 2)
		if err != nil {
			t.Fatalf("RunRelay lỗi: %v", err)
		}
		batches = append(batches, n)
	}
	if !slices.Equal(batches, []int{2, 2, 1, 0}) {
		t.Fatalf("số event mỗi lượt = %v, mong đợi [2 2 1 0]", batches)
	}
	if !slices.Equal(seen, ids) {
		t.Fatalf("thứ tự publish = %v, mong đợi %v", seen, ids)
	}
}

func TestPublishFailureRollsBackTheWholeBatchAndPropagatesTheError(t *testing.T) {
	ctx := context.Background()
	pool, _ := newPool(t)
	mustPlace(t, pool, "o-1", "o-2", "o-3")
	ids := outboxIDs(t, pool)
	var calls []int64
	errBroker := errors.New("broker từ chối event thứ hai")

	// Event thứ hai lỗi: cả batch nằm trong một transaction nên ROLLBACK, không dòng nào được đánh dấu.
	_, err := RunRelay(ctx, pool, func(_ context.Context, e OutboxEvent) error {
		calls = append(calls, e.ID)
		if len(calls) == 2 {
			return errBroker
		}
		return nil
	}, 10)
	if !errors.Is(err, errBroker) {
		t.Fatalf("RunRelay err = %v, mong đợi errBroker", err)
	}
	if !slices.Equal(calls, ids[:2]) {
		t.Fatalf("publish được gọi với %v, mong đợi %v", calls, ids[:2])
	}
	if n := count(t, pool, "select count(*) from outbox where published_at is null"); n != 3 {
		t.Fatalf("chưa đánh dấu %d dòng, mong đợi 3", n)
	}

	// Lần chạy sau publish lại event đầu (đã được publish ở lần lỗi): đó là at-least-once, không phải mất dữ liệu.
	calls = nil
	n, err := RunRelay(ctx, pool, func(_ context.Context, e OutboxEvent) error {
		calls = append(calls, e.ID)
		return nil
	}, 10)
	if err != nil || n != 3 || !slices.Equal(calls, ids) {
		t.Fatalf("RunRelay = %d, %v, calls = %v, mong đợi 3, nil, %v", n, err, calls, ids)
	}
	if left := count(t, pool, "select count(*) from outbox where published_at is null"); left != 0 {
		t.Fatalf("còn %d dòng chưa đánh dấu", left)
	}
}

func TestSeveralRelaysWithSkipLockedDoNotPreserveInsertionOrder(t *testing.T) {
	ctx := context.Background()
	pool, _ := newPool(t)
	mustPlace(t, pool, "o-1", "o-2", "o-3", "o-4")
	ids := outboxIDs(t, pool)
	var mu sync.Mutex
	var log []int64
	record := func(id int64) {
		mu.Lock()
		defer mu.Unlock()
		log = append(log, id)
	}

	// Relay A khóa dòng 1 và 2 rồi đứng chờ ở event đầu. Relay B chạy trọn vẹn trong lúc đó:
	// SKIP LOCKED bỏ qua dòng 1 và 2 nên B publish dòng 3 và 4 trước.
	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	resultA := make(chan error, 1)
	var publishedA int
	go func() {
		n, err := RunRelay(ctx, pool, func(_ context.Context, e OutboxEvent) error {
			once.Do(func() {
				close(started)
				<-release
			})
			record(e.ID)
			return nil
		}, 2)
		publishedA = n
		resultA <- err
	}()
	releaseOnce := sync.OnceFunc(func() { close(release) })
	defer releaseOnce()
	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("relay A không bắt đầu publish")
	}
	publishedB, err := RunRelay(ctx, pool, func(_ context.Context, e OutboxEvent) error {
		record(e.ID)
		return nil
	}, 2)
	if err != nil || publishedB != 2 {
		t.Fatalf("relay B = %d, %v, mong đợi 2, nil", publishedB, err)
	}
	releaseOnce()
	select {
	case err := <-resultA:
		if err != nil || publishedA != 2 {
			t.Fatalf("relay A = %d, %v, mong đợi 2, nil", publishedA, err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("relay A không kết thúc sau khi được thả")
	}

	want := []int64{ids[2], ids[3], ids[0], ids[1]}
	if !slices.Equal(log, want) {
		t.Fatalf("thứ tự publish = %v, mong đợi %v", log, want)
	}
	if n := count(t, pool, "select count(*) from outbox where published_at is null"); n != 0 {
		t.Fatalf("còn %d dòng chưa đánh dấu", n)
	}
}

func TestRelayRejectsANonPositiveBatchSize(t *testing.T) {
	pool, _ := newPool(t)
	for _, batchSize := range []int{0, -1} {
		_, err := RunRelay(context.Background(), pool, func(context.Context, OutboxEvent) error { return nil }, batchSize)
		if !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("batchSize = %d: err = %v, mong đợi ErrInvalidArgument", batchSize, err)
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

func TestChaosRelayCrashAfterPublishBeforeMarkRepublishesAndConsumerDedupes(t *testing.T) {
	ctx := context.Background()
	pool, schema := newPool(t)
	b := newBroker(t)
	marker := b.queue + ":published"
	rdb := redis.NewClient(mustRedisOptions(t))
	t.Cleanup(func() {
		rdb.Del(context.Background(), marker)
		_ = rdb.Close()
	})
	mustPlace(t, pool, "o-1", "o-2")
	ids := outboxIDs(t, pool)
	first, second := strconv.FormatInt(ids[0], 10), strconv.FormatInt(ids[1], 10)

	// Helper process: chạy lại chính test binary này, chỉ chạy TestHelperProcess. Đó là một relay thật:
	// publish event đầu (có confirm), ghi marker vào Redis rồi treo TRƯỚC khi đánh dấu published_at.
	cmd := exec.Command(os.Args[0], "-test.run=^TestHelperProcess$")
	cmd.Env = append(os.Environ(),
		helperEnv+"=1", "SCHEMA="+schema, "QUEUE="+b.queue, "MARKER_KEY="+marker,
	)
	stderr := &safeBuffer{}
	cmd.Stdout, cmd.Stderr = stderr, stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("không khởi động được helper process: %v", err)
	}
	// Cleanup chạy ngược thứ tự đăng ký: khối này đăng ký TRƯỚC reap nên chạy SAU nó, khi buffer stderr đã đầy đủ.
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("stderr của child:\n%s", stderr.String())
		}
	})
	// Luôn kill và reap child, kể cả khi test fail: không để process mồ côi. Cleanup này đăng ký sau pool nên chạy trước.
	waited := make(chan error, 1)
	exited := make(chan struct{}) // đóng khi child đã thoát, để test fail nhanh nếu child chết lúc khởi động
	go func() {
		waited <- cmd.Wait()
		close(exited)
	}()
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
		t.Fatalf(format, args...) // stderr của child được in bởi Cleanup
	}

	// Chờ child báo "đã publish và confirm xong event đầu" (marker ngắn hơn timeout của test).
	markedID := testkit.Eventually(t, 30*time.Second, func() (string, bool) {
		v, err := rdb.Get(ctx, marker).Result()
		if err == nil {
			return v, true
		}
		select {
		case <-exited:
			fail("child đã thoát trước khi báo marker (xem stderr của child bên dưới)")
		default:
		}
		return "", false
	})
	if markedID != first {
		fail("child báo đã publish event %q, mong đợi %q", markedID, first)
	}
	testkit.Eventually(t, 10*time.Second, func() (struct{}, bool) {
		return struct{}{}, b.recording.deliveriesOf(first) >= 1
	})

	// Child đang giữ transaction mở: chưa dòng nào được đánh dấu, và cả hai dòng đang bị khóa
	// (SKIP LOCKED của parent không thấy dòng nào).
	if n := count(t, pool, "select count(*) from outbox where published_at is null"); n != 2 {
		fail("chưa đánh dấu %d dòng, mong đợi 2", n)
	}
	if n := count(t, pool, "select count(*) from (select id from outbox where published_at is null order by id for update skip locked) s"); n != 0 {
		fail("parent khóa được %d dòng, mong đợi 0 vì child đang giữ lock", n)
	}

	// SIGKILL: relay chết sau publish, trước UPDATE và COMMIT.
	var exitErr *exec.ExitError
	if err := reap(); !errors.As(err, &exitErr) {
		fail("Wait trả %v, mong đợi *exec.ExitError", err)
	}
	if ws, ok := exitErr.Sys().(syscall.WaitStatus); !ok || !ws.Signaled() || ws.Signal() != syscall.SIGKILL {
		fail("child không chết vì SIGKILL: %v", exitErr)
	}

	// Relay mới trong parent. Connection của child chết nên Postgres rollback transaction và nhả lock,
	// nhưng việc đó không tức thì: trong lúc lock còn giữ, SKIP LOCKED trả 0 dòng. Do đó chờ bằng Eventually.
	republished := testkit.Eventually(t, 20*time.Second, func() (int, bool) {
		n, err := RunRelay(ctx, pool, b.publish, 10)
		return n, err == nil && n > 0
	})
	if republished != 2 {
		t.Fatalf("relay mới publish %d event, mong đợi 2", republished)
	}
	testkit.Eventually(t, 10*time.Second, func() (struct{}, bool) {
		return struct{}{}, b.recording.deliveriesOf(first) >= 2 && b.recording.deliveriesOf(second) >= 1
	})

	// Event đầu được giao ít nhất hai lần (một từ child, một từ relay mới), nhưng effect chỉ áp dụng một lần.
	if n := b.recording.deliveriesOf(first); n < 2 {
		t.Fatalf("event %s được giao %d lần, mong đợi ít nhất 2", first, n)
	}
	if got := b.recording.appliedIDs(); !slices.Equal(got, []string{first, second}) {
		t.Fatalf("effect đã áp dụng cho %v, mong đợi [%s %s] mỗi id đúng một lần", got, first, second)
	}
	// Không dòng nào bị mất: tất cả đã được đánh dấu.
	if n := count(t, pool, "select count(*) from outbox where published_at is null"); n != 0 {
		t.Fatalf("còn %d dòng chưa đánh dấu", n)
	}
	if n := count(t, pool, "select count(*) from outbox"); n != 2 {
		t.Fatalf("outbox có %d dòng, mong đợi 2", n)
	}
}

func mustRedisOptions(t *testing.T) *redis.Options {
	t.Helper()
	opts, err := redis.ParseURL(redisURL())
	if err != nil {
		t.Fatalf("REDIS_URL không hợp lệ: %v", err)
	}
	return opts
}

// TestHelperProcess không phải test thật: nó chỉ chạy khi test binary được chạy lại với LAB05_HELPER=1.
// Khi đó nó là một relay thật: publish event đầu lên RabbitMQ (chờ confirm), ghi marker vào Redis rồi treo
// trong callback publish cho tới khi bị SIGKILL, nên relay không bao giờ đánh dấu published_at.
// Có thời hạn cứng 60 giây để không bao giờ thành process mồ côi.
func TestHelperProcess(t *testing.T) {
	if os.Getenv(helperEnv) != "1" {
		t.Skip("chỉ chạy như helper process của test chaos")
	}
	ctx := context.Background()
	pool, err := OpenPool(ctx, os.Getenv("SCHEMA"))
	if err != nil {
		t.Fatalf("OpenPool lỗi: %v", err)
	}
	defer pool.Close()
	conn, err := amqp.Dial(AMQPURL())
	if err != nil {
		t.Fatalf("amqp.Dial lỗi: %v", err)
	}
	defer func() { _ = conn.Close() }()
	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("mở channel lỗi: %v", err)
	}
	publish, err := NewPublisher(ch, os.Getenv("QUEUE"))
	if err != nil {
		t.Fatalf("NewPublisher lỗi: %v", err)
	}
	rdb := redis.NewClient(mustRedisOptions(t))
	defer func() { _ = rdb.Close() }()
	marker := os.Getenv("MARKER_KEY")
	hang := make(chan struct{})
	go func() {
		_, err := RunRelay(ctx, pool, func(ctx context.Context, e OutboxEvent) error {
			if err := publish(ctx, e); err != nil {
				return err
			}
			if err := rdb.Set(ctx, marker, strconv.FormatInt(e.ID, 10), 0).Err(); err != nil {
				return err
			}
			<-hang // treo cho tới khi bị kill
			return nil
		}, 10)
		fmt.Fprintln(os.Stderr, "helper: RunRelay kết thúc:", err)
		os.Exit(1)
	}()
	// Không bao giờ thả hang: hết 60 giây thì thoát, process cha lẽ ra đã kill từ lâu.
	<-time.After(60 * time.Second)
}
