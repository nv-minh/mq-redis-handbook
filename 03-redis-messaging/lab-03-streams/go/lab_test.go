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
		t.Fatalf("REDIS_URL không hợp lệ: %v", err)
	}
	return opts
}

// env giữ client lệnh dùng chung và các key stream cần xóa khi test kết thúc.
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
			// DEL xóa stream cùng với các consumer group và pending list của chúng.
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

// newQueue dựng một đối tượng phía consumer: client lệnh dùng chung, client blocking riêng.
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
		t.Fatalf("Publish lỗi: %v", err)
	}
	return id
}

func mustConsume(t *testing.T, q *StreamQueue, group, consumer string, count int) []Message {
	t.Helper()
	messages, err := q.Consume(context.Background(), group, consumer, count)
	if err != nil {
		t.Fatalf("Consume lỗi: %v", err)
	}
	return messages
}

func mustPendingCount(t *testing.T, q *StreamQueue, group string) int64 {
	t.Helper()
	n, err := q.PendingCount(context.Background(), group)
	if err != nil {
		t.Fatalf("PendingCount lỗi: %v", err)
	}
	return n
}

func mustPending(t *testing.T, q *StreamQueue, group string) []PendingEntry {
	t.Helper()
	entries, err := q.PendingEntries(context.Background(), group)
	if err != nil {
		t.Fatalf("PendingEntries lỗi: %v", err)
	}
	return entries
}

func mustCreateGroup(t *testing.T, q *StreamQueue, group string) {
	t.Helper()
	if err := q.CreateGroup(context.Background(), group); err != nil {
		t.Fatalf("CreateGroup lỗi: %v", err)
	}
}

func ids(messages []Message) []string {
	out := make([]string, len(messages))
	for i, m := range messages {
		out[i] = m.ID
	}
	return out
}

// compareIDs sắp id của stream theo phần mili giây, rồi theo phần sequence.
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

	// Hai consumer của cùng một group đọc đồng thời cho tới khi mọi thứ đã được giao.
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
				t.Errorf("Consume(%s) lỗi: %v", name, err)
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

	// Mỗi id được giao đúng một lần, và gộp lại các consumer nhận đủ mọi message.
	all := append(append([]string{}, received["c1"]...), received["c2"]...)
	if len(all) != total {
		t.Fatalf("đã giao %d message, mong đợi %d", len(all), total)
	}
	sort.Slice(all, func(i, j int) bool { return compareIDs(all[i], all[j]) < 0 })
	if !slices.Equal(all, published) {
		t.Fatalf("các id đã giao = %v, mong đợi mỗi id trong %v đúng một lần", all, published)
	}

	// Server cũng đồng ý: pending list giữ mỗi id một lần, thuộc về consumer đã nhận nó.
	pending := mustPending(t, c1, group)
	if len(pending) != total {
		t.Fatalf("số entry pending = %d, mong đợi %d", len(pending), total)
	}
	for _, entry := range pending {
		if !slices.Contains(received[entry.Consumer], entry.ID) {
			t.Fatalf("entry %s thuộc về %s nhưng consumer đó không nhận nó", entry.ID, entry.Consumer)
		}
		if entry.DeliveryCount != 1 {
			t.Fatalf("delivery count của entry %s = %d, mong đợi 1", entry.ID, entry.DeliveryCount)
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
		t.Fatalf("Consume trả về %d message, mong đợi 3", len(messages))
	}
	// Đã giao nhưng chưa ack: cả ba nằm trong pending list (PEL).
	if n := mustPendingCount(t, q, group); n != 3 {
		t.Fatalf("pending trước khi ack = %d, mong đợi 3", n)
	}

	acked, err := q.Ack(ctx, group, messages[0].ID)
	if err != nil || acked != 1 {
		t.Fatalf("Ack = %d, %v; mong đợi 1", acked, err)
	}
	if n := mustPendingCount(t, q, group); n != 2 {
		t.Fatalf("pending sau một lần ack = %d, mong đợi 2", n)
	}
	// Ack lại cùng một id không xóa thêm gì.
	acked, err = q.Ack(ctx, group, messages[0].ID)
	if err != nil || acked != 0 {
		t.Fatalf("Ack lần hai = %d, %v; mong đợi 0", acked, err)
	}
	if n := mustPendingCount(t, q, group); n != 2 {
		t.Fatalf("pending sau khi ack lặp lại = %d, mong đợi 2", n)
	}

	for _, m := range messages[1:] {
		if _, err := q.Ack(ctx, group, m.ID); err != nil {
			t.Fatal(err)
		}
	}
	if n := mustPendingCount(t, q, group); n != 0 {
		t.Fatalf("pending sau khi ack hết = %d, mong đợi 0", n)
	}
}

// waitUntilIdle poll (không bao giờ sleep) tới khi entry pending đầu tiên đã idle ít nhất idleAtLeast.
// Hàm trả về idle time mà nó quan sát được.
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

	// Consumer A đọc entry rồi "crash": nó không bao giờ ack và client của nó bị đóng.
	delivered := mustConsume(t, dead, group, "consumer-a", 1)
	if !slices.Equal(ids(delivered), []string{id}) {
		t.Fatalf("delivered = %v, mong đợi [%s]", ids(delivered), id)
	}
	_ = deadConn.Close()

	// Chờ (không sleep) tới khi entry đã idle ít nhất gấp đôi minIdle, để lần claim
	// bên dưới cách xa ranh giới "idle time lớn hơn min-idle-time".
	idleBeforeClaim := waitUntilIdle(t, rescuer, group, 2*minIdle)

	claimed, err := rescuer.ClaimStale(ctx, group, "consumer-b", minIdle)
	if err != nil {
		t.Fatalf("ClaimStale lỗi: %v", err)
	}
	if len(claimed.Messages) != 1 || claimed.Messages[0].ID != id || claimed.Messages[0].Fields["job"] != "send-email" {
		t.Fatalf("messages đã claim = %+v, mong đợi đúng một entry %s", claimed.Messages, id)
	}
	if len(claimed.DeletedIDs) != 0 {
		t.Fatalf("các id đã xóa = %v, mong đợi rỗng", claimed.DeletedIDs)
	}

	// Entry đã chuyển sang pending list của consumer B, và lần claim được tính là lần giao thứ hai.
	pending := mustPending(t, rescuer, group)
	if len(pending) != 1 || pending[0].ID != id || pending[0].Consumer != "consumer-b" || pending[0].DeliveryCount != 2 {
		t.Fatalf("pending sau khi claim = %+v, mong đợi %s thuộc về consumer-b với delivery count 2", pending, id)
	}
	if pending[0].Idle >= idleBeforeClaim { // lần claim đã đặt lại idle time
		t.Fatalf("idle sau khi claim = %v, mong đợi nhỏ hơn %v trước đó", pending[0].Idle, idleBeforeClaim)
	}

	// B hoàn thành công việc.
	if n, err := rescuer.Ack(ctx, group, id); err != nil || n != 1 {
		t.Fatalf("Ack = %d, %v; mong đợi 1", n, err)
	}
	if n := mustPendingCount(t, rescuer, group); n != 0 {
		t.Fatalf("pending sau khi ack = %d, mong đợi 0", n)
	}
}

func TestClaimStaleWithNothingPendingReturnsEmptyResult(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t)
	q, _ := e.newQueue(e.streamKey())
	group := testkit.UniqueName("workers")
	mustCreateGroup(t, q, group)

	// Group đã tồn tại nhưng chưa từng giao gì.
	claimed, err := q.ClaimStale(ctx, group, "consumer-b", 100*time.Millisecond)
	if err != nil || len(claimed.Messages) != 0 || len(claimed.DeletedIDs) != 0 {
		t.Fatalf("ClaimStale trên PEL rỗng = %+v, %v; mong đợi rỗng và không có lỗi", claimed, err)
	}

	// Đã giao và đã ack: pending list lại rỗng.
	id := mustPublish(t, q, map[string]string{"n": "1"})
	mustConsume(t, q, group, "consumer-a", 1)
	if _, err := q.Ack(ctx, group, id); err != nil {
		t.Fatal(err)
	}
	claimed, err = q.ClaimStale(ctx, group, "consumer-b", 100*time.Millisecond)
	if err != nil || len(claimed.Messages) != 0 || len(claimed.DeletedIDs) != 0 {
		t.Fatalf("ClaimStale sau khi ack = %+v, %v; mong đợi rỗng và không có lỗi", claimed, err)
	}
}

func TestXautoclaimSkipsEntriesThatAreNotIdleLongEnough(t *testing.T) {
	e := newEnv(t)
	q, _ := e.newQueue(e.streamKey())
	group := testkit.UniqueName("workers")
	mustCreateGroup(t, q, group)
	id := mustPublish(t, q, map[string]string{"n": "1"})
	mustConsume(t, q, group, "consumer-a", 1)

	// Min idle một phút áp lên một entry vừa được giao cách đây vài mili giây.
	claimed, err := q.ClaimStale(context.Background(), group, "consumer-b", time.Minute)
	if err != nil || len(claimed.Messages) != 0 {
		t.Fatalf("ClaimStale = %+v, %v; mong đợi không claim được gì", claimed, err)
	}
	pending := mustPending(t, q, group)
	if len(pending) != 1 || pending[0].ID != id || pending[0].Consumer != "consumer-a" || pending[0].DeliveryCount != 1 {
		t.Fatalf("pending = %+v, mong đợi %s vẫn thuộc về consumer-a với delivery count 1", pending, id)
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
	// Entry rời khỏi stream (XDEL, hoặc trimming) trong khi vẫn còn trong pending list.
	if n, err := e.commands.XDel(ctx, stream, id).Result(); err != nil || n != 1 {
		t.Fatalf("XDEL = %d, %v; mong đợi 1", n, err)
	}
	_ = waitUntilIdle(t, q, group, 2*minIdle)

	// Redis 7.0+: XAUTOCLAIM không claim nó, loại nó khỏi PEL và trả về id của nó ở
	// phần tử thứ ba của reply. XAutoClaim có kiểu của go-redis bỏ phần tử đó, vì vậy
	// lab parse reply thô.
	claimed, err := q.ClaimStale(ctx, group, "consumer-b", minIdle)
	if err != nil {
		t.Fatalf("ClaimStale lỗi: %v", err)
	}
	if len(claimed.Messages) != 0 || !slices.Equal(claimed.DeletedIDs, []string{id}) {
		t.Fatalf("ClaimStale = %+v, mong đợi không có message và các id đã xóa là [%s]", claimed, id)
	}
	if n := mustPendingCount(t, q, group); n != 0 {
		t.Fatalf("pending = %d, mong đợi 0", n)
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
		t.Fatalf("Consume = %+v, mong đợi rỗng", messages)
	}
	if waited := time.Since(started); waited < blockTime-50*time.Millisecond {
		t.Fatalf("Consume return sau %v, mong đợi khoảng block time %v", waited, blockTime)
	}
}

func TestCreateGroupIsIdempotent(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t)
	q, _ := e.newQueue(e.streamKey())
	group := testkit.UniqueName("workers")
	mustCreateGroup(t, q, group)
	// XGROUP CREATE lần thứ hai bị server báo lỗi BUSYGROUP, và lab chỉ nuốt đúng lỗi đó.
	if err := q.CreateGroup(ctx, group); err != nil {
		t.Fatalf("CreateGroup lần hai = %v, mong đợi nil", err)
	}
	// Các lỗi khác không bị nuốt: tạo group trên key sai kiểu là lỗi WRONGTYPE.
	notAStream := e.streamKey()
	if err := e.commands.Set(ctx, notAStream, "x", 0).Err(); err != nil {
		t.Fatal(err)
	}
	wrongType, _ := e.newQueue(notAStream)
	err := wrongType.CreateGroup(ctx, group)
	if err == nil || !strings.Contains(err.Error(), "WRONGTYPE") {
		t.Fatalf("CreateGroup trên key kiểu string = %v, mong đợi lỗi WRONGTYPE", err)
	}
}
