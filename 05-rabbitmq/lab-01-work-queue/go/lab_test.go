package lab

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nv-minh/mq-redis-handbook/internal/testkit"
	amqp "github.com/rabbitmq/amqp091-go"
)

// env giữ mọi thứ một test đã mở hoặc đã tạo, để t.Cleanup đóng và xóa hết theo đúng thứ tự.
type env struct {
	t      *testing.T
	conns  []*amqp.Connection
	dones  []<-chan struct{}
	gates  []func()
	queues []string
}

func newEnv(t *testing.T) *env {
	t.Helper()
	e := &env{t: t}
	// Một cleanup duy nhất để thứ tự rõ ràng: mở khóa các handler đang treo, đóng connection,
	// chờ goroutine của worker thoát (không leak), rồi xóa queue bằng một connection mới.
	t.Cleanup(func() {
		for _, release := range e.gates {
			release()
		}
		for _, conn := range e.conns {
			_ = conn.Close()
		}
		for _, done := range e.dones {
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				t.Errorf("goroutine của worker không thoát sau khi đóng connection")
			}
		}
		if len(e.queues) == 0 {
			return
		}
		conn, err := amqp.Dial(URL())
		if err != nil {
			t.Errorf("không dọn được queue %v: %v", e.queues, err)
			return
		}
		defer func() { _ = conn.Close() }()
		ch, err := conn.Channel()
		if err != nil {
			t.Errorf("mở channel để dọn queue: %v", err)
			return
		}
		for _, queue := range e.queues {
			if _, err := ch.QueueDelete(queue, false, false, false); err != nil {
				t.Errorf("xóa queue %s: %v", queue, err)
			}
		}
	})
	return e
}

func (e *env) dial() *amqp.Connection {
	e.t.Helper()
	conn, err := amqp.Dial(URL())
	if err != nil {
		e.t.Fatalf("không kết nối được RabbitMQ (%s): %v. Đã chạy make up chưa?", URL(), err)
	}
	e.conns = append(e.conns, conn)
	return conn
}

func (e *env) channel(conn *amqp.Connection) *amqp.Channel {
	e.t.Helper()
	ch, err := conn.Channel()
	if err != nil {
		e.t.Fatalf("mở channel: %v", err)
	}
	return ch
}

func (e *env) newQueue(ch *amqp.Channel) string {
	e.t.Helper()
	queue := testkit.UniqueName("lab01-work")
	e.queues = append(e.queues, queue)
	if err := DeclareWorkQueue(ch, queue); err != nil {
		e.t.Fatalf("DeclareWorkQueue: %v", err)
	}
	return queue
}

// worker chạy StartWorker và nhớ kênh done để cleanup chờ goroutine thoát.
func (e *env) worker(ch *amqp.Channel, cfg WorkerConfig) {
	e.t.Helper()
	done, err := StartWorker(ch, cfg)
	if err != nil {
		e.t.Fatalf("StartWorker: %v", err)
	}
	e.dones = append(e.dones, done)
}

// gate trả về một channel chặn handler cho tới khi release được gọi (an toàn khi gọi nhiều lần).
// Cleanup tự gọi release để handler treo không giữ goroutine.
func (e *env) gate() (<-chan struct{}, func()) {
	open := make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(open) }) }
	e.gates = append(e.gates, release)
	return open, release
}

// readyCount là số message đang ready (chưa giao cho consumer nào) trong queue.
func readyCount(t *testing.T, ch *amqp.Channel, queue string) int {
	t.Helper()
	q, err := ch.QueueDeclarePassive(queue, true, false, false, false, nil)
	if err != nil {
		t.Fatalf("QueueDeclarePassive %s: %v", queue, err)
	}
	return q.Messages
}

func publishOne(t *testing.T, ch *amqp.Channel, queue string, bodies ...string) {
	t.Helper()
	if err := PublishTasks(context.Background(), ch, queue, bodies); err != nil {
		t.Fatalf("PublishTasks: %v", err)
	}
}

func TestPrefetch1GivesSlowWorkerFewerMessages(t *testing.T) {
	const total = 10
	e := newEnv(t)
	conn := e.dial()
	publisher := e.channel(conn)
	queue := e.newQueue(publisher)

	slowReceived := make(chan struct{})
	var slowOnce sync.Once
	releaseSlow, release := e.gate()
	var slowCalls, fastCalls atomic.Int64

	// Worker chậm giữ message đầu tiên của nó cho tới khi test cho phép, nên nó không bao giờ ack trong lúc test quan sát.
	e.worker(e.channel(conn), WorkerConfig{Queue: queue, ConsumerTag: testkit.UniqueName("slow"), Prefetch: 1,
		Handler: func(amqp.Delivery) error {
			slowCalls.Add(1)
			slowOnce.Do(func() { close(slowReceived) })
			<-releaseSlow
			return nil
		}})
	// Worker nhanh ack ngay, nhưng chỉ bắt đầu sau khi worker chậm đã nhận message của nó.
	// Nhờ vậy kết quả không phụ thuộc việc broker giao message đầu tiên cho worker nào.
	e.worker(e.channel(conn), WorkerConfig{Queue: queue, ConsumerTag: testkit.UniqueName("fast"), Prefetch: 1,
		Handler: func(amqp.Delivery) error {
			<-slowReceived
			fastCalls.Add(1)
			return nil
		}})

	bodies := make([]string, total)
	for i := range bodies {
		bodies[i] = "task-" + string(rune('a'+i))
	}
	publishOne(t, publisher, queue, bodies...)

	// Prefetch 1: worker chậm chỉ giữ đúng 1 message chưa ack, nên 9 message còn lại đều về worker nhanh.
	testkit.Eventually(t, 10*time.Second, func() (struct{}, bool) { return struct{}{}, fastCalls.Load() == total-1 })
	if got := slowCalls.Load(); got != 1 {
		t.Fatalf("worker chậm nhận %d message, mong đợi đúng 1", got)
	}
	if got := fastCalls.Load(); got != total-1 {
		t.Fatalf("worker nhanh xử lý %d message, mong đợi %d", got, total-1)
	}
	if got := readyCount(t, publisher, queue); got != 0 {
		t.Fatalf("queue còn %d message ready, mong đợi 0", got)
	}
	release()
}

func TestUnackedMessageIsRedeliveredAfterWorkerDisconnects(t *testing.T) {
	e := newEnv(t)
	connA := e.dial()
	connB := e.dial()
	publisher := e.channel(connB)
	queue := e.newQueue(publisher)
	hold, _ := e.gate()

	// Worker A nhận message nhưng không bao giờ ack (handler treo), rồi connection của nó bị đóng: mô phỏng crash.
	aReceived := make(chan amqp.Delivery, 1)
	e.worker(e.channel(connA), WorkerConfig{Queue: queue, ConsumerTag: testkit.UniqueName("worker-a"), Prefetch: 1,
		Handler: func(d amqp.Delivery) error {
			aReceived <- d
			<-hold
			return nil
		}})
	publishOne(t, publisher, queue, "job-1")
	first := <-aReceived
	if string(first.Body) != "job-1" || first.Redelivered {
		t.Fatalf("worker A nhận %q redelivered=%v, mong đợi job-1 redelivered=false", first.Body, first.Redelivered)
	}

	if err := connA.Close(); err != nil {
		t.Fatalf("đóng connection của A: %v", err)
	}

	// Broker requeue message chưa ack khi connection đóng, worker B nhận lại đúng message đó với cờ redelivered.
	bReceived := make(chan amqp.Delivery, 1)
	e.worker(e.channel(connB), WorkerConfig{Queue: queue, ConsumerTag: testkit.UniqueName("worker-b"), Prefetch: 1,
		Handler: func(d amqp.Delivery) error {
			select {
			case bReceived <- d:
			default:
			}
			return nil
		}})
	select {
	case second := <-bReceived:
		if string(second.Body) != "job-1" || !second.Redelivered {
			t.Fatalf("worker B nhận %q redelivered=%v, mong đợi job-1 redelivered=true", second.Body, second.Redelivered)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("worker B không nhận lại message sau khi A ngắt kết nối")
	}
}

func TestGetOnEmptyQueueReturnsNothing(t *testing.T) {
	e := newEnv(t)
	ch := e.channel(e.dial())
	queue := e.newQueue(ch)

	// basic.get trên queue rỗng trả get-empty: ok=false, không có message và không có lỗi.
	msg, ok, err := ch.Get(queue, true)
	if err != nil || ok {
		t.Fatalf("Get trên queue rỗng = %v, %v, %v; mong đợi ok=false và không lỗi", msg, ok, err)
	}
}

// consumeScripted consume bằng manual ack và gọi act cho từng delivery. Goroutine thoát khi channel đóng.
func (e *env) consumeScripted(ch *amqp.Channel, queue string, act func(d amqp.Delivery)) {
	e.t.Helper()
	deliveries, err := ch.Consume(queue, testkit.UniqueName("scripted"), false, false, false, false, nil)
	if err != nil {
		e.t.Fatalf("Consume: %v", err)
	}
	done := make(chan struct{})
	e.dones = append(e.dones, done)
	go func() {
		defer close(done)
		for d := range deliveries {
			act(d)
		}
	}()
}

func TestRejectRequeueCountsTowardDeliveryCountButNackDoesNot(t *testing.T) {
	e := newEnv(t)
	conn := e.dial()

	// header trả về giá trị số của một header (int64 theo amqp091-go) hoặc false nếu header vắng mặt.
	header := func(d amqp.Delivery, key string) (int64, bool) {
		v, ok := d.Headers[key]
		if !ok {
			return 0, false
		}
		n, isInt := v.(int64)
		if !isInt {
			t.Errorf("header %s có kiểu %T, mong đợi int64", key, v)
		}
		return n, true
	}

	// Giao lần đầu chưa có header nào. Mỗi lần reject(requeue=true) rồi giao lại, x-delivery-count tăng 1.
	rejectCh := e.channel(conn)
	rejectQueue := e.newQueue(rejectCh)
	var mu sync.Mutex
	var rejected []amqp.Delivery
	e.consumeScripted(rejectCh, rejectQueue, func(d amqp.Delivery) {
		mu.Lock()
		rejected = append(rejected, d)
		n := len(rejected)
		mu.Unlock()
		if n < 3 {
			_ = d.Reject(true)
		} else {
			_ = d.Ack(false)
		}
	})
	publishOne(t, rejectCh, rejectQueue, "m")
	testkit.Eventually(t, 10*time.Second, func() (struct{}, bool) {
		mu.Lock()
		defer mu.Unlock()
		return struct{}{}, len(rejected) == 3
	})
	mu.Lock()
	if _, ok := header(rejected[0], "x-delivery-count"); ok {
		t.Errorf("lần giao đầu không được có x-delivery-count")
	}
	// Đo trên RabbitMQ 4.3.6: lần giao lại đầu tiên có x-delivery-count = 1 (không phải 0).
	if n, ok := header(rejected[1], "x-delivery-count"); !ok || n != 1 {
		t.Errorf("x-delivery-count lần giao lại đầu = %d (có=%v), mong đợi 1", n, ok)
	}
	if n, ok := header(rejected[1], "x-acquired-count"); !ok || n != 1 {
		t.Errorf("x-acquired-count lần giao lại đầu = %d (có=%v), mong đợi 1", n, ok)
	}
	if n, ok := header(rejected[2], "x-delivery-count"); !ok || n != 2 {
		t.Errorf("x-delivery-count lần giao lại thứ hai = %d (có=%v), mong đợi 2", n, ok)
	}
	mu.Unlock()

	// Từ 4.3, nack(requeue=true) chỉ tăng x-acquired-count, không tăng x-delivery-count.
	nackCh := e.channel(conn)
	nackQueue := e.newQueue(nackCh)
	var nacked []amqp.Delivery
	e.consumeScripted(nackCh, nackQueue, func(d amqp.Delivery) {
		mu.Lock()
		nacked = append(nacked, d)
		n := len(nacked)
		mu.Unlock()
		if n < 3 {
			_ = d.Nack(false, true)
		} else {
			_ = d.Ack(false)
		}
	})
	publishOne(t, nackCh, nackQueue, "m")
	testkit.Eventually(t, 10*time.Second, func() (struct{}, bool) {
		mu.Lock()
		defer mu.Unlock()
		return struct{}{}, len(nacked) == 3
	})
	mu.Lock()
	defer mu.Unlock()
	if _, ok := header(nacked[1], "x-delivery-count"); ok {
		t.Errorf("nack(requeue=true) không được tạo x-delivery-count")
	}
	if n, ok := header(nacked[1], "x-acquired-count"); !ok || n != 1 {
		t.Errorf("x-acquired-count sau nack đầu = %d (có=%v), mong đợi 1", n, ok)
	}
	if n, ok := header(nacked[2], "x-acquired-count"); !ok || n != 2 {
		t.Errorf("x-acquired-count sau nack thứ hai = %d (có=%v), mong đợi 2", n, ok)
	}
}

func TestWithoutPrefetchQuorumQueueDeliversAtMost2000Unacked(t *testing.T) {
	const published = 2100
	e := newEnv(t)
	conn := e.dial()
	publisher := e.channel(conn)
	queue := e.newQueue(publisher)

	// Consumer không gọi Qos và không bao giờ ack: prefetch không giới hạn.
	var received atomic.Int64
	e.consumeScripted(e.channel(conn), queue, func(amqp.Delivery) { received.Add(1) })
	bodies := make([]string, published)
	for i := range bodies {
		bodies[i] = "m"
	}
	publishOne(t, publisher, queue, bodies...)

	// Quorum queue tự chặn ở 2000 unacked: 100 message còn lại nằm ready trong queue.
	testkit.Eventually(t, 15*time.Second, func() (struct{}, bool) {
		return struct{}{}, readyCount(t, publisher, queue) == 100
	})
	testkit.Eventually(t, 15*time.Second, func() (struct{}, bool) { return struct{}{}, received.Load() == 2000 })
}
