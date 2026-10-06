package lab

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/nv-minh/mq-redis-handbook/internal/testkit"
	amqp "github.com/rabbitmq/amqp091-go"
)

var errDownstream = errors.New("lỗi giả lập: downstream không phản hồi")

// env giữ connection, worker và tên topology của một test để t.Cleanup đóng và xóa hết, kể cả khi test fail.
type env struct {
	t     *testing.T
	conns []*amqp.Connection
	dones []<-chan struct{}
	names []string
}

func newEnv(t *testing.T) *env {
	t.Helper()
	e := &env{t: t}
	t.Cleanup(func() {
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
		if len(e.names) == 0 {
			return
		}
		conn, err := amqp.Dial(URL())
		if err != nil {
			t.Errorf("không dọn được topology: %v", err)
			return
		}
		defer func() { _ = conn.Close() }()
		ch, err := conn.Channel()
		if err != nil {
			t.Errorf("mở channel để dọn topology: %v", err)
			return
		}
		// Tên queue và exchange của topology đều là <name>.work, <name>.retry, <name>.dlq.
		var all []string
		for _, n := range e.names {
			all = append(all, n+".work", n+".retry", n+".dlq")
		}
		for _, name := range all {
			if _, err := ch.QueueDelete(name, false, false, false); err != nil {
				t.Errorf("xóa queue %s: %v", name, err)
			}
		}
		for _, name := range all {
			if err := ch.ExchangeDelete(name, false, false); err != nil {
				t.Errorf("xóa exchange %s: %v", name, err)
			}
		}
	})
	return e
}

type setup struct {
	e      *env
	ch     *amqp.Channel
	check  *amqp.Channel
	queues Queues
}

// setup dựng topology retry với tên duy nhất. Tên được đăng ký trước để cleanup xóa dù test fail.
func (e *env) setup(opts Options) *setup {
	e.t.Helper()
	conn, err := amqp.Dial(URL())
	if err != nil {
		e.t.Fatalf("không kết nối được RabbitMQ (%s): %v. Đã chạy make up chưa?", URL(), err)
	}
	e.conns = append(e.conns, conn)
	ch, err := conn.Channel()
	if err != nil {
		e.t.Fatalf("mở channel: %v", err)
	}
	name := testkit.UniqueName("lab03-retry")
	e.names = append(e.names, name)
	queues, err := SetupRetryTopology(ch, name, opts)
	if err != nil {
		e.t.Fatalf("SetupRetryTopology: %v", err)
	}
	return &setup{e: e, ch: ch, queues: queues}
}

// start chạy worker trên channel của setup, handler luôn lỗi và ghi lại thời điểm mỗi lần gọi.
func (s *setup) startFailing(calls *callLog) {
	s.e.t.Helper()
	done, err := StartWorker(s.ch, s.queues, func([]byte) error {
		calls.add(time.Now())
		return errDownstream
	}, testkit.UniqueName("worker"))
	if err != nil {
		s.e.t.Fatalf("StartWorker: %v", err)
	}
	s.e.dones = append(s.e.dones, done)
}

// callLog ghi thời điểm mỗi lần handler được gọi, an toàn khi gọi từ goroutine của worker.
type callLog struct {
	mu    sync.Mutex
	times []time.Time
}

func (c *callLog) add(at time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.times = append(c.times, at)
}

func (c *callLog) snapshot() []time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]time.Time(nil), c.times...)
}

func readyCount(t *testing.T, ch *amqp.Channel, queue string) int {
	t.Helper()
	q, err := ch.QueueDeclarePassive(queue, true, false, false, false, nil)
	if err != nil {
		t.Fatalf("QueueDeclarePassive %s: %v", queue, err)
	}
	return q.Messages
}

func (s *setup) publish(body string) {
	s.e.t.Helper()
	if err := PublishWork(context.Background(), s.ch, s.queues, body); err != nil {
		s.e.t.Fatalf("PublishWork: %v", err)
	}
}

// waitForDlq chờ tới khi DLQ có đủ count message.
func (s *setup) waitForDlq(count int) {
	s.e.t.Helper()
	testkit.Eventually(s.e.t, 15*time.Second, func() (struct{}, bool) {
		return struct{}{}, readyCount(s.e.t, s.checkChannel(), s.queues.Dlq) == count
	})
}

// checkChannel trả về channel riêng (mở một lần) cho các lần kiểm tra số message và lấy message từ DLQ.
// Channel của worker không dùng được vì nó đang bận consume và reject.
func (s *setup) checkChannel() *amqp.Channel {
	s.e.t.Helper()
	if s.check == nil {
		ch, err := s.e.conns[0].Channel()
		if err != nil {
			s.e.t.Fatalf("mở channel kiểm tra: %v", err)
		}
		s.check = ch
	}
	return s.check
}

func (s *setup) getDead() amqp.Delivery {
	s.e.t.Helper()
	d, ok, err := s.checkChannel().Get(s.queues.Dlq, true)
	if err != nil || !ok {
		s.e.t.Fatalf("Get từ DLQ = %v, %v; mong đợi một message", ok, err)
	}
	return d
}

func TestFailingMessageIsRetriedMaxRetriesTimesThenLandsInDlq(t *testing.T) {
	const maxRetries = 3
	s := newEnv(t).setup(Options{MaxRetries: maxRetries, RetryDelay: 100 * time.Millisecond})
	if s.queues.Work == s.queues.Retry || s.queues.Retry == s.queues.Dlq || s.queues.Work == s.queues.Dlq {
		t.Fatalf("ba queue phải khác tên nhau: %+v", s.queues)
	}
	var calls callLog
	s.startFailing(&calls)

	s.publish("poison")
	s.waitForDlq(1)

	// Lần xử lý đầu tiên cộng với maxRetries lần retry.
	if got := len(calls.snapshot()); got != maxRetries+1 {
		t.Fatalf("handler được gọi %d lần, mong đợi %d", got, maxRetries+1)
	}
	// Message không bị mất và không kẹt ở đâu khác: đúng một bản ở DLQ, work và retry đều rỗng.
	if got := readyCount(t, s.checkChannel(), s.queues.Work); got != 0 {
		t.Fatalf("work còn %d message, mong đợi 0", got)
	}
	if got := readyCount(t, s.checkChannel(), s.queues.Retry); got != 0 {
		t.Fatalf("retry còn %d message, mong đợi 0", got)
	}
	if dead := s.getDead(); string(dead.Body) != "poison" {
		t.Fatalf("message ở DLQ = %q, mong đợi poison", dead.Body)
	}
}

func TestDlqMessageCarriesXDeathCountAndReason(t *testing.T) {
	s := newEnv(t).setup(Options{MaxRetries: 2, RetryDelay: 100 * time.Millisecond})
	var calls callLog
	s.startFailing(&calls)

	s.publish("poison")
	s.waitForDlq(1)

	dead := s.getDead()
	// x-death là array các table, một phần tử cho mỗi cặp {queue, reason}, lần dead-letter gần nhất đứng đầu.
	// Hình dạng đo được với amqp091-go v1.15.0: []interface{} của amqp.Table.
	xDeath, ok := dead.Headers["x-death"].([]interface{})
	if !ok || len(xDeath) != 2 {
		t.Fatalf("x-death = %#v, mong đợi array hai phần tử", dead.Headers["x-death"])
	}
	expired := mustTable(t, xDeath[0])
	rejected := mustTable(t, xDeath[1])
	assertEntry(t, expired, s.queues.Retry, "expired", 2, s.queues.Retry, "retry")
	assertEntry(t, rejected, s.queues.Work, "rejected", 2, s.queues.Work, "task")
	// time được giải mã thành time.Time (độ phân giải giây).
	if _, ok := rejected["time"].(time.Time); !ok {
		t.Errorf("x-death[].time có kiểu %T, mong đợi time.Time", rejected["time"])
	}
	if dead.Headers["x-first-death-reason"] != "rejected" || dead.Headers["x-first-death-queue"] != s.queues.Work {
		t.Errorf("x-first-death-* = %v / %v, mong đợi rejected / %s",
			dead.Headers["x-first-death-reason"], dead.Headers["x-first-death-queue"], s.queues.Work)
	}
	// Hai header do worker thêm khi chuyển message sang DLQ: lý do lỗi cuối cùng và tổng số lần đã xử lý.
	if dead.Headers["x-failure-reason"] != errDownstream.Error() {
		t.Errorf("x-failure-reason = %v, mong đợi %q", dead.Headers["x-failure-reason"], errDownstream.Error())
	}
	if attempts, ok := dead.Headers["x-attempts"].(int64); !ok || attempts != 3 {
		t.Errorf("x-attempts = %#v, mong đợi int64(3)", dead.Headers["x-attempts"])
	}
}

func mustTable(t *testing.T, v interface{}) amqp.Table {
	t.Helper()
	table, ok := v.(amqp.Table)
	if !ok {
		t.Fatalf("phần tử x-death có kiểu %T, mong đợi amqp.Table", v)
	}
	return table
}

func assertEntry(t *testing.T, entry amqp.Table, queue, reason string, count int64, exchange, routingKey string) {
	t.Helper()
	// count có kiểu int64 (kiểu `l` của AMQP) trong amqp091-go v1.15.0.
	got, ok := entry["count"].(int64)
	if !ok {
		t.Errorf("count có kiểu %T, mong đợi int64", entry["count"])
	}
	keys, _ := entry["routing-keys"].([]interface{})
	if entry["queue"] != queue || entry["reason"] != reason || got != count ||
		entry["exchange"] != exchange || len(keys) != 1 || keys[0] != routingKey {
		t.Errorf("x-death entry = %v, mong đợi queue=%s reason=%s count=%d exchange=%s routing-keys=[%s]",
			entry, queue, reason, count, exchange, routingKey)
	}
}

func TestRetryRespectsRetryDelay(t *testing.T) {
	const retryDelay = 300 * time.Millisecond
	s := newEnv(t).setup(Options{MaxRetries: 1, RetryDelay: retryDelay})
	// Thời điểm handler thất bại ngay trước khi worker reject, và thời điểm message được giao lại (cùng đồng hồ monotonic).
	var calls callLog
	s.startFailing(&calls)

	s.publish("poison")
	s.waitForDlq(1)

	times := calls.snapshot()
	if len(times) != 2 {
		t.Fatalf("handler được gọi %d lần, mong đợi 2", len(times))
	}
	// Chỉ khẳng định cận dưới (sai số 20 ms), không bao giờ khẳng định cận trên chặt: tải của máy làm thời gian dài ra.
	if elapsed := times[1].Sub(times[0]); elapsed < retryDelay-20*time.Millisecond {
		t.Fatalf("message được giao lại sau %v, mong đợi ít nhất %v", elapsed, retryDelay-20*time.Millisecond)
	}
}

func TestMaxRetriesZeroSendsFirstFailureStraightToDlq(t *testing.T) {
	s := newEnv(t).setup(Options{MaxRetries: 0, RetryDelay: 100 * time.Millisecond})
	var calls callLog
	s.startFailing(&calls)

	s.publish("poison")
	s.waitForDlq(1)

	if got := len(calls.snapshot()); got != 1 {
		t.Fatalf("handler được gọi %d lần, mong đợi 1", got)
	}
	if got := readyCount(t, s.checkChannel(), s.queues.Retry); got != 0 {
		t.Fatalf("retry có %d message, mong đợi 0", got)
	}
	dead := s.getDead()
	// Message chưa từng đi qua queue retry nên chưa có x-death, chỉ có lý do lỗi do worker ghi.
	if _, has := dead.Headers["x-death"]; has {
		t.Errorf("message ở DLQ không được có x-death, nhận %v", dead.Headers["x-death"])
	}
	if dead.Headers["x-failure-reason"] != errDownstream.Error() {
		t.Errorf("x-failure-reason = %v, mong đợi %q", dead.Headers["x-failure-reason"], errDownstream.Error())
	}
	if attempts, ok := dead.Headers["x-attempts"].(int64); !ok || attempts != 1 {
		t.Errorf("x-attempts = %#v, mong đợi int64(1)", dead.Headers["x-attempts"])
	}
}

func TestNackWithoutRequeueDeadLettersAndRaisesXDeathCount(t *testing.T) {
	s := newEnv(t).setup(Options{MaxRetries: 5, RetryDelay: 100 * time.Millisecond})
	// Không dùng worker của lab: tự consume để nack lần đầu bằng basic.nack (requeue=false) rồi quan sát lần giao lại.
	deliveries, err := s.ch.Consume(s.queues.Work, testkit.UniqueName("scripted"), false, false, false, false, nil)
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	second := make(chan amqp.Delivery, 1)
	done := make(chan struct{})
	s.e.dones = append(s.e.dones, done)
	go func() {
		defer close(done)
		first := true
		for d := range deliveries {
			if first {
				first = false
				_ = d.Nack(false, false)
				continue
			}
			_ = d.Ack(false)
			select {
			case second <- d:
			default:
			}
		}
	}()

	s.publish("poison")
	select {
	case d := <-second:
		// nack(requeue=false) dead-letter giống hệt reject(requeue=false): message đi qua queue retry và x-death của queue work tăng.
		if got := Attempts(d.Headers, s.queues.Work); got != 1 {
			t.Errorf("Attempts sau nack(requeue=false) = %d, mong đợi 1", got)
		}
		if d.Headers["x-first-death-reason"] != "rejected" {
			t.Errorf("x-first-death-reason = %v, mong đợi rejected", d.Headers["x-first-death-reason"])
		}
	case <-time.After(10 * time.Second):
		t.Fatal("message không quay lại work queue sau khi nack(requeue=false)")
	}
}
