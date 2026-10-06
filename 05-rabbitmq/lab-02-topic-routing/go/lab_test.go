package lab

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/nv-minh/mq-redis-handbook/internal/testkit"
	amqp "github.com/rabbitmq/amqp091-go"
)

// env giữ connection, exchange và queue của một test để t.Cleanup đóng và xóa hết, kể cả khi test fail.
type env struct {
	t         *testing.T
	conns     []*amqp.Connection
	exchanges []string
	queues    []string
}

func newEnv(t *testing.T) *env {
	t.Helper()
	e := &env{t: t}
	t.Cleanup(func() {
		for _, conn := range e.conns {
			_ = conn.Close()
		}
		if len(e.exchanges) == 0 && len(e.queues) == 0 {
			return
		}
		// Dọn bằng một connection mới: channel của test có thể đã chết vì lỗi protocol.
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
		for _, queue := range e.queues {
			if _, err := ch.QueueDelete(queue, false, false, false); err != nil {
				t.Errorf("xóa queue %s: %v", queue, err)
			}
		}
		for _, exchange := range e.exchanges {
			if err := ch.ExchangeDelete(exchange, false, false); err != nil {
				t.Errorf("xóa exchange %s: %v", exchange, err)
			}
		}
	})
	return e
}

func (e *env) channel() *amqp.Channel {
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
	return ch
}

// routing dựng topic exchange và queue với một binding, đăng ký tên để cleanup xóa dù test fail.
func (e *env) routing(ch *amqp.Channel, pattern string) TopicRouting {
	e.t.Helper()
	name := testkit.UniqueName("lab02-orders")
	// Đăng ký tên trước khi declare: xóa một exchange hoặc queue chưa tồn tại không phải lỗi.
	e.exchanges = append(e.exchanges, name)
	e.queues = append(e.queues, name+".queue")
	r, err := DeclareTopicRouting(ch, name, pattern)
	if err != nil {
		e.t.Fatalf("DeclareTopicRouting: %v", err)
	}
	return r
}

func messageCount(t *testing.T, ch *amqp.Channel, queue string) int {
	t.Helper()
	q, err := ch.QueueDeclarePassive(queue, true, false, false, false, nil)
	if err != nil {
		t.Fatalf("QueueDeclarePassive %s: %v", queue, err)
	}
	return q.Messages
}

func mustPublish(t *testing.T, p *Publisher, exchange, key string, mandatory bool) *amqp.Return {
	t.Helper()
	returned, err := p.Publish(context.Background(), exchange, key, key, mandatory)
	if err != nil {
		t.Fatalf("Publish %s: %v", key, err)
	}
	return returned
}

func waitForCount(t *testing.T, ch *amqp.Channel, queue string, want int) {
	t.Helper()
	// Confirm chỉ cho biết broker đã xử lý, queue được cập nhật gần như đồng thời nên chờ bằng Eventually.
	testkit.Eventually(t, 10*time.Second, func() (struct{}, bool) {
		return struct{}{}, messageCount(t, ch, queue) == want
	})
}

func assertKeys(t *testing.T, ch *amqp.Channel, queue string, want []string) {
	t.Helper()
	got, err := ReceiveRoutingKeys(ch, queue)
	if err != nil {
		t.Fatalf("ReceiveRoutingKeys: %v", err)
	}
	if !slices.Equal(got, want) {
		t.Fatalf("routing key nhận được = %v, mong đợi %v", got, want)
	}
}

func TestOrdersStarCreatedMatchesOneWordOnly(t *testing.T) {
	e := newEnv(t)
	ch := e.channel()
	r := e.routing(ch, "orders.*.created")
	p, err := NewPublisher(ch)
	if err != nil {
		t.Fatal(err)
	}

	// `*` thay thế đúng một từ: chỉ key đầu tiên khớp.
	mustPublish(t, p, r.Exchange, "orders.eu.created", false)
	mustPublish(t, p, r.Exchange, "orders.created", false)
	mustPublish(t, p, r.Exchange, "orders.eu.vn.created", false)

	waitForCount(t, ch, r.Queue, 1)
	assertKeys(t, ch, r.Queue, []string{"orders.eu.created"})
}

func TestOrdersHashMatchesZeroOrMoreWords(t *testing.T) {
	e := newEnv(t)
	ch := e.channel()
	r := e.routing(ch, "orders.#")
	p, err := NewPublisher(ch)
	if err != nil {
		t.Fatal(err)
	}

	// `#` thay thế không hoặc nhiều từ: khớp cả `orders` trơn, nhưng không khớp prefix khác.
	for _, key := range []string{"orders", "orders.eu", "orders.eu.vn.created", "payments.created"} {
		mustPublish(t, p, r.Exchange, key, false)
	}

	waitForCount(t, ch, r.Queue, 3)
	assertKeys(t, ch, r.Queue, []string{"orders", "orders.eu", "orders.eu.vn.created"})
}

func TestUnroutedMessageIsDroppedWithoutMandatoryFlag(t *testing.T) {
	e := newEnv(t)
	ch := e.channel()
	r := e.routing(ch, "orders.eu.created")
	p, err := NewPublisher(ch)
	if err != nil {
		t.Fatal(err)
	}

	// Không có binding nào khớp `payments.refund`, và không đặt mandatory.
	// Broker vẫn confirm (Publish chỉ return khi nhận confirm), nhưng message bị bỏ lặng lẽ.
	if returned := mustPublish(t, p, r.Exchange, "payments.refund", false); returned != nil {
		t.Fatalf("publish không mandatory không được có return, nhận %+v", returned)
	}
	// Confirm đã về nghĩa là broker đã route xong, nên queue chắc chắn không nhận được gì.
	if got := messageCount(t, ch, r.Queue); got != 0 {
		t.Fatalf("queue có %d message, mong đợi 0", got)
	}

	// Tương phản: cùng một routing key nhưng mandatory=true thì broker trả message về trước khi confirm.
	returned := mustPublish(t, p, r.Exchange, "payments.refund", true)
	if returned == nil {
		t.Fatal("mandatory=true với message không route được phải có return")
	}
	if returned.ReplyCode != 312 || returned.ReplyText != "NO_ROUTE" || returned.RoutingKey != "payments.refund" {
		t.Fatalf("return = %d %q key=%q, mong đợi 312 NO_ROUTE payments.refund",
			returned.ReplyCode, returned.ReplyText, returned.RoutingKey)
	}
	if got := messageCount(t, ch, r.Queue); got != 0 {
		t.Fatalf("queue có %d message sau mandatory publish, mong đợi 0", got)
	}

	// mandatory=true mà route được thì không có return.
	if returned := mustPublish(t, p, r.Exchange, "orders.eu.created", true); returned != nil {
		t.Fatalf("message route được không được có return, nhận %+v", returned)
	}
	waitForCount(t, ch, r.Queue, 1)
}
