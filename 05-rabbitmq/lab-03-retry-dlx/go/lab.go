// Package lab cài đặt retry bằng dead letter exchange và TTL trên RabbitMQ: work queue, queue retry
// (wait queue) với TTL cố định, và dead letter queue (DLQ) cho message hết lượt retry.
package lab

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// URL là địa chỉ broker. Lab đọc AMQP_URL, mặc định là RabbitMQ của make up.
func URL() string {
	if url := os.Getenv("AMQP_URL"); url != "" {
		return url
	}
	return "amqp://guest:guest@127.0.0.1:5672"
}

// Options cấu hình topology retry.
type Options struct {
	// MaxRetries là số lần retry tối đa sau lần xử lý đầu tiên. 0 nghĩa là lỗi đầu tiên đi thẳng vào DLQ.
	MaxRetries int
	// RetryDelay là thời gian message nằm chờ trong queue retry trước khi quay lại work queue.
	// Giá trị được làm tròn xuống mili giây vì x-message-ttl tính bằng ms.
	RetryDelay time.Duration
}

// Queues là tên ba queue của topology, và MaxRetries để worker biết khi nào bỏ cuộc.
// Mỗi queue có một direct exchange cùng tên: <name>.work, <name>.retry, <name>.dlq.
type Queues struct {
	Work       string
	Retry      string
	Dlq        string
	MaxRetries int
}

// Routing key cố định cho từng chặng. Đặt x-dead-letter-routing-key tường minh thay vì dùng routing key gốc
// để vòng work -> retry -> work không phụ thuộc vào key mà publisher chọn.
const (
	workKey  = "task"
	retryKey = "retry"
	dlqKey   = "dead"
)

// SetupRetryTopology dựng topology retry bằng DLX và TTL (cả ba queue đều là quorum, khai báo tường minh):
//
//	publisher -> exchange work -> queue work --(reject requeue=false: dead-letter)--> exchange retry
//	exchange retry -> queue retry (không có consumer, x-message-ttl = RetryDelay)
//	queue retry --(hết TTL: dead-letter)--> exchange work -> queue work (thử lại)
//	worker đã hết lượt retry -> exchange dlq -> queue dlq (parking lot, cần người xử lý)
//
// Queue retry dùng một TTL cố định cho mọi message. Message hết hạn ở đầu queue, nên trộn nhiều TTL khác nhau
// trong cùng một queue sẽ làm message TTL ngắn bị chặn sau message TTL dài.
// Muốn nhiều mức delay thì dùng một queue retry cho mỗi mức.
func SetupRetryTopology(ch *amqp.Channel, name string, opts Options) (Queues, error) {
	if opts.MaxRetries < 0 {
		return Queues{}, fmt.Errorf("MaxRetries phải không âm, nhận %d", opts.MaxRetries)
	}
	delayMs := opts.RetryDelay.Milliseconds()
	if delayMs <= 0 {
		return Queues{}, fmt.Errorf("RetryDelay phải ít nhất 1 ms, nhận %v", opts.RetryDelay)
	}
	q := Queues{Work: name + ".work", Retry: name + ".retry", Dlq: name + ".dlq", MaxRetries: opts.MaxRetries}
	for _, exchange := range []string{q.Work, q.Retry, q.Dlq} {
		if err := ch.ExchangeDeclare(exchange, amqp.ExchangeDirect, true, false, false, false, nil); err != nil {
			return q, err
		}
	}
	declare := func(queue string, extra amqp.Table) error {
		args := amqp.Table{"x-queue-type": "quorum"}
		for k, v := range extra {
			args[k] = v
		}
		_, err := ch.QueueDeclare(queue, true, false, false, false, args)
		return err
	}
	if err := declare(q.Work, amqp.Table{"x-dead-letter-exchange": q.Retry, "x-dead-letter-routing-key": retryKey}); err != nil {
		return q, err
	}
	if err := declare(q.Retry, amqp.Table{"x-message-ttl": delayMs, "x-dead-letter-exchange": q.Work, "x-dead-letter-routing-key": workKey}); err != nil {
		return q, err
	}
	if err := declare(q.Dlq, nil); err != nil {
		return q, err
	}
	for _, bind := range [][3]string{{q.Work, workKey, q.Work}, {q.Retry, retryKey, q.Retry}, {q.Dlq, dlqKey, q.Dlq}} {
		if err := ch.QueueBind(bind[0], bind[1], bind[2], false, nil); err != nil {
			return q, err
		}
	}
	return q, nil
}

// PublishWork bật confirm mode, publish một task vào exchange work (persistent) và chờ broker confirm.
func PublishWork(ctx context.Context, ch *amqp.Channel, q Queues, body string) error {
	if err := ch.Confirm(false); err != nil {
		return err
	}
	return publishConfirmed(ctx, ch, q.Work, workKey, amqp.Publishing{DeliveryMode: amqp.Persistent, Body: []byte(body)})
}

// Attempts trả về số lần message đã thất bại và bị reject khỏi work queue, đọc từ header x-death.
// x-death là array các table, mỗi phần tử gom theo cặp {queue, reason} với count là số lần dead-letter.
// Chỉ tính phần tử của queue work với reason rejected, vì phần tử của queue retry (reason expired) đếm cùng các lần đó.
// amqp091-go v1.15.0 giải mã x-death thành []interface{} của amqp.Table và count thành int64.
func Attempts(headers amqp.Table, workQueue string) int {
	deaths, _ := headers["x-death"].([]interface{})
	total := 0
	for _, entry := range deaths {
		death, ok := entry.(amqp.Table)
		if !ok {
			continue
		}
		if count, ok := death["count"].(int64); ok && death["queue"] == workQueue && death["reason"] == "rejected" {
			total += int(count)
		}
	}
	return total
}

// StartWorker chạy một worker trên queue work trong một goroutine, mỗi lần xử lý một message (prefetch 1),
// và trả về kênh done đóng khi goroutine thoát (khi channel hoặc connection đóng).
//
//   - handler thành công: ack.
//   - handler lỗi và attempts < MaxRetries: Reject(requeue=false). Broker dead-letter message sang exchange retry,
//     message nằm trong queue retry đủ RetryDelay rồi tự quay lại work. Phải dùng reject chứ không dùng nack
//     khi cần x-death tăng, và từ 4.3 chỉ reject mới tính vào delivery limit.
//   - handler lỗi và attempts >= MaxRetries: publish một bản sao sang DLQ, giữ nguyên header (kể cả x-death)
//     cộng x-failure-reason (lỗi cuối cùng) và x-attempts (tổng số lần đã xử lý), chờ broker confirm rồi mới ack bản gốc.
//     Nếu publish sang DLQ không được confirm thì Reject(requeue=true): message không bao giờ bị mất, tệ nhất là
//     xuất hiện hai bản ở DLQ nếu worker chết giữa lúc confirm và ack (at-least-once).
//
// Tổng số lần handler được gọi cho một message luôn bằng MaxRetries + 1.
// Worker bật confirm mode trên ch vì nó publish sang DLQ trên chính channel này.
func StartWorker(ch *amqp.Channel, q Queues, handler func(body []byte) error, consumerTag string) (<-chan struct{}, error) {
	if err := ch.Confirm(false); err != nil {
		return nil, fmt.Errorf("confirm.select: %w", err)
	}
	if err := ch.Qos(1, 0, false); err != nil {
		return nil, fmt.Errorf("basic.qos: %w", err)
	}
	deliveries, err := ch.Consume(q.Work, consumerTag, false, false, false, false, nil)
	if err != nil {
		return nil, fmt.Errorf("basic.consume: %w", err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for d := range deliveries {
			handle(ch, q, handler, d)
		}
	}()
	return done, nil
}

// handle xử lý một delivery. Ack và Reject trả lỗi khi channel đã đóng. Khi đó broker đã tự requeue message nên bỏ qua.
func handle(ch *amqp.Channel, q Queues, handler func([]byte) error, d amqp.Delivery) {
	failure := handler(d.Body)
	if failure == nil {
		_ = d.Ack(false)
		return
	}
	attempts := Attempts(d.Headers, q.Work)
	if attempts < q.MaxRetries {
		_ = d.Reject(false)
		return
	}
	headers := amqp.Table{}
	for k, v := range d.Headers {
		headers[k] = v
	}
	headers["x-failure-reason"] = failure.Error()
	headers["x-attempts"] = int64(attempts + 1)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := publishConfirmed(ctx, ch, q.Dlq, dlqKey, amqp.Publishing{DeliveryMode: amqp.Persistent, Headers: headers, Body: d.Body})
	if err != nil {
		_ = d.Reject(true)
		return
	}
	_ = d.Ack(false)
}

func publishConfirmed(ctx context.Context, ch *amqp.Channel, exchange, routingKey string, msg amqp.Publishing) error {
	confirmation, err := ch.PublishWithDeferredConfirmWithContext(ctx, exchange, routingKey, false, false, msg)
	if err != nil {
		return err
	}
	acked, err := confirmation.WaitContext(ctx)
	if err != nil {
		return err
	}
	if !acked {
		return errors.New("broker nack message khi publish")
	}
	return nil
}
