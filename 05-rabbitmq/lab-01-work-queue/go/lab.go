// Package lab cài đặt một work queue trên RabbitMQ: quorum queue, publisher confirm,
// worker dùng manual ack và prefetch để chia việc công bằng.
package lab

import (
	"context"
	"errors"
	"fmt"
	"os"

	amqp "github.com/rabbitmq/amqp091-go"
)

// URL là địa chỉ broker. Lab đọc AMQP_URL, mặc định là RabbitMQ của make up.
func URL() string {
	if url := os.Getenv("AMQP_URL"); url != "" {
		return url
	}
	return "amqp://guest:guest@127.0.0.1:5672"
}

// DeclareWorkQueue khai báo work queue kiểu quorum, durable, không exclusive, không auto-delete.
//
// x-queue-type được truyền tường minh: compose của handbook đặt default_queue_type = quorum,
// nhưng broker mặc định (stock) là classic, và khai báo lại một queue với type khác sẽ lỗi 406.
// Quorum queue không thể exclusive, auto-delete hay transient.
func DeclareWorkQueue(ch *amqp.Channel, queue string) error {
	_, err := ch.QueueDeclare(queue, true, false, false, false, amqp.Table{"x-queue-type": "quorum"})
	return err
}

// PublishTasks publish các task vào queue (qua default exchange, routing key là tên queue) rồi chờ
// broker confirm từng message. Message là persistent. Hàm bật confirm mode trên channel
// và chỉ return khi broker đã nhận hết, nên người gọi không cần sleep.
func PublishTasks(ctx context.Context, ch *amqp.Channel, queue string, bodies []string) error {
	if err := ch.Confirm(false); err != nil {
		return err
	}
	confirmations := make([]*amqp.DeferredConfirmation, 0, len(bodies))
	for _, body := range bodies {
		c, err := ch.PublishWithDeferredConfirmWithContext(ctx, "", queue, false, false, amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			Body:         []byte(body),
		})
		if err != nil {
			return err
		}
		confirmations = append(confirmations, c)
	}
	for _, c := range confirmations {
		acked, err := c.WaitContext(ctx)
		if err != nil {
			return err
		}
		if !acked {
			return errors.New("broker nack message khi publish")
		}
	}
	return nil
}

// WorkerConfig mô tả một worker.
type WorkerConfig struct {
	Queue string
	// ConsumerTag trống thì broker tự sinh.
	ConsumerTag string
	// Prefetch là số message chưa ack tối đa broker giao cho worker. 0 là không giới hạn.
	Prefetch int
	// Handler xử lý một delivery. Trả nil thì worker ack, trả lỗi thì worker reject(requeue=true).
	Handler func(d amqp.Delivery) error
}

// StartWorker chạy một worker với manual ack trong một goroutine và trả về kênh done,
// kênh này đóng khi goroutine thoát (khi channel hoặc connection đóng).
//
// Prefetch quyết định chuyện fair dispatch: với prefetch 1 broker chỉ giao message mới khi message trước
// đã được ack, nên worker chậm không bị dồn việc. Worker không tự ack khi channel đóng:
// broker requeue mọi message chưa ack với cờ redelivered.
func StartWorker(ch *amqp.Channel, cfg WorkerConfig) (<-chan struct{}, error) {
	if cfg.Prefetch > 0 {
		if err := ch.Qos(cfg.Prefetch, 0, false); err != nil {
			return nil, fmt.Errorf("basic.qos: %w", err)
		}
	}
	deliveries, err := ch.Consume(cfg.Queue, cfg.ConsumerTag, false, false, false, false, nil)
	if err != nil {
		return nil, fmt.Errorf("basic.consume: %w", err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for d := range deliveries {
			// Ack hoặc Reject trả lỗi khi channel đã đóng. Khi đó broker đã tự requeue message nên bỏ qua.
			if err := cfg.Handler(d); err != nil {
				_ = d.Reject(true)
			} else {
				_ = d.Ack(false)
			}
		}
	}()
	return done, nil
}
