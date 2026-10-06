// Command demo so sánh prefetch không giới hạn với prefetch 1 trên hai worker (một nhanh, một chậm),
// rồi cho thấy message chưa ack được giao lại khi một worker crash.
package main

import (
	"context"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	lab "github.com/nv-minh/mq-redis-handbook/05-rabbitmq/lab-01-work-queue/go"
	"github.com/nv-minh/mq-redis-handbook/internal/testkit"
	amqp "github.com/rabbitmq/amqp091-go"
)

const total = 12

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() (err error) {
	ctx := context.Background()
	conn, err := amqp.Dial(lab.URL())
	if err != nil {
		return fmt.Errorf("không kết nối được RabbitMQ (%s): %w. Đã chạy make up chưa?", lab.URL(), err)
	}
	defer func() { _ = conn.Close() }()
	publisher, err := conn.Channel()
	if err != nil {
		return err
	}
	var queues []string
	defer func() {
		for _, q := range queues {
			if _, delErr := publisher.QueueDelete(q, false, false, false); delErr != nil && err == nil {
				err = delErr
			}
		}
	}()
	newQueue := func(prefix string) (string, error) {
		q := testkit.UniqueName(prefix)
		queues = append(queues, q)
		return q, lab.DeclareWorkQueue(publisher, q)
	}

	fmt.Printf("--- %d task, worker nhanh mất 20 ms, worker chậm mất 300 ms ---\n", total)
	for _, prefetch := range []int{0, 1} {
		queue, err := newQueue("demo-work")
		if err != nil {
			return err
		}
		var fast, slow, finished atomic.Int64
		allDone := make(chan struct{})
		handler := func(counter *atomic.Int64, d time.Duration) func(amqp.Delivery) error {
			return func(amqp.Delivery) error {
				time.Sleep(d)
				counter.Add(1)
				if finished.Add(1) == total {
					close(allDone)
				}
				return nil
			}
		}
		slowCh, err := conn.Channel()
		if err != nil {
			return err
		}
		fastCh, err := conn.Channel()
		if err != nil {
			return err
		}
		if _, err := lab.StartWorker(slowCh, lab.WorkerConfig{Queue: queue, Prefetch: prefetch, Handler: handler(&slow, 300*time.Millisecond)}); err != nil {
			return err
		}
		if _, err := lab.StartWorker(fastCh, lab.WorkerConfig{Queue: queue, Prefetch: prefetch, Handler: handler(&fast, 20*time.Millisecond)}); err != nil {
			return err
		}
		bodies := make([]string, total)
		for i := range bodies {
			bodies[i] = fmt.Sprintf("task-%d", i)
		}
		started := time.Now()
		if err := lab.PublishTasks(ctx, publisher, queue, bodies); err != nil {
			return err
		}
		<-allDone
		label := "không giới hạn"
		if prefetch > 0 {
			label = fmt.Sprint(prefetch)
		}
		fmt.Printf("prefetch %-14s: worker nhanh xử lý %2d, worker chậm xử lý %2d, xong sau %d ms\n",
			label, fast.Load(), slow.Load(), time.Since(started).Milliseconds())
		_ = slowCh.Close()
		_ = fastCh.Close()
	}
	fmt.Println("Kết luận: prefetch không giới hạn chia đều theo kiểu round-robin mù, prefetch 1 để việc chảy về worker rảnh.")

	fmt.Println("--- worker crash giữa chừng ---")
	queue, err := newQueue("demo-crash")
	if err != nil {
		return err
	}
	crashing, err := amqp.Dial(lab.URL())
	if err != nil {
		return err
	}
	crashCh, err := crashing.Channel()
	if err != nil {
		return err
	}
	received := make(chan struct{})
	release := make(chan struct{})
	aDone, err := lab.StartWorker(crashCh, lab.WorkerConfig{Queue: queue, Prefetch: 1, Handler: func(d amqp.Delivery) error {
		fmt.Printf("worker A nhận %s (redelivered=%v), chưa ack\n", d.Body, d.Redelivered)
		close(received)
		<-release
		return nil
	}})
	if err != nil {
		return err
	}
	if err := lab.PublishTasks(ctx, publisher, queue, []string{"job-1"}); err != nil {
		return err
	}
	<-received
	_ = crashing.Close()
	close(release)
	<-aDone
	fmt.Println("worker A crash (connection bị đóng) mà không ack")

	bCh, err := conn.Channel()
	if err != nil {
		return err
	}
	redelivered := make(chan struct{})
	if _, err := lab.StartWorker(bCh, lab.WorkerConfig{Queue: queue, Prefetch: 1, Handler: func(d amqp.Delivery) error {
		fmt.Printf("worker B nhận %s (redelivered=%v) và ack\n", d.Body, d.Redelivered)
		close(redelivered)
		return nil
	}}); err != nil {
		return err
	}
	<-redelivered
	fmt.Println("Kết luận: message chưa ack được broker requeue khi connection đóng, nên consumer cần idempotent.")
	return nil
}
