// Command demo cho ba message đi qua cùng một worker: "ok" thành công ngay, "flaky" lỗi hai lần rồi thành công,
// "poison" luôn lỗi nên đi hết vòng retry (reject -> queue retry chờ TTL -> work) rồi rơi vào DLQ.
// Cuối cùng in x-death của message trong DLQ.
package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	lab "github.com/nv-minh/mq-redis-handbook/05-rabbitmq/lab-03-retry-dlx/go"
	"github.com/nv-minh/mq-redis-handbook/internal/testkit"
	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	maxRetries = 3
	retryDelay = 500 * time.Millisecond
)

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
	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	check, err := conn.Channel()
	if err != nil {
		return err
	}

	name := testkit.UniqueName("demo-retry")
	defer func() {
		for _, suffix := range []string{".work", ".retry", ".dlq"} {
			if _, delErr := check.QueueDelete(name+suffix, false, false, false); delErr != nil && err == nil {
				err = delErr
			}
		}
		for _, suffix := range []string{".work", ".retry", ".dlq"} {
			if delErr := check.ExchangeDelete(name+suffix, false, false); delErr != nil && err == nil {
				err = delErr
			}
		}
	}()
	queues, err := lab.SetupRetryTopology(ch, name, lab.Options{MaxRetries: maxRetries, RetryDelay: retryDelay})
	if err != nil {
		return err
	}
	fmt.Printf("topology: work=%s\n", queues.Work)
	fmt.Printf("          retry=%s (TTL %d ms, dead-letter ngược về work)\n", queues.Retry, retryDelay.Milliseconds())
	fmt.Printf("          dlq=%s, tối đa %d lần retry\n", queues.Dlq, maxRetries)

	started := time.Now()
	var mu sync.Mutex
	attempts := map[string]int{}
	logf := func(format string, args ...any) {
		fmt.Printf("[+%5d ms] %s\n", time.Since(started).Milliseconds(), fmt.Sprintf(format, args...))
	}
	done, err := lab.StartWorker(ch, queues, func(body []byte) error {
		mu.Lock()
		attempts[string(body)]++
		attempt := attempts[string(body)]
		mu.Unlock()
		fails := string(body) == "poison" || (string(body) == "flaky" && attempt <= 2)
		result := "thành công"
		if fails {
			result = "LỖI"
		}
		logf("%s: lần xử lý %d %s", body, attempt, result)
		if fails {
			return fmt.Errorf("%s lỗi ở lần %d", body, attempt)
		}
		return nil
	}, "")
	if err != nil {
		return err
	}
	// Đóng channel của worker trước khi dọn topology, và chờ goroutine của nó thoát.
	defer func() { <-done }()
	defer func() { _ = ch.Close() }()

	for _, body := range []string{"ok", "flaky", "poison"} {
		if err := lab.PublishWork(ctx, ch, queues, body); err != nil {
			return err
		}
	}

	deadline := time.Now().Add(20 * time.Second)
	for {
		q, err := check.QueueDeclarePassive(queues.Dlq, true, false, false, false, nil)
		if err != nil {
			return err
		}
		if q.Messages == 1 {
			break
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("DLQ không nhận được message sau 20 giây")
		}
		time.Sleep(50 * time.Millisecond)
	}
	dead, ok, err := check.Get(queues.Dlq, true)
	if err != nil || !ok {
		return fmt.Errorf("Get từ DLQ = %v, %v", ok, err)
	}
	logf("DLQ nhận %s", dead.Body)
	fmt.Printf("x-failure-reason: %v\n", dead.Headers["x-failure-reason"])
	fmt.Printf("x-attempts: %v lần xử lý (1 lần đầu + %d lần retry)\n", dead.Headers["x-attempts"], maxRetries)
	deaths, _ := dead.Headers["x-death"].([]interface{})
	for _, entry := range deaths {
		death, _ := entry.(amqp.Table)
		fmt.Printf("x-death: queue=%v reason=%v count=%v\n", death["queue"], death["reason"], death["count"])
	}
	fmt.Println("Kết luận: x-death đếm mỗi lần message bị dead-letter, worker dùng nó để biết khi nào bỏ cuộc và chuyển sang DLQ.")
	return nil
}
