// Command demo publish cùng một bộ routing key tới các topic pattern khác nhau và in ra key nào vào được queue,
// rồi cho thấy message không route được bị bỏ lặng lẽ khi không có mandatory và bị trả về (312 NO_ROUTE) khi có mandatory.
package main

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	lab "github.com/nv-minh/mq-redis-handbook/05-rabbitmq/lab-02-topic-routing/go"
	"github.com/nv-minh/mq-redis-handbook/internal/testkit"
	amqp "github.com/rabbitmq/amqp091-go"
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
	var exchanges, queues []string
	defer func() {
		for _, q := range queues {
			if _, delErr := ch.QueueDelete(q, false, false, false); delErr != nil && err == nil {
				err = delErr
			}
		}
		for _, x := range exchanges {
			if delErr := ch.ExchangeDelete(x, false, false); delErr != nil && err == nil {
				err = delErr
			}
		}
	}()
	declare := func(prefix, pattern string) (lab.TopicRouting, error) {
		name := testkit.UniqueName(prefix)
		exchanges = append(exchanges, name)
		queues = append(queues, name+".queue")
		return lab.DeclareTopicRouting(ch, name, pattern)
	}
	count := func(queue string) (int, error) {
		q, err := ch.QueueDeclarePassive(queue, true, false, false, false, nil)
		return q.Messages, err
	}

	publisher, err := lab.NewPublisher(ch)
	if err != nil {
		return err
	}
	keys := []string{"orders", "orders.created", "orders.eu.created", "orders.eu.vn.created", "payments.created"}
	fmt.Printf("routing key được publish: %s\n", strings.Join(keys, ", "))

	// Số key khớp mong đợi của mỗi pattern, để biết khi nào queue đã nhận đủ.
	patterns := []struct {
		pattern string
		matched int
	}{{"orders.*.created", 1}, {"orders.#", 4}, {"orders.eu.created", 1}}
	for _, p := range patterns {
		r, err := declare("demo-topic", p.pattern)
		if err != nil {
			return err
		}
		for _, key := range keys {
			if _, err := publisher.Publish(ctx, r.Exchange, key, key, false); err != nil {
				return err
			}
		}
		// Confirm đã về, nhưng queue được cập nhật bất đồng bộ: poll tới khi queue có đủ số message mong đợi.
		deadline := time.Now().Add(10 * time.Second)
		for {
			n, err := count(r.Queue)
			if err != nil {
				return err
			}
			if n == p.matched {
				break
			}
			if time.Now().After(deadline) {
				return fmt.Errorf("queue %s có %d message, mong đợi %d", r.Queue, n, p.matched)
			}
			time.Sleep(50 * time.Millisecond)
		}
		received, err := lab.ReceiveRoutingKeys(ch, r.Queue)
		if err != nil {
			return err
		}
		fmt.Printf("binding %-18s nhận: %s\n", p.pattern, strings.Join(received, ", "))
	}
	fmt.Println("Kết luận: `*` khớp đúng một từ, `#` khớp không hoặc nhiều từ, binding không có wildcard khớp chính xác.")

	r, err := declare("demo-unrouted", "orders.eu.created")
	if err != nil {
		return err
	}
	fmt.Println("--- publish payments.refund, không có binding nào khớp ---")
	silent, err := publisher.Publish(ctx, r.Exchange, "payments.refund", "x", false)
	if err != nil {
		return err
	}
	n, err := count(r.Queue)
	if err != nil {
		return err
	}
	fmt.Printf("không mandatory: broker vẫn confirm, return=%s, queue có %d message\n", presence(silent), n)
	returned, err := publisher.Publish(ctx, r.Exchange, "payments.refund", "x", true)
	if err != nil {
		return err
	}
	if returned == nil {
		return fmt.Errorf("mandatory=true mà không có return")
	}
	fmt.Printf("mandatory=true : return %d %s cho routing key %s, đến trước confirm\n", returned.ReplyCode, returned.ReplyText, returned.RoutingKey)
	fmt.Println("Kết luận: confirm không chứng minh message vào queue, muốn biết phải dùng mandatory và nghe basic.return.")
	return nil
}

func presence(r *amqp.Return) string {
	if r == nil {
		return "không có"
	}
	return "có"
}
