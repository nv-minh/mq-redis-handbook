// Command demo cho thấy transactional outbox: đặt 3 order (mỗi order một dòng outbox trong cùng transaction),
// mô phỏng relay chết sau khi publish event đầu và trước khi đánh dấu, rồi chạy lại relay.
// Consumer dedupe theo event id nên mỗi effect chỉ áp dụng một lần dù event đầu được giao hai lần.
// Schema và queue bị xóa ở cuối.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	lab "github.com/nv-minh/mq-redis-handbook/08-patterns/lab-05-outbox/go"
	"github.com/nv-minh/mq-redis-handbook/internal/testkit"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() (err error) {
	ctx := context.Background()
	schema := testkit.UniqueName("demo-outbox")
	queue := testkit.UniqueName("demo-outbox")
	defer func() {
		// Dọn xong mọi thứ kể cả khi demo lỗi giữa chừng. Lỗi dọn được gộp vào err.
		err = errors.Join(err, lab.DropSchema(ctx, schema))
	}()
	if err := lab.CreateSchema(ctx, schema); err != nil {
		return err
	}
	pool, err := lab.OpenPool(ctx, schema)
	if err != nil {
		return err
	}
	defer pool.Close()
	if err := lab.CreateTables(ctx, pool); err != nil {
		return err
	}

	conn, err := amqp.Dial(lab.AMQPURL())
	if err != nil {
		return err
	}
	defer func() {
		if cleaner, cerr := amqp.Dial(lab.AMQPURL()); cerr == nil {
			if ch, cerr := cleaner.Channel(); cerr == nil {
				_, _ = ch.QueueDelete(queue, false, false, false)
			}
			_ = cleaner.Close()
		}
		_ = conn.Close()
	}()
	pubCh, err := conn.Channel()
	if err != nil {
		return err
	}
	if err := lab.DeclareQueue(pubCh, queue); err != nil {
		return err
	}
	publish, err := lab.NewPublisher(pubCh, queue)
	if err != nil {
		return err
	}

	// Consumer dedupe theo event id: bản giao lặp được ack nhưng không áp dụng effect lần nữa.
	var mu sync.Mutex
	deliveries := map[string]int{}
	var order []string
	consCh, err := conn.Channel()
	if err != nil {
		return err
	}
	if err := lab.StartConsumer(consCh, queue, testkit.UniqueName("demo-consumer"), func(e lab.ReceivedEvent) {
		mu.Lock()
		defer mu.Unlock()
		deliveries[e.EventID]++
		n := deliveries[e.EventID]
		verdict := "trùng, bỏ qua effect"
		if n == 1 {
			order = append(order, e.EventID)
			verdict = "áp dụng effect"
		}
		fmt.Printf("  consumer: event %s (lần giao %d) -> %s\n", e.EventID, n, verdict)
	}); err != nil {
		return err
	}

	fmt.Println("== 1. PlaceOrder ghi order và dòng outbox trong cùng một transaction ==")
	for _, id := range []string{"order-1", "order-2", "order-3"} {
		if err := lab.PlaceOrder(ctx, pool, lab.Order{ID: id, Amount: 100}); err != nil {
			return err
		}
	}
	unpublished := func() (n int) {
		_ = pool.QueryRow(ctx, "select count(*) from outbox where published_at is null").Scan(&n)
		return n
	}
	fmt.Printf("đã ghi 3 order và 3 dòng outbox, chưa publish: %d\n", unpublished())

	fmt.Println("\n== 2. Relay chết SAU khi publish event đầu và TRƯỚC khi commit ==")
	var firstID int64
	_, relayErr := lab.RunRelay(ctx, pool, func(ctx context.Context, e lab.OutboxEvent) error {
		if err := publish(ctx, e); err != nil {
			return err
		}
		firstID = e.ID
		return errors.New("mô phỏng: relay chết ngay sau khi broker confirm")
	}, 10)
	fmt.Printf("relay lỗi: %v\n", relayErr)
	fmt.Printf("event %d đã tới broker nhưng transaction rollback\n", firstID)
	fmt.Printf("dòng chưa publish vẫn là %d (không dòng nào được đánh dấu)\n", unpublished())

	fmt.Println("\n== 3. Relay mới chạy lại, batchSize 2 ==")
	for {
		n, err := lab.RunRelay(ctx, pool, publish, 2)
		if err != nil {
			return err
		}
		fmt.Printf("RunRelay publish %d event\n", n)
		if n == 0 {
			break
		}
	}
	first := strconv.FormatInt(firstID, 10)
	deadline := time.Now().Add(10 * time.Second)
	for {
		mu.Lock()
		done := len(order) == 3 && deliveries[first] >= 2
		mu.Unlock()
		if done {
			break
		}
		if time.Now().After(deadline) {
			return errors.New("consumer chưa nhận đủ event sau 10 giây")
		}
		time.Sleep(20 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()
	fmt.Println("\n== Kết quả ==")
	for _, id := range order {
		fmt.Printf("  event %s: giao %d lần\n", id, deliveries[id])
	}
	fmt.Printf("effect đã áp dụng %d lần cho %d event khác nhau\n", len(order), len(deliveries))
	fmt.Println("Kết luận: at-least-once làm event đầu bị giao hai lần, dedupe theo event id giữ effect đúng một lần.")
	return nil
}
