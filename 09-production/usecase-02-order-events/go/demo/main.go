// Command demo cho thấy luồng order hướng sự kiện: POST /orders, outbox, relay, Kafka, hai consumer idempotent,
// saga hoàn kho khi thanh toán lỗi, event bị giao trùng và lag trên /metrics. Schema, topic và group bị xóa ở cuối.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	lab "github.com/nv-minh/mq-redis-handbook/09-production/usecase-02-order-events/go"
	"github.com/nv-minh/mq-redis-handbook/internal/testkit"
)

const sku = "ao-thun"

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

// waitFor poll tới khi cond đúng hoặc hết timeout.
func waitFor(timeout time.Duration, cond func() (bool, error)) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ok, err := cond()
		if err != nil {
			return err
		}
		if ok {
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("điều kiện chưa đạt sau %s", timeout)
}

type demo struct {
	pool *pgxpool.Pool
	svc  *lab.Service
	http http.Client
}

func (d *demo) post(body map[string]any) (int, map[string]any, error) {
	raw, _ := json.Marshal(body)
	resp, err := d.http.Post(d.svc.URL()+"/orders", "application/json", bytes.NewReader(raw))
	if err != nil {
		return 0, nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	var out map[string]any
	err = json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out, err
}

func (d *demo) text(sql string, args ...any) string {
	var s string
	if err := d.pool.QueryRow(context.Background(), sql, args...).Scan(&s); err != nil {
		return "?"
	}
	return s
}

func (d *demo) stock() string {
	return d.text("select quantity::text from stock where sku = $1", sku)
}

func (d *demo) trail(orderID string) string {
	rows, err := d.pool.Query(context.Background(), "select status from order_history where order_id = $1 order by id", orderID)
	if err != nil {
		return "?"
	}
	defer rows.Close()
	var parts []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err == nil {
			parts = append(parts, s)
		}
	}
	return strings.Join(parts, " -> ")
}

// metric đọc giá trị một series từ endpoint /metrics thật, 0 nếu series chưa có.
func (d *demo) metric(name string, labels map[string]string) float64 {
	resp, err := d.http.Get(d.svc.MetricsURL())
	if err != nil {
		return 0
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	for _, line := range strings.Split(string(body), "\n") {
		if !strings.HasPrefix(line, name+"{") {
			continue
		}
		ok := true
		for k, v := range labels {
			ok = ok && strings.Contains(line, fmt.Sprintf("%s=%q", k, v))
		}
		if ok {
			var value float64
			_, _ = fmt.Sscanf(line[strings.LastIndex(line, " ")+1:], "%g", &value)
			return value
		}
	}
	return 0
}

func run() (err error) {
	ctx := context.Background()
	schema := testkit.UniqueName("demo-orders")
	topic := testkit.UniqueName("demo-order-events")
	groupPrefix := testkit.UniqueName("demo-group")

	// Dọn dẹp chạy ngược thứ tự đăng ký (relay và service trước, rồi pool, schema, group, topic).
	// Mỗi bước chạy dù bước trước lỗi. Lỗi dọn dẹp không che lỗi của phần chính.
	var cleanups []func() error
	defer func() {
		var errs []error
		for i := len(cleanups) - 1; i >= 0; i-- {
			errs = append(errs, cleanups[i]())
		}
		if cleanupErr := errors.Join(errs...); cleanupErr != nil && err == nil {
			err = cleanupErr
		}
		if err == nil {
			fmt.Println("Đã dọn schema, topic và group của demo.")
		}
	}()
	cleanups = append(cleanups,
		func() error { return lab.DeleteTopic(context.WithoutCancel(ctx), topic) },
		func() error { return lab.DeleteGroup(context.WithoutCancel(ctx), groupPrefix+"-payment") },
		func() error { return lab.DeleteGroup(context.WithoutCancel(ctx), groupPrefix+"-inventory") },
	)

	// Chuẩn bị: schema, topic 3 partition, kho 10 áo và service (HTTP + producer + metrics).
	if err := lab.CreateSchema(ctx, schema); err != nil {
		return err
	}
	cleanups = append(cleanups, func() error { return lab.DropSchema(context.WithoutCancel(ctx), schema) })
	pool, err := lab.OpenPool(ctx, schema)
	if err != nil {
		return err
	}
	cleanups = append(cleanups, func() error { pool.Close(); return nil })
	if err := lab.CreateTables(ctx, pool); err != nil {
		return err
	}
	if err := lab.CreateTopic(ctx, topic, 3); err != nil {
		return err
	}
	if err := lab.SeedStock(ctx, pool, sku, 10); err != nil {
		return err
	}
	svc, err := lab.NewService(ctx, lab.Config{Pool: pool, Topic: topic, GroupPrefix: groupPrefix})
	if err != nil {
		return err
	}
	cleanups = append(cleanups, func() error { return svc.Close(context.WithoutCancel(ctx)) })
	d := &demo{pool: pool, svc: svc, http: http.Client{Timeout: 10 * time.Second}}
	fmt.Printf("Service đang chạy tại %s, metrics tại %s\n", svc.URL(), svc.MetricsURL())
	fmt.Printf("Kho ban đầu: %s sản phẩm %s\n\n", d.stock(), sku)

	if _, err := svc.StartConsumer(lab.ConsumerInventory, lab.ConsumerOptions{}); err != nil {
		return err
	}
	if _, err := svc.StartConsumer(lab.ConsumerPayment, lab.ConsumerOptions{PaymentLimit: 1000}); err != nil {
		return err
	}
	// Relay chạy nền: mỗi 200 ms gom các dòng outbox chưa gửi và publish lên Kafka.
	relayCtx, stopRelay := context.WithCancel(ctx)
	var relayDone sync.WaitGroup
	relayDone.Add(1)
	go func() {
		defer relayDone.Done()
		for {
			if _, err := svc.RunRelay(relayCtx); err != nil && relayCtx.Err() == nil {
				fmt.Printf("[relay] lỗi, sẽ thử lại: %v\n", err)
			}
			select {
			case <-relayCtx.Done():
				return
			case <-time.After(200 * time.Millisecond):
			}
		}
	}()
	var stopOnce sync.Once
	stop := func() error { stopOnce.Do(func() { stopRelay(); relayDone.Wait() }); return nil }
	cleanups = append(cleanups, stop)

	fmt.Println("== 1. Đặt order hợp lệ (amount 250): reserve kho, charge, order thành paid ==")
	status, out, err := d.post(map[string]any{"amount": 250, "sku": sku, "quantity": 2})
	if err != nil {
		return err
	}
	okID, _ := out["id"].(string)
	fmt.Printf("POST /orders -> %d, id %s\n", status, okID)
	if err := waitFor(30*time.Second, func() (bool, error) { return d.text("select status from orders where id = $1", okID) == "paid", nil }); err != nil {
		return err
	}
	fmt.Printf("trạng thái: %s\n", d.trail(okID))
	fmt.Printf("kho còn %s (trừ 2)\n\n", d.stock())

	fmt.Println("== 2. Thanh toán lỗi (amount 5000 vượt hạn mức 1000): saga hoàn kho ==")
	status, out, err = d.post(map[string]any{"amount": 5000, "sku": sku, "quantity": 3})
	if err != nil {
		return err
	}
	badID, _ := out["id"].(string)
	fmt.Printf("POST /orders -> %d, id %s\n", status, badID)
	if err := waitFor(30*time.Second, func() (bool, error) {
		return d.text("select status from orders where id = $1", badID) == "cancelled", nil
	}); err != nil {
		return err
	}
	fmt.Printf("trạng thái: %s\n", d.trail(badID))
	fmt.Printf("kho còn %s: 3 sản phẩm đã được giữ rồi trả lại (compensation)\n\n", d.stock())

	fmt.Println("== 3. Event bị giao trùng: consumer dedupe theo eventId trong cùng transaction với effect ==")
	var eventID string
	var payload []byte
	if err := pool.QueryRow(ctx, "select event_id, payload from outbox where aggregate_id = $1 and type = 'OrderPlaced'", okID).Scan(&eventID, &payload); err != nil {
		return err
	}
	envelope, _ := json.Marshal(lab.DomainEvent{EventID: eventID, Type: lab.EventOrderPlaced, OrderID: okID, Data: payload})
	producer := lab.NewProducer()
	cleanups = append(cleanups, producer.Close)
	for range 2 {
		if err := producer.Send(ctx, topic, okID, string(envelope)); err != nil {
			return err
		}
	}
	paymentDup := map[string]string{"consumer": "payment", "result": "duplicate"}
	if err := waitFor(30*time.Second, func() (bool, error) { return d.metric("consumer_events_total", paymentDup) >= 2, nil }); err != nil {
		return err
	}
	fmt.Printf("đã publish lại OrderPlaced của %s hai lần\n", okID)
	fmt.Printf("payment duplicate = %v, số dòng payment của order = %s, kho còn %s\n",
		d.metric("consumer_events_total", paymentDup), d.text("select count(*)::text from payments where order_id = $1", okID), d.stock())
	fmt.Println("effect vẫn chỉ áp dụng một lần.")
	fmt.Println()

	fmt.Println("== 4. Lag của consumer group trên /metrics ==")
	lagOf := func(name string) float64 {
		return d.metric("consumer_group_lag", map[string]string{"group": svc.GroupID(name), "topic": topic})
	}
	if err := waitFor(30*time.Second, func() (bool, error) { return lagOf(lab.ConsumerInventory) == 0, nil }); err != nil {
		return err
	}
	for _, name := range []string{lab.ConsumerInventory, lab.ConsumerPayment} {
		fmt.Printf("consumer_group_lag{group=%q} = %v\n", svc.GroupID(name), lagOf(name))
	}
	fmt.Println("lag 0: hai consumer đã xử lý và commit offset mọi event trên topic.")
	fmt.Println()

	fmt.Println("== 5. Các event trên topic (key là order id) ==")
	_ = stop()
	if _, err := svc.RunRelay(ctx); err != nil {
		return err
	}
	records, err := lab.ReadTopic(ctx, topic)
	if err != nil {
		return err
	}
	for _, r := range records {
		if r.Event == nil {
			fmt.Printf("partition %d offset %d  poison\n", r.Partition, r.Offset)
			continue
		}
		fmt.Printf("partition %d offset %d  %s  order %s\n", r.Partition, r.Offset, r.Event.Type, r.Event.OrderID)
	}
	fmt.Println("\nGiao hàng là at-least-once từ đầu tới cuối; tính đúng đắn (effectively-once) đến từ dedup trong transaction của Postgres.")
	return nil
}
