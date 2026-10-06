// Command demo cho thấy DLQ trên Redis Streams: một poison message bị thử lại bằng XAUTOCLAIM tới
// MaxAttempts rồi vào DLQ kèm metadata, sau khi "sửa bug" thì replay về stream chính.
// Demo cũng mô phỏng một consumer chết giữa chừng.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	lab "github.com/nv-minh/mq-redis-handbook/08-patterns/lab-03-dlq/go"
	"github.com/nv-minh/mq-redis-handbook/internal/testkit"
	"github.com/redis/go-redis/v9"
)

const (
	group   = "workers"
	minIdle = 200 * time.Millisecond
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx := context.Background()
	url := os.Getenv("REDIS_URL")
	if url == "" {
		url = "redis://127.0.0.1:6379"
	}
	opts, err := redis.ParseURL(url)
	if err != nil {
		return err
	}
	commands := redis.NewClient(opts)
	blocking := redis.NewClient(opts)
	defer func() { _ = commands.Close() }()
	defer func() { _ = blocking.Close() }()

	stream := testkit.UniqueName("demo:orders")
	dlq := stream + ":dlq"
	defer commands.Del(ctx, stream, dlq)
	q := lab.NewQueue(lab.Config{
		Commands: commands, Blocking: blocking, Stream: stream, Group: group, DLQStream: dlq,
		Consumer: "consumer-1", MinIdle: minIdle, BlockTime: 100 * time.Millisecond,
	})
	if err := q.EnsureGroup(ctx); err != nil {
		return err
	}

	fmt.Println("== 1. Poison message: retry bằng XAUTOCLAIM rồi vào DLQ ==")
	if _, err := q.Publish(ctx, map[string]string{"order": "1", "note": "ok"}); err != nil {
		return err
	}
	poisonID, err := q.Publish(ctx, map[string]string{"order": "2", "note": "không parse được"})
	if err != nil {
		return err
	}
	stats, err := q.ConsumeWithDLQ(ctx, lab.Options{MaxAttempts: 3}, func(_ context.Context, d lab.Delivery) error {
		fmt.Printf("  handler nhận %s (lần giao thứ %d)\n", d.ID, d.DeliveryCount)
		if d.Fields["order"] == "2" {
			return errors.New("cannot parse order 2")
		}
		return nil
	})
	if err != nil {
		return err
	}
	fmt.Printf("thống kê: %+v\n", stats)
	pending, err := q.PendingEntries(ctx)
	if err != nil {
		return err
	}
	fmt.Printf("message poison %s đã được XACK khỏi main stream, pending còn %d entry\n", poisonID, len(pending))
	if err := printDLQ(ctx, q, false); err != nil {
		return err
	}

	fmt.Println("\n== 2. Sửa bug rồi replay DLQ về main stream ==")
	moved, err := q.ReplayDLQ(ctx, 10)
	if err != nil {
		return err
	}
	fmt.Printf("ReplayDLQ(10) đã chuyển %d entry (limit lớn hơn kích thước DLQ)\n", moved)
	moved, err = q.ReplayDLQ(ctx, 10)
	if err != nil {
		return err
	}
	fmt.Printf("ReplayDLQ(10) lần nữa trên DLQ rỗng: %d\n", moved)
	replayed, err := q.ConsumeWithDLQ(ctx, lab.Options{MaxAttempts: 3}, func(_ context.Context, d lab.Delivery) error {
		fmt.Printf("  handler đã sửa xử lý thành công %v\n", d.Fields)
		return nil
	})
	if err != nil {
		return err
	}
	fmt.Printf("thống kê sau replay: %+v\n", replayed)

	fmt.Println("\n== 3. Consumer chết giữa chừng: delivery count vượt maxAttempts thì vào DLQ ==")
	if _, err := q.Publish(ctx, map[string]string{"order": "3"}); err != nil {
		return err
	}
	// Mô phỏng consumer chết: đọc bằng XREADGROUP rồi bỏ đó, không xử lý, không XACK.
	if err := commands.Do(ctx, "XREADGROUP", "GROUP", group, "consumer-dead", "STREAMS", stream, ">").Err(); err != nil {
		return err
	}
	for {
		entries, err := q.PendingEntries(ctx)
		if err != nil {
			return err
		}
		if len(entries) == 1 && entries[0].Idle >= 2*minIdle {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	rescued, err := q.ConsumeWithDLQ(ctx, lab.Options{MaxAttempts: 1}, func(context.Context, lab.Delivery) error {
		fmt.Println("  handler không được gọi trong kịch bản này")
		return nil
	})
	if err != nil {
		return err
	}
	fmt.Printf("thống kê: %+v\n", rescued)
	if err := printDLQ(ctx, q, true); err != nil {
		return err
	}
	fmt.Println("Cửa sổ crash giữa XADD vào DLQ và XACK: entry có thể vào DLQ hai lần (at-least-once), xem README.")
	return nil
}

// printDLQ in các entry của DLQ: toàn bộ field, hoặc chỉ field error.
func printDLQ(ctx context.Context, q *lab.Queue, onlyError bool) error {
	entries, err := q.DLQEntries(ctx)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if onlyError {
			fmt.Println("DLQ entry:", e.Fields["error"])
			continue
		}
		pretty, _ := json.MarshalIndent(e.Fields, "", "  ")
		fmt.Println("DLQ entry:", string(pretty))
	}
	return nil
}
