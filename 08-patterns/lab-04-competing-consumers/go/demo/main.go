// Command demo cho thấy competing consumers: 4 worker cùng một consumer group chia nhau 20 message
// (mỗi message đúng một worker), rồi một consumer "chết" giữa chừng bỏ lại một message trong PEL
// và một worker còn sống claim nó. Cách chia việc giữa các worker thay đổi mỗi lần chạy.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"maps"
	"os"
	"slices"
	"sync"
	"time"

	lab "github.com/nv-minh/mq-redis-handbook/08-patterns/lab-04-competing-consumers/go"
	"github.com/nv-minh/mq-redis-handbook/internal/testkit"
	"github.com/redis/go-redis/v9"
)

const (
	groupName = "workers"
	minIdle   = 300 * time.Millisecond
)

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
		time.Sleep(20 * time.Millisecond)
	}
	return errors.New("hết thời gian chờ")
}

func run() (err error) {
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
	defer func() { _ = commands.Close() }()
	stream := testkit.UniqueName("demo:jobs")
	defer commands.Del(ctx, stream)
	g := lab.NewWorkerGroup(lab.Config{
		Commands: commands, Options: opts, Stream: stream, Group: groupName,
		MinIdle: minIdle, BlockTime: 100 * time.Millisecond,
	})
	if err := g.EnsureGroup(ctx); err != nil {
		return err
	}
	pendingEmpty := func() (bool, error) {
		entries, err := g.PendingEntries(ctx)
		return len(entries) == 0, err
	}

	fmt.Println("== 1. Bốn worker chia nhau 20 message ==")
	const total = 20
	for i := 0; i < total; i++ {
		if _, err := g.Publish(ctx, map[string]string{"job": fmt.Sprintf("job-%d", i)}); err != nil {
			return err
		}
	}
	var mu sync.Mutex
	perWorker := map[string]int{}
	processed := 0
	handle, err := g.StartWorkers(4, func(_ context.Context, d lab.Delivery) error {
		mu.Lock()
		defer mu.Unlock()
		perWorker[d.Consumer]++
		processed++
		return nil
	})
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, handle.Stop()) }()
	if err := waitFor(20*time.Second, func() (bool, error) {
		mu.Lock()
		done := processed >= total
		mu.Unlock()
		if !done {
			return false, nil
		}
		return pendingEmpty()
	}); err != nil {
		return err
	}
	if err := handle.Stop(); err != nil {
		return err
	}
	fmt.Printf("đã xử lý %d/%d message, PEL rỗng\n", processed, total)
	for _, name := range slices.Sorted(maps.Keys(perWorker)) {
		fmt.Printf("  %s: %d message\n", name, perWorker[name])
	}

	fmt.Println("\n== 2. Một consumer chết giữa chừng, worker còn sống tiếp quản ==")
	lostID, err := g.Publish(ctx, map[string]string{"job": "job-mồ-côi"})
	if err != nil {
		return err
	}
	// Mô phỏng consumer chết: đọc bằng XREADGROUP rồi bỏ đó, không xử lý, không XACK.
	if err := commands.Do(ctx, "XREADGROUP", "GROUP", groupName, "consumer-dead", "STREAMS", stream, ">").Err(); err != nil {
		return err
	}
	entries, err := g.PendingEntries(ctx)
	if err != nil {
		return err
	}
	fmt.Printf("PEL sau khi consumer chết: %+v\n", entries)
	if err := waitFor(10*time.Second, func() (bool, error) {
		entries, err := g.PendingEntries(ctx)
		return len(entries) == 1 && entries[0].Idle >= 2*minIdle, err
	}); err != nil {
		return err
	}
	var rescued []lab.Delivery
	rescuers, err := g.StartWorkers(2, func(_ context.Context, d lab.Delivery) error {
		mu.Lock()
		defer mu.Unlock()
		rescued = append(rescued, d)
		return nil
	})
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, rescuers.Stop()) }()
	if err := waitFor(20*time.Second, func() (bool, error) {
		mu.Lock()
		done := len(rescued) >= 1
		mu.Unlock()
		if !done {
			return false, nil
		}
		return pendingEmpty()
	}); err != nil {
		return err
	}
	if err := rescuers.Stop(); err != nil {
		return err
	}
	fmt.Printf("%s đã claim %s (deliveryCount %d), xử lý và ack. PEL rỗng.\n", rescued[0].Consumer, lostID, rescued[0].DeliveryCount)
	fmt.Println("Giao hàng là at-least-once: message có thể được xử lý lại, nên handler phải idempotent.")
	return nil
}
