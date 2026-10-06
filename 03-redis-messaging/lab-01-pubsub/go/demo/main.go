// Command demo cho thấy Pub/Sub không giữ lịch sử và fan-out tới mọi subscriber.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	lab "github.com/nv-minh/mq-redis-handbook/03-redis-messaging/lab-01-pubsub/go"
	"github.com/nv-minh/mq-redis-handbook/internal/testkit"
	"github.com/redis/go-redis/v9"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

// waitFor poll tới khi cond đúng hoặc hết năm giây.
func waitFor(cond func() bool) error {
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			return fmt.Errorf("điều kiện chưa đạt sau 5s")
		}
		time.Sleep(50 * time.Millisecond)
	}
	return nil
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
	rdb := redis.NewClient(opts)
	defer func() { _ = rdb.Close() }()
	channel := testkit.UniqueName("demo:pubsub")

	fmt.Println("== offline: chưa ai subscribe ==")
	for _, message := range []string{"offline-1", "offline-2", "offline-3"} {
		receivers, err := lab.Publish(ctx, rdb, channel, message)
		if err != nil {
			return err
		}
		fmt.Printf("PUBLISH %s -> số receiver = %d\n", message, receivers)
	}

	fmt.Println("== một subscriber vào muộn ==")
	late, err := lab.Subscribe(ctx, rdb, channel)
	if err != nil {
		return err
	}
	defer func() { _ = late.Close() }()
	n, err := lab.NumSubscribers(ctx, rdb, channel)
	if err != nil {
		return err
	}
	fmt.Println("PUBSUB NUMSUB =", n)
	receivers, err := lab.Publish(ctx, rdb, channel, "online-1")
	if err != nil {
		return err
	}
	fmt.Printf("PUBLISH online-1 -> số receiver = %d\n", receivers)
	if err := waitFor(func() bool { return len(late.Messages()) >= 1 }); err != nil {
		return err
	}
	fmt.Println("subscriber vào muộn đã nhận:", strings.Join(late.Messages(), " "), "(3 message offline đã mất)")

	fmt.Println("== fan-out: thêm 2 subscriber, 5 message ==")
	subs := []*lab.Subscription{late}
	for i := 0; i < 2; i++ {
		sub, err := lab.Subscribe(ctx, rdb, channel)
		if err != nil {
			return err
		}
		defer func() { _ = sub.Close() }()
		subs = append(subs, sub)
	}
	for i := 0; i < 5; i++ {
		receivers, err := lab.Publish(ctx, rdb, channel, fmt.Sprintf("fanout-%d", i))
		if err != nil {
			return err
		}
		if i == 0 {
			fmt.Printf("mỗi PUBLISH tới số receiver = %d\n", receivers)
		}
	}
	for i, sub := range subs {
		want := 5
		if i == 0 {
			want = 6 // subscriber vào muộn còn giữ "online-1"
		}
		if err := waitFor(func() bool { return len(sub.Messages()) >= want }); err != nil {
			return err
		}
		fmt.Printf("subscriber %d đã nhận: %s\n", i+1, strings.Join(sub.Messages(), " "))
	}
	return nil
}
