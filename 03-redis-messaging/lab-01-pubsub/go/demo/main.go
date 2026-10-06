// Command demo shows that Pub/Sub keeps no history and fans out to every subscriber.
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

// waitFor polls until cond is true or five seconds pass.
func waitFor(cond func() bool) error {
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			return fmt.Errorf("condition not met within 5s")
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

	fmt.Println("== offline: nobody is subscribed ==")
	for _, message := range []string{"offline-1", "offline-2", "offline-3"} {
		receivers, err := lab.Publish(ctx, rdb, channel, message)
		if err != nil {
			return err
		}
		fmt.Printf("PUBLISH %s -> receivers = %d\n", message, receivers)
	}

	fmt.Println("== a late subscriber joins ==")
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
	fmt.Printf("PUBLISH online-1 -> receivers = %d\n", receivers)
	if err := waitFor(func() bool { return len(late.Messages()) >= 1 }); err != nil {
		return err
	}
	fmt.Println("late subscriber received:", strings.Join(late.Messages(), " "), "(the 3 offline messages are gone)")

	fmt.Println("== fan-out: 2 more subscribers, 5 messages ==")
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
			fmt.Printf("each PUBLISH reaches receivers = %d\n", receivers)
		}
	}
	for i, sub := range subs {
		want := 5
		if i == 0 {
			want = 6 // the late subscriber also holds "online-1"
		}
		if err := waitFor(func() bool { return len(sub.Messages()) >= want }); err != nil {
			return err
		}
		fmt.Printf("subscriber %d received: %s\n", i+1, strings.Join(sub.Messages(), " "))
	}
	return nil
}
