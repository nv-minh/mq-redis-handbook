// Command demo cho thấy consumer A crash mà không XACK, consumer B tiếp quản các entry bằng
// XAUTOCLAIM, và dạng reply thô mà go-redis nhận được trên RESP3.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	lab "github.com/nv-minh/mq-redis-handbook/03-redis-messaging/lab-03-streams/go"
	"github.com/nv-minh/mq-redis-handbook/internal/testkit"
	"github.com/redis/go-redis/v9"
)

const (
	group    = "workers"
	minIdle  = 300 * time.Millisecond
	blockFor = 500 * time.Millisecond
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
	blockingA := redis.NewClient(opts)
	blockingB := redis.NewClient(opts)
	defer func() { _ = commands.Close() }()
	defer func() { _ = blockingB.Close() }()

	stream := testkit.UniqueName("demo:stream")
	defer commands.Del(ctx, stream)
	consumerA := lab.NewStreamQueue(lab.Config{Commands: commands, Blocking: blockingA, Stream: stream, BlockTime: blockFor})
	consumerB := lab.NewStreamQueue(lab.Config{Commands: commands, Blocking: blockingB, Stream: stream, BlockTime: blockFor})

	showPending := func(label string) error {
		entries, err := consumerB.PendingEntries(ctx, group)
		if err != nil {
			return err
		}
		fmt.Printf("%s: PEL = ", label)
		for _, e := range entries {
			fmt.Printf("{id:%s consumer:%s idle:%dms deliveries:%d} ", e.ID, e.Consumer, e.Idle.Milliseconds(), e.DeliveryCount)
		}
		fmt.Println()
		return nil
	}

	if err := consumerA.CreateGroup(ctx, group); err != nil {
		return err
	}
	for _, job := range []string{"job-1", "job-2", "job-3"} {
		if _, err := consumerA.Publish(ctx, map[string]string{"job": job}); err != nil {
			return err
		}
	}

	read, err := consumerA.Consume(ctx, group, "consumer-a", 2)
	if err != nil {
		return err
	}
	fmt.Printf("consumer-a đã đọc: %+v\n", read)
	if err := showPending("sau khi đọc, chưa ack gì"); err != nil {
		return err
	}

	_ = blockingA.Close()
	fmt.Println("consumer-a crash mà không XACK")
	deadline := time.Now().Add(10 * time.Second)
	for {
		entries, err := consumerB.PendingEntries(ctx, group)
		if err != nil {
			return err
		}
		if len(entries) > 0 && entries[0].Idle >= 2*minIdle {
			break
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("entry chưa bao giờ đạt idle yêu cầu")
		}
		time.Sleep(50 * time.Millisecond)
	}

	claimed, err := consumerB.ClaimStale(ctx, group, "consumer-b", minIdle)
	if err != nil {
		return err
	}
	fmt.Printf("consumer-b đã claim: %+v\n", claimed)
	if err := showPending("sau XAUTOCLAIM (chủ sở hữu là consumer-b, deliveries 2)"); err != nil {
		return err
	}

	for _, m := range claimed.Messages {
		if _, err := consumerB.Ack(ctx, group, m.ID); err != nil {
			return err
		}
	}
	n, err := consumerB.PendingCount(ctx, group)
	if err != nil {
		return err
	}
	fmt.Println("số pending sau khi ack:", n)

	rest, err := consumerB.Consume(ctx, group, "consumer-b", 10)
	if err != nil {
		return err
	}
	fmt.Printf("consumer-b còn đọc entry chưa từng được giao: %+v\n", rest)

	fmt.Println("== dạng reply thô (go-redis v9.23.0, RESP3) ==")
	pending, err := commands.Do(ctx, "XPENDING", stream, group).Result()
	if err != nil {
		return err
	}
	fmt.Printf("XPENDING dạng summary: %#v\n", pending)
	autoclaim, err := commands.Do(ctx, "XAUTOCLAIM", stream, group, "consumer-c", 0, "0-0").Result()
	if err != nil {
		return err
	}
	fmt.Printf("XAUTOCLAIM (3 phần tử): %#v\n", autoclaim)
	raw, err := commands.Do(ctx, "XREADGROUP", "GROUP", group, "consumer-c", "COUNT", 1, "STREAMS", stream, "0").Result()
	if err != nil {
		return err
	}
	fmt.Printf("XREADGROUP đọc lại pending của chính nó (thô, là một map trên RESP3): %#v\n", raw)
	return nil
}
