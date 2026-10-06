// Command demo shows consumer A crashing without XACK, consumer B taking the entries over with
// XAUTOCLAIM, and the raw reply shapes go-redis gets on RESP3.
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
	fmt.Printf("consumer-a read: %+v\n", read)
	if err := showPending("after read, nothing acked"); err != nil {
		return err
	}

	_ = blockingA.Close()
	fmt.Println("consumer-a crashes without XACK")
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
			return fmt.Errorf("entry never became idle")
		}
		time.Sleep(50 * time.Millisecond)
	}

	claimed, err := consumerB.ClaimStale(ctx, group, "consumer-b", minIdle)
	if err != nil {
		return err
	}
	fmt.Printf("consumer-b claimed: %+v\n", claimed)
	if err := showPending("after XAUTOCLAIM (owner is consumer-b, deliveries 2)"); err != nil {
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
	fmt.Println("pending count after acks:", n)

	rest, err := consumerB.Consume(ctx, group, "consumer-b", 10)
	if err != nil {
		return err
	}
	fmt.Printf("consumer-b also reads the never delivered entry: %+v\n", rest)

	fmt.Println("== raw reply shapes (go-redis v9.23.0, RESP3) ==")
	pending, err := commands.Do(ctx, "XPENDING", stream, group).Result()
	if err != nil {
		return err
	}
	fmt.Printf("XPENDING summary: %#v\n", pending)
	autoclaim, err := commands.Do(ctx, "XAUTOCLAIM", stream, group, "consumer-c", 0, "0-0").Result()
	if err != nil {
		return err
	}
	fmt.Printf("XAUTOCLAIM (3 elements): %#v\n", autoclaim)
	raw, err := commands.Do(ctx, "XREADGROUP", "GROUP", group, "consumer-c", "COUNT", 1, "STREAMS", stream, "0").Result()
	if err != nil {
		return err
	}
	fmt.Printf("XREADGROUP re-read of own pending (raw, a map on RESP3): %#v\n", raw)
	return nil
}
