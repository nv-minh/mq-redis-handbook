// Command demo shows a consumer crashing without ack, the recovery pass and a second consumer
// finishing the message.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	lab "github.com/nv-minh/mq-redis-handbook/03-redis-messaging/lab-02-list-queue/go"
	"github.com/nv-minh/mq-redis-handbook/internal/testkit"
	"github.com/redis/go-redis/v9"
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
	crashedBlocking := redis.NewClient(opts)
	survivorBlocking := redis.NewClient(opts)
	defer func() { _ = commands.Close() }()
	defer func() { _ = survivorBlocking.Close() }()

	queueKey := testkit.UniqueName("demo:list-queue")
	crashing := lab.NewReliableQueue(lab.Config{Commands: commands, Blocking: crashedBlocking, Queue: queueKey, BlockTimeout: time.Second})
	survivor := lab.NewReliableQueue(lab.Config{Commands: commands, Blocking: survivorBlocking, Queue: queueKey, BlockTimeout: time.Second})
	defer func() {
		commands.Del(ctx, queueKey, survivor.ProcessingKey("worker-a"), survivor.ProcessingKey("worker-b"))
	}()

	show := func(label string) error {
		queue, err := commands.LRange(ctx, queueKey, 0, -1).Result()
		if err != nil {
			return err
		}
		a, err := commands.LRange(ctx, survivor.ProcessingKey("worker-a"), 0, -1).Result()
		if err != nil {
			return err
		}
		b, err := commands.LRange(ctx, survivor.ProcessingKey("worker-b"), 0, -1).Result()
		if err != nil {
			return err
		}
		fmt.Printf("%s: queue=%q worker-a=%q worker-b=%q\n", label, queue, a, b)
		return nil
	}

	for _, msg := range []string{"job-1", "job-2"} {
		if err := survivor.Enqueue(ctx, msg); err != nil {
			return err
		}
	}
	if err := show("after enqueue"); err != nil {
		return err
	}

	msg, _, err := crashing.Dequeue(ctx, "worker-a")
	if err != nil {
		return err
	}
	fmt.Println("worker-a dequeues:", msg)
	if err := show("worker-a holds " + msg + " (no ack yet)"); err != nil {
		return err
	}

	_ = crashedBlocking.Close()
	fmt.Println("worker-a crashes without ack")
	moved, err := survivor.RecoverStale(ctx, "worker-a")
	if err != nil {
		return err
	}
	fmt.Println("RecoverStale(worker-a) moved:", moved)
	if err := show("after recovery"); err != nil {
		return err
	}

	started := time.Now()
	for {
		msg, ok, err := survivor.Dequeue(ctx, "worker-b")
		if err != nil {
			return err
		}
		if !ok {
			fmt.Printf("worker-b dequeue on an empty queue: none after blocking %d ms in total\n", time.Since(started).Milliseconds())
			break
		}
		acked, err := survivor.Ack(ctx, "worker-b", msg)
		if err != nil {
			return err
		}
		fmt.Printf("worker-b dequeues %s, acks: %v\n", msg, acked)
	}
	return show("end")
}
