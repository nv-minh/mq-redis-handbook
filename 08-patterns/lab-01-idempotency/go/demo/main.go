// Command demo cho thấy idempotent consumer: cùng một message được giao nhiều lần mà effect chỉ chạy một lần,
// effect lỗi thì key được nhả để retry, và cửa sổ "bản trùng đến trong lúc effect đầu còn chạy".
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	lab "github.com/nv-minh/mq-redis-handbook/08-patterns/lab-01-idempotency/go"
	"github.com/nv-minh/mq-redis-handbook/internal/testkit"
	"github.com/redis/go-redis/v9"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func exists(ctx context.Context, rdb *redis.Client, id string) bool {
	n, _ := rdb.Exists(ctx, lab.DedupKey(id)).Result()
	return n == 1
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

	var ids []string
	newID := func() string {
		id := testkit.UniqueName("demo-msg")
		ids = append(ids, id)
		return id
	}
	defer func() {
		for _, id := range ids {
			rdb.Del(ctx, lab.DedupKey(id))
		}
	}()

	fmt.Println("== 1. Giao trùng: broker giao cùng một message 3 lần (at-least-once) ==")
	id1 := newID()
	charged := 0
	charge := func(context.Context) error { charged++; return nil }
	for i := 1; i <= 3; i++ {
		got, err := lab.ProcessOnce(ctx, rdb, id1, charge, 60)
		if err != nil {
			return err
		}
		fmt.Printf("lần giao %d: %s\n", i, got)
	}
	ttl, _ := rdb.TTL(ctx, lab.DedupKey(id1)).Result()
	fmt.Printf("effect (trừ tiền) đã chạy %d lần, key còn %.0fs\n", charged, ttl.Seconds())

	fmt.Println("\n== 2. Effect lỗi: key được nhả để lần giao lại còn xử lý được ==")
	id2 := newID()
	_, err = lab.ProcessOnce(ctx, rdb, id2, func(context.Context) error { return errors.New("downstream timeout") }, 60)
	fmt.Printf("lần 1: effect lỗi (%v)\n", err)
	fmt.Printf("key còn tồn tại sau lỗi: %v\n", exists(ctx, rdb, id2))
	got, err := lab.ProcessOnce(ctx, rdb, id2, charge, 60)
	if err != nil {
		return err
	}
	fmt.Printf("lần 2 (redelivery): %s\n", got)

	fmt.Println("\n== 3. Cửa sổ đã biết: bản trùng đến khi effect đầu còn chạy ==")
	id3 := newID()
	release := make(chan struct{})
	firstDone := make(chan error, 1)
	go func() {
		_, err := lab.ProcessOnce(ctx, rdb, id3, func(context.Context) error {
			<-release
			return errors.New("effect đầu lỗi")
		}, 60)
		firstDone <- err
	}()
	for !exists(ctx, rdb, id3) {
		time.Sleep(5 * time.Millisecond)
	}
	second, err := lab.ProcessOnce(ctx, rdb, id3, charge, 60)
	if err != nil {
		close(release)
		return err
	}
	fmt.Printf("bản trùng đến lúc effect đầu còn chạy: %s (consumer sẽ ack nó)\n", second)
	close(release)
	fmt.Printf("effect đầu kết thúc: %v\n", <-firstDone)
	fmt.Printf("key còn tồn tại: %v, nhưng bản trùng đã ack nên effect mất.\n", exists(ctx, rdb, id3))
	fmt.Println("Cách giảm thiểu: trạng thái processing (TTL ngắn) và done (TTL dài), xem README.")
	return nil
}
