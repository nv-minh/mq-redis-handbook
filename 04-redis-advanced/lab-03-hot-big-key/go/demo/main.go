// Command demo creates one 1 MiB value and many tiny keys under a unique prefix, finds the big
// one with SCAN + MEMORY USAGE, then frees it with UNLINK. Only keys under the demo prefix are touched.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	lab "github.com/nv-minh/mq-redis-handbook/04-redis-advanced/lab-03-hot-big-key/go"
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
	rdb := redis.NewClient(opts)
	defer func() { _ = rdb.Close() }()

	prefix := testkit.UniqueName("demo:bigkey")
	var created []string
	// UNLINK frees the memory in a background thread. DEL on a big collection would block Redis.
	defer func() {
		n, _ := rdb.Unlink(ctx, created...).Result()
		fmt.Println("UNLINK removed:", n)
	}()

	pipe := rdb.Pipeline()
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("%s:small:%d", prefix, i)
		created = append(created, key)
		pipe.Set(ctx, key, "x", 0)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return err
	}
	bigKey := prefix + ":big"
	created = append(created, bigKey)
	if err := rdb.Set(ctx, bigKey, strings.Repeat("x", 1024*1024), 0).Err(); err != nil {
		return err
	}

	small, _ := rdb.MemoryUsage(ctx, prefix+":small:0").Result()
	big, _ := rdb.MemoryUsage(ctx, bigKey).Result()
	fmt.Printf("MEMORY USAGE small key: %d bytes, big key: %d bytes\n", small, big)

	started := time.Now()
	found, err := lab.FindBigKeys(ctx, rdb, 512*1024, prefix+":*")
	if err != nil {
		return err
	}
	fmt.Printf("FindBigKeys(512 KiB, %q) -> %v in %d ms\n", prefix+":*", found, time.Since(started).Milliseconds())
	fmt.Println("scanned 1001 keys, returned", len(found))
	return nil
}
