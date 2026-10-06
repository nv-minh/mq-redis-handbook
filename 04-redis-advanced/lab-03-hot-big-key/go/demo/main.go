// Command demo tạo một value 1 MiB và nhiều key nhỏ dưới một prefix duy nhất, tìm key lớn bằng
// SCAN + MEMORY USAGE, rồi giải phóng bằng UNLINK. Demo chỉ đụng tới các key dưới prefix của nó.
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

	fmt.Println("Demo tìm big key: 1000 key nhỏ và một chuỗi 1 MiB dưới cùng một prefix.")
	prefix := testkit.UniqueName("demo:bigkey")
	var created []string
	// UNLINK giải phóng bộ nhớ ở thread nền, còn DEL trên collection lớn sẽ chặn Redis.
	defer func() {
		n, _ := rdb.Unlink(ctx, created...).Result()
		fmt.Println("UNLINK đã xóa:", n, "key")
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
	fmt.Printf("MEMORY USAGE của key nhỏ: %d byte, của key lớn: %d byte (allocator làm tròn lên so với 1 MiB dữ liệu)\n", small, big)

	started := time.Now()
	found, err := lab.FindBigKeys(ctx, rdb, 512*1024, prefix+":*")
	if err != nil {
		return err
	}
	fmt.Printf("FindBigKeys(512 KiB, %q) -> %v trong %d ms\n", prefix+":*", found, time.Since(started).Milliseconds())
	fmt.Println("Đã quét 1001 key, trả về", len(found), "key lớn: ngưỡng quyết định key nào được coi là big.")
	return nil
}
