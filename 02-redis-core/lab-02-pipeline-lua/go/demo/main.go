// Command demo shows round trips of sequential vs pipelined commands, then naive vs Lua
// check-and-decrement under 50 concurrent callers.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"

	lab "github.com/nv-minh/mq-redis-handbook/02-redis-core/lab-02-pipeline-lua/go"
	"github.com/nv-minh/mq-redis-handbook/internal/testkit"
	"github.com/redis/go-redis/v9"
)

const (
	callers = 50
	units   = 10
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func race(ctx context.Context, rdb *redis.Client, key string, decrement func(context.Context, *redis.Client, string) (bool, error)) (int64, string, error) {
	if err := rdb.Set(ctx, key, units, 0).Err(); err != nil {
		return 0, "", err
	}
	var (
		wg        sync.WaitGroup
		successes atomic.Int64
		firstErr  atomic.Value
	)
	start := make(chan struct{})
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			ok, err := decrement(ctx, rdb, key)
			if err != nil {
				firstErr.CompareAndSwap(nil, err)
			}
			if ok {
				successes.Add(1)
			}
		}()
	}
	close(start)
	wg.Wait()
	if err, ok := firstErr.Load().(error); ok {
		return 0, "", err
	}
	final, err := rdb.Get(ctx, key).Result()
	return successes.Load(), final, err
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
	rdb, counter := lab.NewCountingClient(opts)
	defer func() { _ = rdb.Close() }()

	prefix := testkit.UniqueName("demo:pipeline")
	naiveKey, luaKey := prefix+":naive", prefix+":lua"
	defer func() {
		keys := []string{naiveKey, luaKey}
		for i := 0; i < 100; i++ {
			keys = append(keys, fmt.Sprintf("%s:seq:%d", prefix, i), fmt.Sprintf("%s:pipe:%d", prefix, i))
		}
		rdb.Del(ctx, keys...)
	}()

	fmt.Println("== round trips: 100 SET commands ==")
	// Warm up both connection pools so handshake writes are not counted.
	if err := lab.SetSequential(ctx, rdb, prefix+":seq", 1); err != nil {
		return err
	}
	if err := lab.SetPipelined(ctx, rdb, prefix+":pipe", 1); err != nil {
		return err
	}
	counter.Reset()
	if err := lab.SetSequential(ctx, rdb, prefix+":seq", 100); err != nil {
		return err
	}
	fmt.Println("sequential: socket writes =", counter.Writes())
	counter.Reset()
	if err := lab.SetPipelined(ctx, rdb, prefix+":pipe", 100); err != nil {
		return err
	}
	fmt.Println("pipelined:  socket writes =", counter.Writes())

	fmt.Printf("== %d concurrent callers share a counter that holds %d ==\n", callers, units)
	ok, final, err := race(ctx, rdb, naiveKey, lab.DecrIfPositiveNaive)
	if err != nil {
		return err
	}
	fmt.Printf("naive GET then DECR: %d callers succeeded, counter ended at %s\n", ok, final)
	ok, final, err = race(ctx, rdb, luaKey, lab.DecrIfPositive)
	if err != nil {
		return err
	}
	fmt.Printf("Lua script:          %d callers succeeded, counter ended at %s\n", ok, final)
	return nil
}
