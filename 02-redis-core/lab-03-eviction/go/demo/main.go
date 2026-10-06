// Command demo shows allkeys-lru evicting cold keys (and keeping the hot one) and noeviction
// rejecting writes with OOM. It CHANGES maxmemory settings of the server and restores them.
// Only run it against the Redis of this handbook's compose stack (make up).
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	lab "github.com/nv-minh/mq-redis-handbook/02-redis-core/lab-03-eviction/go"
	"github.com/nv-minh/mq-redis-handbook/internal/testkit"
	"github.com/redis/go-redis/v9"
)

const (
	headroom = 256 * 1024
	coldKeys = 1500
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
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
	rdb := redis.NewClient(opts)
	defer func() { _ = rdb.Close() }()

	prefix := testkit.UniqueName("demo:eviction")
	original, err := lab.ReadConfig(ctx, rdb)
	if err != nil {
		return err
	}
	defer func() {
		if rerr := lab.WriteConfig(ctx, rdb, original); rerr != nil && err == nil {
			err = rerr
		}
		if derr := lab.DeleteByPrefix(ctx, rdb, prefix); derr != nil && err == nil {
			err = derr
		}
		now, _ := lab.ReadConfig(ctx, rdb)
		fmt.Printf("restored config: %+v\n", now)
	}()
	fmt.Printf("saved config: %+v\n", original)

	fmt.Println("== allkeys-lru: write past the limit while touching one hot key ==")
	if err := lab.SeedKeys(ctx, rdb, prefix+":cold", coldKeys, 1024); err != nil {
		return err
	}
	hot := prefix + ":hot"
	if err := rdb.Set(ctx, hot, "hot", 0).Err(); err != nil {
		return err
	}
	for { // wait until the cold keys look old (LRU idle time has a 1 second resolution)
		idle, err := rdb.ObjectIdleTime(ctx, prefix+":cold:0").Result()
		if err != nil {
			return err
		}
		if idle >= 2*time.Second {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	rdb.Get(ctx, hot)
	maxmemory, err := lab.LimitMemory(ctx, rdb, "allkeys-lru", headroom)
	if err != nil {
		return err
	}
	fmt.Printf("maxmemory set to %d bytes (used_memory + %d)\n", maxmemory, headroom)
	evicted, err := lab.FillUntilEviction(ctx, rdb, prefix+":fill", 20_000, lab.FillOptions{TouchKey: hot, MinEvicted: 900})
	if err != nil {
		return err
	}
	cold := make([]string, coldKeys)
	for i := range cold {
		cold[i] = fmt.Sprintf("%s:cold:%d", prefix, i)
	}
	surviving, _ := rdb.Exists(ctx, cold...).Result()
	hotAlive, _ := rdb.Exists(ctx, hot).Result()
	fmt.Printf("evicted_keys grew by %d\n", evicted)
	fmt.Printf("hot key survived: %v\n", hotAlive == 1)
	fmt.Printf("cold keys left: %d of %d\n", surviving, coldKeys)

	if err := lab.WriteConfig(ctx, rdb, original); err != nil {
		return err
	}
	if err := lab.DeleteByPrefix(ctx, rdb, prefix); err != nil {
		return err
	}

	fmt.Println("== noeviction: the same pressure rejects writes ==")
	before, _ := lab.EvictedKeys(ctx, rdb)
	if _, err := lab.LimitMemory(ctx, rdb, "noeviction", headroom); err != nil {
		return err
	}
	msg, err := lab.FillUntilRejected(ctx, rdb, prefix+":fill", 20_000, 1024)
	if err != nil {
		return err
	}
	fmt.Println("write error:", msg)
	after, _ := lab.EvictedKeys(ctx, rdb)
	fmt.Printf("evicted_keys grew by %d\n", after-before)
	return nil
}
