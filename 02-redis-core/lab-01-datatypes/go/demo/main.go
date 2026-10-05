// Command demo walks through the five core Redis data types and the leaderboard.
package main

import (
	"context"
	"fmt"
	"log"
	"os"

	lab "github.com/nv-minh/mq-redis-handbook/02-redis-core/lab-01-datatypes/go"
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

	prefix := testkit.UniqueName("demo:datatypes")
	keys := []string{prefix + ":string", prefix + ":hash", prefix + ":list", prefix + ":set", prefix + ":board", prefix + ":empty"}
	defer func() { rdb.Del(ctx, keys...) }()

	fmt.Println("== string: counter with INCR ==")
	rdb.Set(ctx, keys[0], "40", 0)
	rdb.Incr(ctx, keys[0])
	rdb.IncrBy(ctx, keys[0], 1)
	fmt.Println("GET ->", rdb.Get(ctx, keys[0]).Val())

	fmt.Println("== hash: one object, several fields ==")
	rdb.HSet(ctx, keys[1], "name", "alice", "plan", "pro")
	rdb.HIncrBy(ctx, keys[1], "logins", 3)
	fmt.Println("HGETALL ->", rdb.HGetAll(ctx, keys[1]).Val())

	fmt.Println("== list: ordered, push at the head, pop at the tail ==")
	rdb.LPush(ctx, keys[2], "c", "b", "a")
	fmt.Println("LRANGE ->", rdb.LRange(ctx, keys[2], 0, -1).Val())
	fmt.Println("RPOP ->", rdb.RPop(ctx, keys[2]).Val())

	fmt.Println("== set: unique, unordered ==")
	rdb.SAdd(ctx, keys[3], "x", "y", "x")
	fmt.Println("SCARD ->", rdb.SCard(ctx, keys[3]).Val(), "(x was added twice)")
	fmt.Println("SISMEMBER y ->", rdb.SIsMember(ctx, keys[3], "y").Val())

	fmt.Println("== sorted set: leaderboard ==")
	board := lab.NewLeaderboard(rdb, keys[4])
	for _, p := range []struct {
		name  string
		score float64
	}{{"alice", 50}, {"bob", 90}, {"carol", 70}, {"dave", 10}, {"erin", 80}} {
		if err := board.Add(ctx, p.name, p.score); err != nil {
			return err
		}
	}
	top, err := board.Top(ctx, 3)
	if err != nil {
		return err
	}
	fmt.Println("Top(3) ->", top)
	withScores, err := board.TopWithScores(ctx, 2)
	if err != nil {
		return err
	}
	fmt.Println("TopWithScores(2) ->", withScores)
	raw, err := rdb.Do(ctx, "ZRANGE", keys[4], 0, 1, "REV", "WITHSCORES").Result()
	if err != nil {
		return err
	}
	fmt.Printf("raw WITHSCORES reply (RESP3, go-redis 9) -> %v\n", raw)
	empty, err := lab.NewLeaderboard(rdb, keys[5]).Top(ctx, 3)
	if err != nil {
		return err
	}
	fmt.Println("Top(3) on an empty board ->", empty)
	return nil
}
