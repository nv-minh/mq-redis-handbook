// Command demo shows hash slots and hash tags on the Docker cluster and the CROSSSLOT error.
// Needs `make up PROFILE="sentinel cluster"`. Keys are removed at the end.
package main

import (
	"context"
	"fmt"
	"log"

	lab "github.com/nv-minh/mq-redis-handbook/04-redis-advanced/lab-02-cluster/go"
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
	cluster := lab.ConnectCluster()
	node := redis.NewClient(&redis.Options{Addr: "127.0.0.1:7001", Protocol: 2})
	defer func() { _ = node.Close() }()
	var created []string
	defer func() {
		for _, key := range created {
			cluster.Del(ctx, key)
		}
		_ = cluster.Close()
	}()

	fmt.Println("--- hash tag rules (SlotFor must equal CLUSTER KEYSLOT) ---")
	for _, key := range []string{"foo", "{user1000}.following", "{user1000}.followers", "foo{}{bar}", "foo{{bar}}zap", "foo{bar}{zap}", "日本語"} {
		slot := lab.SlotFor(key)
		keyslot, err := node.Do(ctx, "CLUSTER", "KEYSLOT", key).Int()
		if err != nil {
			return err
		}
		owner, err := lab.SlotOwner(ctx, slot)
		if err != nil {
			return err
		}
		fmt.Printf("%-24q SlotFor=%-5d CLUSTER KEYSLOT=%-5d owner=%s -> %s\n", key, slot, keyslot, owner, lab.HostAddr(owner))
	}

	fmt.Println("--- MSET across slots on ONE node ---")
	prefix := testkit.UniqueName("demo:cross")
	a, b := prefix+"-a", prefix+"-b"
	for i := 0; lab.SlotFor(a) == lab.SlotFor(b); i++ {
		b = fmt.Sprintf("%s-b%d", prefix, i)
	}
	created = append(created, a, b)
	fmt.Printf("MSET %s %s (slots %d and %d) -> %v\n", a, b, lab.SlotFor(a), lab.SlotFor(b), node.MSet(ctx, a, "1", b, "2").Err())

	fmt.Println("--- same keys with a shared hash tag, through the cluster client ---")
	tag := testkit.UniqueName("demo:tag")
	tagged := []string{"{" + tag + "}:pending", "{" + tag + "}:processing"}
	created = append(created, tagged...)
	fmt.Printf("MSET %v -> %v\n", tagged, cluster.MSet(ctx, tagged[0], "1", tagged[1], "2").Err())
	got, err := cluster.MGet(ctx, tagged...).Result()
	fmt.Println("MGET ->", got, err)
	return nil
}
