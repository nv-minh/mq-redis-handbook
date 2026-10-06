// Command demo cho thấy hash slot và hash tag trên cluster Docker, và lỗi CROSSSLOT.
// Cần `make up PROFILE="sentinel cluster"`. Các key tạo ra được xóa ở cuối.
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

	fmt.Println("Demo Redis Cluster: hash slot, hash tag và CROSSSLOT.")
	fmt.Println("--- Quy tắc hash tag (SlotFor tự cài phải bằng CLUSTER KEYSLOT của server) ---")
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
		fmt.Printf("%-24q SlotFor=%-5d CLUSTER KEYSLOT=%-5d master=%s -> %s\n", key, slot, keyslot, owner, lab.HostAddr(owner))
	}

	fmt.Println("Nhận xét: hai key user1000 cùng slot nhờ hash tag, còn tag rỗng `{}` thì băm cả key.")
	fmt.Println("--- MSET hai key khác slot gửi tới MỘT node ---")
	prefix := testkit.UniqueName("demo:cross")
	a, b := prefix+"-a", prefix+"-b"
	for i := 0; lab.SlotFor(a) == lab.SlotFor(b); i++ {
		b = fmt.Sprintf("%s-b%d", prefix, i)
	}
	created = append(created, a, b)
	fmt.Printf("MSET %s %s (slot %d và %d) -> %v\n", a, b, lab.SlotFor(a), lab.SlotFor(b), node.MSet(ctx, a, "1", b, "2").Err())
	fmt.Println("Nhận xét: server từ chối vì các key không cùng slot.")

	fmt.Println("--- Cùng kiểu key nhưng chung hash tag, qua cluster client ---")
	tag := testkit.UniqueName("demo:tag")
	tagged := []string{"{" + tag + "}:pending", "{" + tag + "}:processing"}
	created = append(created, tagged...)
	fmt.Printf("MSET %v -> %v\n", tagged, cluster.MSet(ctx, tagged[0], "1", tagged[1], "2").Err())
	got, err := cluster.MGet(ctx, tagged...).Result()
	fmt.Println("MGET ->", got, err)
	fmt.Println("Nhận xét: chung hash tag nên cùng slot, lệnh multi-key chạy được (cách làm cho reliable queue BLMOVE).")
	return nil
}
