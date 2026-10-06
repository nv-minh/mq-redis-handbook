// Command demo ghi dữ liệu qua Sentinel, dừng master, đo xem ghi lỗi trong bao lâu rồi khôi phục node.
// Cần `make up PROFILE="sentinel cluster"`. Node bị dừng sẽ được bật lại ở cuối.
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	lab "github.com/nv-minh/mq-redis-handbook/04-redis-advanced/lab-01-sentinel/go"
	"github.com/nv-minh/mq-redis-handbook/internal/testkit"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func show(ctx context.Context, label string) error {
	t, err := lab.ReadTopology(ctx)
	if err != nil {
		return err
	}
	fmt.Printf("%s: master=%s replicas=%v sentinels=%d\n", label, t.Master, t.Replicas, t.Sentinels)
	return nil
}

func waitHealthy(ctx context.Context) error {
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		if t, err := lab.ReadTopology(ctx); err == nil && t.Healthy() {
			return nil
		}
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("topology chưa khỏe lại sau 90s")
}

func run() error {
	ctx := context.Background()
	client := lab.ConnectViaSentinel()
	defer func() { _ = client.Close() }()
	key := testkit.UniqueName("demo:sentinel")

	fmt.Println("Demo Sentinel failover: client đi theo master mới sau khi master cũ bị dừng.")
	if err := show(ctx, "Bắt đầu (topology khỏe)"); err != nil {
		return err
	}
	fmt.Println("SET qua Sentinel:", client.Set(ctx, key, "before", 0).Err())

	master, err := lab.CurrentMaster(ctx)
	if err != nil {
		return err
	}
	service := lab.ServiceOf(master)
	fmt.Printf("Dừng %s (master hiện tại, tìm bằng SENTINEL get-master-addr-by-name)\n", service)
	if err := lab.StopService(ctx, service); err != nil {
		return err
	}
	defer func() {
		fmt.Printf("Bật lại %s (nó sẽ quay về làm replica của master mới)\n", service)
		if err := lab.StartService(ctx, service); err != nil {
			log.Print(err)
			return
		}
		if err := waitHealthy(ctx); err != nil {
			log.Print(err)
		}
		_ = show(ctx, "Kết thúc (topology khỏe lại: 1 master, 2 replica, 3 sentinel)")
		client.Del(ctx, key)
	}()

	stoppedAt := time.Now()
	failed := 0
	for time.Since(stoppedAt) < 60*time.Second {
		attempt, cancel := context.WithTimeout(ctx, 2*time.Second)
		err := client.Set(attempt, key, "after", 0).Err()
		cancel()
		if err == nil {
			break
		}
		failed++
		time.Sleep(100 * time.Millisecond)
	}
	fmt.Printf("Lần ghi thành công đầu tiên sau %d ms kể từ khi dừng master (%d lần ghi lỗi trước đó)\n", time.Since(stoppedAt).Milliseconds(), failed)
	newMaster, err := lab.CurrentMaster(ctx)
	if err != nil {
		return err
	}
	fmt.Println("Master mới theo Sentinel:", newMaster)
	got, _ := client.Get(ctx, key).Result()
	fmt.Println("GET sau failover:", got, "(dữ liệu ghi sau failover nằm trên master mới)")
	return nil
}
