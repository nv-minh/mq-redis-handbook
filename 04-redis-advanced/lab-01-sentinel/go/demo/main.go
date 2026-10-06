// Command demo writes through Sentinel, stops the master, measures how long writes fail and
// restores the node. Needs `make up PROFILE="sentinel cluster"`.
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
	return fmt.Errorf("topology not healthy after 90s")
}

func run() error {
	ctx := context.Background()
	client := lab.ConnectViaSentinel()
	defer func() { _ = client.Close() }()
	key := testkit.UniqueName("demo:sentinel")

	if err := show(ctx, "start"); err != nil {
		return err
	}
	fmt.Println("SET through Sentinel:", client.Set(ctx, key, "before", 0).Err())

	master, err := lab.CurrentMaster(ctx)
	if err != nil {
		return err
	}
	service := lab.ServiceOf(master)
	fmt.Printf("stopping %s (the current master)\n", service)
	if err := lab.StopService(ctx, service); err != nil {
		return err
	}
	defer func() {
		fmt.Printf("restarting %s\n", service)
		if err := lab.StartService(ctx, service); err != nil {
			log.Print(err)
			return
		}
		if err := waitHealthy(ctx); err != nil {
			log.Print(err)
		}
		_ = show(ctx, "end")
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
	fmt.Printf("first successful write %d ms after the stop (%d failed attempts before it)\n", time.Since(stoppedAt).Milliseconds(), failed)
	newMaster, err := lab.CurrentMaster(ctx)
	if err != nil {
		return err
	}
	fmt.Println("new master according to Sentinel:", newMaster)
	got, _ := client.Get(ctx, key).Result()
	fmt.Println("GET after failover:", got)
	return nil
}
