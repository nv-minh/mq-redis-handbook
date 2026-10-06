package lab

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/nv-minh/mq-redis-handbook/internal/testkit"
	"github.com/redis/go-redis/v9"
)

const hint = `Redis Sentinel is not up. Start both topologies with: make up PROFILE="sentinel cluster"`

// requireSentinelProfile fails fast with a clear message when the sentinel profile is not running.
func requireSentinelProfile(t *testing.T) {
	t.Helper()
	// The hint is printed only when the test fails, next to WaitForPort's error.
	t.Cleanup(func() {
		if t.Failed() {
			t.Log(hint)
		}
	})
	for _, addr := range []string{"127.0.0.1:26379", "127.0.0.1:26380", "127.0.0.1:26381", "127.0.0.1:6380", "127.0.0.1:6381", "127.0.0.1:6382"} {
		testkit.WaitForPort(t, addr, 3*time.Second)
	}
}

func waitForHealthyTopology(t *testing.T, timeout time.Duration) {
	t.Helper()
	testkit.Eventually(t, timeout, func() (struct{}, bool) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		topology, err := ReadTopology(ctx)
		return struct{}{}, err == nil && topology.Healthy()
	})
}

func TestClientWritesThroughTheMasterThatSentinelReports(t *testing.T) {
	requireSentinelProfile(t)
	ctx := context.Background()
	client := ConnectViaSentinel()
	defer func() { _ = client.Close() }()
	key := testkit.UniqueName("lab04-sentinel")
	defer client.Del(ctx, key)

	if err := client.Set(ctx, key, "hello", 0).Err(); err != nil {
		t.Fatalf("set: %v", err)
	}
	if got, _ := client.Get(ctx, key).Result(); got != "hello" {
		t.Fatalf("get = %q, want hello", got)
	}

	master, err := CurrentMaster(ctx)
	if err != nil {
		t.Fatalf("current master: %v", err)
	}
	direct := redis.NewClient(&redis.Options{Addr: HostAddr(master)})
	defer func() { _ = direct.Close() }()
	if got, _ := direct.Get(ctx, key).Result(); got != "hello" {
		t.Fatalf("value on the master Sentinel reports = %q, want hello", got)
	}
}

func TestSentinelSeesOneMasterAndTwoReplicas(t *testing.T) {
	requireSentinelProfile(t)
	waitForHealthyTopology(t, 90*time.Second)
	topology, err := ReadTopology(context.Background())
	if err != nil {
		t.Fatalf("topology: %v", err)
	}
	if len(topology.Replicas) != 2 || topology.Sentinels != 3 {
		t.Fatalf("topology = %+v, want 2 replicas and 3 sentinels", topology)
	}
	seen := map[string]bool{topology.Master: true}
	for _, r := range topology.Replicas {
		seen[r] = true
	}
	if len(seen) != 3 {
		t.Fatalf("expected 3 distinct nodes, got %v", seen)
	}
}

// stopCurrentMaster stops the master Sentinel reports now and registers its restore.
// The restore runs even when the test fails, and waits until the topology is healthy again.
func stopCurrentMaster(t *testing.T, restore *[]string) string {
	t.Helper()
	ctx := context.Background()
	master, err := CurrentMaster(ctx)
	if err != nil {
		t.Fatalf("current master: %v", err)
	}
	service := ServiceOf(master) // announced host == compose service name
	*restore = append(*restore, service)
	if err := StopService(ctx, service); err != nil {
		t.Fatalf("stop %s: %v", service, err)
	}
	return master
}

func registerRestore(t *testing.T, restore *[]string) {
	t.Helper()
	t.Cleanup(func() {
		ctx := context.Background()
		for i := len(*restore) - 1; i >= 0; i-- {
			if err := StartService(ctx, (*restore)[i]); err != nil {
				t.Errorf("restore %s: %v", (*restore)[i], err)
			}
		}
		*restore = nil
		waitForHealthyTopology(t, 90*time.Second)
	})
}

func TestChaosClientWritesSucceedWithin30sAfterMasterIsStopped(t *testing.T) {
	requireSentinelProfile(t)
	ctx := context.Background()
	var restore []string
	registerRestore(t, &restore)

	client := ConnectViaSentinel()
	defer func() { _ = client.Close() }()
	key := testkit.UniqueName("lab04-sentinel-chaos")
	t.Cleanup(func() {
		// Registered after the restore cleanup, so it runs first; the failover client finds the master.
		cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		client.Del(cctx, key)
	})
	if err := client.Set(ctx, key, "before", 0).Err(); err != nil {
		t.Fatalf("set before: %v", err)
	}

	oldMaster := stopCurrentMaster(t, &restore)
	stoppedAt := time.Now()
	testkit.Eventually(t, 30*time.Second, func() (struct{}, bool) {
		attempt, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		return struct{}{}, client.Set(attempt, key, "after", 0).Err() == nil
	})
	elapsed := time.Since(stoppedAt)
	t.Logf("failover measured: first successful write %s after the master stopped", elapsed.Round(time.Millisecond))
	if elapsed >= 30*time.Second {
		t.Fatalf("write took %s, want < 30s", elapsed)
	}

	newMaster := testkit.Eventually(t, 10*time.Second, func() (string, bool) {
		m, err := CurrentMaster(ctx)
		return m, err == nil && m != oldMaster
	})
	direct := redis.NewClient(&redis.Options{Addr: HostAddr(newMaster)})
	defer func() { _ = direct.Close() }()
	if got, _ := direct.Get(ctx, key).Result(); got != "after" {
		t.Fatalf("value on the new master %s = %q, want after", newMaster, got)
	}
}

func TestChaosOldMasterRejoinsAsReplica(t *testing.T) {
	requireSentinelProfile(t)
	ctx := context.Background()
	var restore []string
	registerRestore(t, &restore)

	oldMaster := stopCurrentMaster(t, &restore)
	newMaster := testkit.Eventually(t, 30*time.Second, func() (string, bool) {
		m, err := CurrentMaster(ctx)
		return m, err == nil && m != oldMaster
	})

	// Bring the old master back. It starts as a master (no --replicaof), Sentinel demotes it.
	if err := StartService(ctx, ServiceOf(oldMaster)); err != nil {
		t.Fatalf("start %s: %v", oldMaster, err)
	}
	restore = nil
	masterHost, _, _ := net.SplitHostPort(newMaster)
	testkit.Eventually(t, 60*time.Second, func() (struct{}, bool) {
		node := redis.NewClient(&redis.Options{Addr: HostAddr(oldMaster), DialTimeout: 2 * time.Second, MaxRetries: -1})
		defer func() { _ = node.Close() }()
		attempt, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		role, err := node.Do(attempt, "ROLE").Slice()
		if err != nil || len(role) < 2 {
			return struct{}{}, false
		}
		// ROLE of a replica: ["slave", masterHost, masterPort, state, offset]
		return struct{}{}, fmt.Sprint(role[0]) == "slave" && fmt.Sprint(role[1]) == masterHost
	})

	waitForHealthyTopology(t, 90*time.Second)
	topology, err := ReadTopology(ctx)
	if err != nil {
		t.Fatalf("topology: %v", err)
	}
	if topology.Master != newMaster {
		t.Fatalf("master = %s, want %s", topology.Master, newMaster)
	}
	found := false
	for _, r := range topology.Replicas {
		found = found || r == oldMaster
	}
	if !found {
		t.Fatalf("old master %s is not among the replicas %v", oldMaster, topology.Replicas)
	}
}
