package lab

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/nv-minh/mq-redis-handbook/internal/testkit"
	"github.com/redis/go-redis/v9"
)

const (
	headroom  = 256 * 1024
	coldKeys  = 1500
	valueSize = 1024
)

func redisURL() string {
	if url := os.Getenv("REDIS_URL"); url != "" {
		return url
	}
	return "redis://127.0.0.1:6379"
}

// setup connects, saves the eviction config BEFORE the first CONFIG SET, and registers cleanups.
// t.Cleanup runs last-in first-out: the config is restored first (so deletes are never blocked by a
// full memory), then the test keys are dropped, then the client is closed.
//
// This lab CHANGES the maxmemory settings of the server it connects to (and restores them).
// Only run it against the Redis of this handbook's compose stack (make up).
func setup(t *testing.T) (*redis.Client, string) {
	t.Helper()
	opts, err := redis.ParseURL(redisURL())
	if err != nil {
		t.Fatalf("parse REDIS_URL: %v", err)
	}
	rdb := redis.NewClient(opts)
	t.Cleanup(func() { _ = rdb.Close() })

	ctx := context.Background()
	// Refuse before any CONFIG SET: wrong server or leftover config. No restore cleanup is
	// registered yet, so a refusal never writes config back.
	if err := AssertOwnRedis(ctx, rdb, OwnRedisMarker); err != nil {
		t.Fatal(err)
	}
	original, err := ReadConfig(ctx, rdb)
	if err != nil {
		t.Fatalf("read config: %v", err)
	}
	prefix := testkit.UniqueName("lab02-evict")
	t.Cleanup(func() {
		cctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := DeleteByPrefix(cctx, rdb, prefix); err != nil {
			t.Errorf("delete keys: %v", err)
		}
	})
	t.Cleanup(func() {
		cctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := WriteConfig(cctx, rdb, original); err != nil {
			t.Errorf("RESTORING THE REDIS CONFIG FAILED, fix by hand (maxmemory=%s policy=%s): %v",
				original.Maxmemory, original.Policy, err)
		}
	})
	return rdb, prefix
}

func evicted(t *testing.T, rdb *redis.Client) int64 {
	t.Helper()
	n, err := EvictedKeys(context.Background(), rdb)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func TestAllkeysLruEvictsColdKeysAndKeepsHotKey(t *testing.T) {
	ctx := context.Background()
	rdb, prefix := setup(t)
	hot := prefix + ":hot"
	coldPrefix := prefix + ":cold"

	// 1. Load cold keys and the hot key while there is no limit.
	if err := SeedKeys(ctx, rdb, coldPrefix, coldKeys, valueSize); err != nil {
		t.Fatal(err)
	}
	if err := rdb.Set(ctx, hot, "hot", 0).Err(); err != nil {
		t.Fatal(err)
	}

	// 2. LRU idle time has a resolution of one second: wait until the cold keys look old.
	testkit.Eventually(t, 10*time.Second, func() (struct{}, bool) {
		idle, err := rdb.ObjectIdleTime(ctx, coldPrefix+":0").Result()
		return struct{}{}, err == nil && idle >= 2*time.Second
	})
	rdb.Get(ctx, hot) // reading the hot key resets its idle time to 0

	// 3. Cap memory just above what is used now, then keep writing while touching the hot key.
	if _, err := LimitMemory(ctx, rdb, "allkeys-lru", headroom); err != nil {
		t.Fatal(err)
	}
	n, err := FillUntilEviction(ctx, rdb, prefix+":fill", 20_000, FillOptions{TouchKey: hot, MinEvicted: 900})
	if err != nil {
		t.Fatal(err)
	}

	// Never assert an exact number: eviction is approximate (sampling).
	if n <= 0 {
		t.Fatalf("evicted = %d, want > 0", n)
	}
	if exists, _ := rdb.Exists(ctx, hot).Result(); exists != 1 {
		t.Fatal("the hot key was evicted")
	}
	cold := make([]string, coldKeys)
	for i := range cold {
		cold[i] = fmt.Sprintf("%s:%d", coldPrefix, i)
	}
	surviving, err := rdb.Exists(ctx, cold...).Result()
	if err != nil {
		t.Fatal(err)
	}
	if surviving >= coldKeys {
		t.Fatalf("all %d cold keys survived, want some evicted", surviving)
	}
}

func TestNoevictionPolicyRejectsWritesWhenFull(t *testing.T) {
	ctx := context.Background()
	rdb, prefix := setup(t)
	if err := rdb.Set(ctx, prefix+":existing", "still readable", 0).Err(); err != nil {
		t.Fatal(err)
	}
	before := evicted(t, rdb)

	if _, err := LimitMemory(ctx, rdb, "noeviction", headroom); err != nil {
		t.Fatal(err)
	}
	msg, err := FillUntilRejected(ctx, rdb, prefix+":fill", 20_000, valueSize)
	if err != nil {
		t.Fatal(err)
	}

	if !strings.HasPrefix(msg, "OOM") {
		t.Fatalf("write error = %q, want it to start with OOM", msg)
	}
	// Reads keep working, nothing was evicted, and the data written before the limit survives.
	if got, err := rdb.Get(ctx, prefix+":existing").Result(); err != nil || got != "still readable" {
		t.Fatalf("GET existing = %q, %v", got, err)
	}
	if after := evicted(t, rdb); after != before {
		t.Fatalf("evicted_keys changed from %d to %d under noeviction", before, after)
	}
	if exists, _ := rdb.Exists(ctx, prefix+":fill:0").Result(); exists != 1 {
		t.Fatal("data written before the limit was lost")
	}
}

func TestVolatilePolicyRejectsWritesWhenNoKeyHasATTL(t *testing.T) {
	ctx := context.Background()
	rdb, prefix := setup(t)
	before := evicted(t, rdb)

	// volatile-lru may only evict keys that have a TTL. None of ours does, so it behaves like noeviction.
	if _, err := LimitMemory(ctx, rdb, "volatile-lru", headroom); err != nil {
		t.Fatal(err)
	}
	msg, err := FillUntilRejected(ctx, rdb, prefix+":fill", 20_000, valueSize)
	if err != nil {
		t.Fatal(err)
	}

	if !strings.HasPrefix(msg, "OOM") {
		t.Fatalf("write error = %q, want it to start with OOM", msg)
	}
	if after := evicted(t, rdb); after != before {
		t.Fatalf("evicted_keys changed from %d to %d", before, after)
	}
}

func TestConfigIsRestoredToTheSavedValues(t *testing.T) {
	ctx := context.Background()
	rdb, _ := setup(t)
	original, err := ReadConfig(ctx, rdb)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := LimitMemory(ctx, rdb, "allkeys-lru", headroom); err != nil {
		t.Fatal(err)
	}
	if got, _ := ReadConfig(ctx, rdb); got.Policy != "allkeys-lru" {
		t.Fatalf("policy = %q, want allkeys-lru", got.Policy)
	}

	if err := WriteConfig(ctx, rdb, original); err != nil {
		t.Fatal(err)
	}
	if got, _ := ReadConfig(ctx, rdb); got != original {
		t.Fatalf("config = %+v, want %+v", got, original)
	}
}

func TestGuardRefusesARedisWithoutTheHandbookMarkerAndChangesNothing(t *testing.T) {
	ctx := context.Background()
	rdb, _ := setup(t)
	before, err := ReadConfig(ctx, rdb)
	if err != nil {
		t.Fatal(err)
	}
	err = AssertOwnRedis(ctx, rdb, "some-other-marker.rdb")
	if err == nil || !strings.Contains(err.Error(), "not the handbook marker") {
		t.Fatalf("AssertOwnRedis = %v, want a refusal naming the marker", err)
	}
	if after, _ := ReadConfig(ctx, rdb); after != before { // read-only: no CONFIG SET happened
		t.Fatalf("config changed from %+v to %+v", before, after)
	}
}

func TestGuardRefusesLeftoverConfigWithAHintToRecreateTheStack(t *testing.T) {
	state := ServerState{DBFilename: OwnRedisMarker, Maxmemory: "4000000", Policy: "allkeys-lru"}
	if msg := GuardProblem(state, OwnRedisMarker); !strings.Contains(msg, "make down && make up") {
		t.Fatalf("GuardProblem = %q, want the recreate hint", msg)
	}
	state.Maxmemory, state.Policy = "0", "noeviction"
	if msg := GuardProblem(state, OwnRedisMarker); msg != "" {
		t.Fatalf("GuardProblem on a pristine server = %q, want empty", msg)
	}
}
