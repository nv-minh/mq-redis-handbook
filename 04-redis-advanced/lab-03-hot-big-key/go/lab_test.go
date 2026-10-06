package lab

import (
	"context"
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/nv-minh/mq-redis-handbook/internal/testkit"
	"github.com/redis/go-redis/v9"
)

const (
	kib = 1024
	mib = 1024 * kib
)

func newClient(t *testing.T) *redis.Client {
	t.Helper()
	url := os.Getenv("REDIS_URL")
	if url == "" {
		url = "redis://127.0.0.1:6379"
	}
	opts, err := redis.ParseURL(url)
	if err != nil {
		t.Fatalf("parse REDIS_URL: %v", err)
	}
	rdb := redis.NewClient(opts)
	t.Cleanup(func() { _ = rdb.Close() })
	return rdb
}

// trackKeys unlinks the keys when the test ends. UNLINK frees big values in a background
// thread, so the cleanup itself never blocks Redis.
func trackKeys(t *testing.T, rdb *redis.Client) func(keys ...string) {
	t.Helper()
	var keys []string
	t.Cleanup(func() {
		if len(keys) > 0 {
			rdb.Unlink(context.Background(), keys...)
		}
	})
	return func(add ...string) { keys = append(keys, add...) }
}

// createSmallKeys creates count tiny string keys under prefix.
func createSmallKeys(t *testing.T, rdb *redis.Client, track func(...string), prefix string, count int) {
	t.Helper()
	pipe := rdb.Pipeline()
	for i := 0; i < count; i++ {
		key := fmt.Sprintf("%s:small:%d", prefix, i)
		track(key)
		pipe.Set(context.Background(), key, "x", 0)
	}
	if _, err := pipe.Exec(context.Background()); err != nil {
		t.Fatalf("create small keys: %v", err)
	}
}

func TestFindBigKeysReturnsKeyOverThreshold(t *testing.T) {
	ctx := context.Background()
	rdb := newClient(t)
	track := trackKeys(t, rdb)
	prefix := testkit.UniqueName("lab04-bigkey")
	createSmallKeys(t, rdb, track, prefix, 300)

	// One 1 MiB string and one list of about 1 MiB made of many small elements.
	bigString, bigList := prefix+":big-string", prefix+":big-list"
	track(bigString, bigList)
	if err := rdb.Set(ctx, bigString, strings.Repeat("x", mib), 0).Err(); err != nil {
		t.Fatalf("set big string: %v", err)
	}
	elements := make([]any, 100)
	for i := range elements {
		elements[i] = strings.Repeat("y", 100)
	}
	for range 100 {
		if err := rdb.RPush(ctx, bigList, elements...).Err(); err != nil {
			t.Fatalf("rpush: %v", err)
		}
	}

	// 512 KiB sits between the tiny keys and the two big ones.
	found, err := FindBigKeys(ctx, rdb, 512*kib, prefix+":*")
	if err != nil {
		t.Fatalf("FindBigKeys: %v", err)
	}
	// Only the big keys are returned, the biggest first, and no small key.
	if want := []string{bigString, bigList}; !slices.Equal(found, want) {
		t.Fatalf("FindBigKeys = %v, want %v", found, want)
	}
}

func TestFindBigKeysIgnoresSmallKeys(t *testing.T) {
	ctx := context.Background()
	rdb := newClient(t)
	track := trackKeys(t, rdb)
	prefix := testkit.UniqueName("lab04-smallkeys")
	createSmallKeys(t, rdb, track, prefix, 500)
	// A 100 KiB value is large for a cache entry but still under the 512 KiB threshold.
	medium := prefix + ":medium"
	track(medium)
	if err := rdb.Set(ctx, medium, strings.Repeat("m", 100*kib), 0).Err(); err != nil {
		t.Fatalf("set medium: %v", err)
	}

	found, err := FindBigKeys(ctx, rdb, 512*kib, prefix+":*")
	if err != nil || len(found) != 0 {
		t.Fatalf("FindBigKeys(512 KiB) = %v err=%v, want none", found, err)
	}
	// The same data is found once the threshold drops below the medium key, which proves the
	// scan did visit it and only the threshold decided.
	found, err = FindBigKeys(ctx, rdb, 50*kib, prefix+":*")
	if err != nil || !slices.Equal(found, []string{medium}) {
		t.Fatalf("FindBigKeys(50 KiB) = %v err=%v, want [%s]", found, err, medium)
	}
}
