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
		t.Fatalf("REDIS_URL không hợp lệ: %v", err)
	}
	rdb := redis.NewClient(opts)
	t.Cleanup(func() { _ = rdb.Close() })
	return rdb
}

// trackKeys UNLINK các key khi test kết thúc.
// UNLINK giải phóng value lớn ở thread nền, nên việc dọn dẹp không chặn Redis (DEL thì có thể chặn).
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

// createSmallKeys tạo count key string rất nhỏ dưới prefix.
func createSmallKeys(t *testing.T, rdb *redis.Client, track func(...string), prefix string, count int) {
	t.Helper()
	pipe := rdb.Pipeline()
	for i := 0; i < count; i++ {
		key := fmt.Sprintf("%s:small:%d", prefix, i)
		track(key)
		pipe.Set(context.Background(), key, "x", 0)
	}
	if _, err := pipe.Exec(context.Background()); err != nil {
		t.Fatalf("tạo key nhỏ lỗi: %v", err)
	}
}

func TestFindBigKeysReturnsKeyOverThreshold(t *testing.T) {
	ctx := context.Background()
	rdb := newClient(t)
	track := trackKeys(t, rdb)
	prefix := testkit.UniqueName("lab04-bigkey")
	createSmallKeys(t, rdb, track, prefix, 300)

	// Một chuỗi 1 MiB và một list khoảng 1 MiB gồm nhiều phần tử nhỏ (big key do nhiều phần tử, không do một value lớn).
	bigString, bigList := prefix+":big-string", prefix+":big-list"
	track(bigString, bigList)
	if err := rdb.Set(ctx, bigString, strings.Repeat("x", mib), 0).Err(); err != nil {
		t.Fatalf("SET chuỗi lớn lỗi: %v", err)
	}
	elements := make([]any, 100)
	for i := range elements {
		elements[i] = strings.Repeat("y", 100)
	}
	for range 100 {
		if err := rdb.RPush(ctx, bigList, elements...).Err(); err != nil {
			t.Fatalf("RPUSH lỗi: %v", err)
		}
	}

	// Ngưỡng 512 KiB nằm giữa các key nhỏ (vài chục byte) và hai key lớn (khoảng 1 MiB).
	found, err := FindBigKeys(ctx, rdb, 512*kib, prefix+":*")
	if err != nil {
		t.Fatalf("FindBigKeys lỗi: %v", err)
	}
	// Chỉ trả về các key lớn, key lớn nhất đứng đầu, và không có key nhỏ nào lọt vào.
	if want := []string{bigString, bigList}; !slices.Equal(found, want) {
		t.Fatalf("FindBigKeys = %v, mong đợi %v", found, want)
	}
}

func TestFindBigKeysIgnoresSmallKeys(t *testing.T) {
	ctx := context.Background()
	rdb := newClient(t)
	track := trackKeys(t, rdb)
	prefix := testkit.UniqueName("lab04-smallkeys")
	createSmallKeys(t, rdb, track, prefix, 500)
	// Value 100 KiB là lớn với một entry cache nhưng vẫn dưới ngưỡng 512 KiB.
	medium := prefix + ":medium"
	track(medium)
	if err := rdb.Set(ctx, medium, strings.Repeat("m", 100*kib), 0).Err(); err != nil {
		t.Fatalf("SET key cỡ vừa lỗi: %v", err)
	}

	found, err := FindBigKeys(ctx, rdb, 512*kib, prefix+":*")
	if err != nil || len(found) != 0 {
		t.Fatalf("FindBigKeys(512 KiB) = %v err=%v, mong đợi rỗng", found, err)
	}
	// Hạ ngưỡng xuống dưới key cỡ vừa thì cùng dữ liệu đó được tìm thấy,
	// chứng tỏ lần quét đã đi qua nó và chỉ có ngưỡng quyết định kết quả.
	found, err = FindBigKeys(ctx, rdb, 50*kib, prefix+":*")
	if err != nil || !slices.Equal(found, []string{medium}) {
		t.Fatalf("FindBigKeys(50 KiB) = %v err=%v, mong đợi [%s]", found, err, medium)
	}
}
