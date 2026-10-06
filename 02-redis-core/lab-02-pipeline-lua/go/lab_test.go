package lab

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/nv-minh/mq-redis-handbook/internal/testkit"
	"github.com/redis/go-redis/v9"
)

const callers = 50

func redisURL() string {
	if url := os.Getenv("REDIS_URL"); url != "" {
		return url
	}
	return "redis://127.0.0.1:6379"
}

func options(t *testing.T) *redis.Options {
	t.Helper()
	opts, err := redis.ParseURL(redisURL())
	if err != nil {
		t.Fatalf("REDIS_URL không hợp lệ: %v", err)
	}
	return opts
}

// newKeys trả về một hàm đăng ký: mọi key nó cấp ra đều bị xóa khi test kết thúc.
func newKeys(t *testing.T, rdb *redis.Client) func(label string) string {
	t.Helper()
	var keys []string
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if len(keys) > 0 {
			rdb.Del(ctx, keys...)
		}
	})
	return func(label string) string {
		key := testkit.UniqueName("lab02-" + label)
		keys = append(keys, key)
		return key
	}
}

func newClient(t *testing.T) *redis.Client {
	t.Helper()
	rdb := redis.NewClient(options(t))
	t.Cleanup(func() { _ = rdb.Close() })
	return rdb
}

// hammer chạy n lời gọi DecrIfPositive cùng bắt đầu tại một thời điểm và trả về số lời gọi thành công.
func hammer(t *testing.T, rdb *redis.Client, key string, n int) int {
	t.Helper()
	ctx := context.Background()
	var (
		wg        sync.WaitGroup
		mu        sync.Mutex
		successes int
	)
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start // thả mọi goroutine cùng một lúc
			ok, err := DecrIfPositive(ctx, rdb, key)
			if err != nil {
				t.Errorf("DecrIfPositive lỗi: %v", err)
				return
			}
			if ok {
				mu.Lock()
				successes++
				mu.Unlock()
			}
		}()
	}
	close(start)
	wg.Wait()
	return successes
}

func value(t *testing.T, rdb *redis.Client, key string) string {
	t.Helper()
	v, err := rdb.Get(context.Background(), key).Result()
	if err != nil {
		t.Fatalf("GET %s: %v", key, err)
	}
	return v
}

func TestPipelineUsesFewerRoundTripsThanSequential(t *testing.T) {
	ctx := context.Background()
	rdb, counter := NewCountingClient(options(t))
	t.Cleanup(func() { _ = rdb.Close() })
	newKey := newKeys(t, rdb)
	prefix := newKey("pipe")
	keys := make([]string, 100)
	for i := range keys {
		keys[i] = fmt.Sprintf("%s:%d", prefix, i)
	}
	t.Cleanup(func() { rdb.Del(context.Background(), keys...) })

	// Làm nóng cả hai connection pool (HELLO và các lệnh tương tự cũng là write), rồi đếm từ 0.
	if err := SetSequential(ctx, rdb, prefix, 1); err != nil {
		t.Fatal(err)
	}
	if err := SetPipelined(ctx, rdb, prefix, 1); err != nil {
		t.Fatal(err)
	}

	counter.Reset()
	if err := SetSequential(ctx, rdb, prefix, 100); err != nil {
		t.Fatal(err)
	}
	sequentialTrips := counter.Writes()

	counter.Reset()
	if err := SetPipelined(ctx, rdb, prefix, 100); err != nil {
		t.Fatal(err)
	}
	pipelinedTrips := counter.Writes()

	// Mỗi lệnh là một write + một reply; pipeline gửi cả 100 lệnh trong một write.
	if sequentialTrips != 100 {
		t.Fatalf("số round trip tuần tự = %d, mong đợi 100", sequentialTrips)
	}
	if pipelinedTrips != 1 {
		t.Fatalf("số round trip pipeline = %d, mong đợi 1", pipelinedTrips)
	}
	// Ít round trip hơn, cùng kết quả: cả 100 key đều tồn tại.
	n, err := rdb.Exists(ctx, keys...).Result()
	if err != nil || n != 100 {
		t.Fatalf("EXISTS = %d, %v; mong đợi 100", n, err)
	}
}

func TestDecrIfPositiveNeverGoesBelowZeroUnder50ConcurrentCallers(t *testing.T) {
	rdb := newClient(t)
	key := newKeys(t, rdb)("never-negative")
	if err := rdb.Set(context.Background(), key, 10, 0).Err(); err != nil {
		t.Fatal(err)
	}

	successes := hammer(t, rdb, key, callers)

	// 50 caller tranh nhau 10 đơn vị: counter dừng ở 0 và không bao giờ âm.
	if got := value(t, rdb, key); got != "0" {
		t.Fatalf("counter = %s, mong đợi 0", got)
	}
	if successes > 10 {
		t.Fatalf("successes = %d, mong đợi <= 10", successes)
	}
}

func TestExactlyNCallersSucceedWhenCounterIsN(t *testing.T) {
	rdb := newClient(t)
	newKey := newKeys(t, rdb)
	for _, n := range []int{1, 20, callers} {
		key := newKey("exact-" + strconv.Itoa(n))
		if err := rdb.Set(context.Background(), key, n, 0).Err(); err != nil {
			t.Fatal(err)
		}
		if got := hammer(t, rdb, key, callers); got != n {
			t.Fatalf("counter = %d: %d caller thành công, mong đợi đúng %d", n, got, n)
		}
		if got := value(t, rdb, key); got != "0" {
			t.Fatalf("counter = %s sau khi rút cạn %d, mong đợi 0", got, n)
		}
	}
}

func TestDecrIfPositiveReturnsFalseAndCreatesNothingForAMissingKey(t *testing.T) {
	ctx := context.Background()
	rdb := newClient(t)
	key := newKeys(t, rdb)("missing")
	ok, err := DecrIfPositive(ctx, rdb, key)
	if err != nil || ok {
		t.Fatalf("DecrIfPositive = %v, %v; mong đợi false, nil", ok, err)
	}
	if n, _ := rdb.Exists(ctx, key).Result(); n != 0 {
		t.Fatalf("key bị tạo ra bởi một lần giảm thất bại")
	}
}

func TestDecrIfPositiveReturnsFalseAtZero(t *testing.T) {
	ctx := context.Background()
	rdb := newClient(t)
	key := newKeys(t, rdb)("zero")
	if err := rdb.Set(ctx, key, 0, 0).Err(); err != nil {
		t.Fatal(err)
	}
	ok, err := DecrIfPositive(ctx, rdb, key)
	if err != nil || ok {
		t.Fatalf("DecrIfPositive = %v, %v; mong đợi false, nil", ok, err)
	}
	if got := value(t, rdb, key); got != "0" {
		t.Fatalf("counter = %s, mong đợi 0", got)
	}
}

func TestNoscriptAfterScriptFlushIsRecoveredByTheClient(t *testing.T) {
	ctx := context.Background()
	rdb := newClient(t)
	key := newKeys(t, rdb)("noscript")
	if err := rdb.Set(ctx, key, 2, 0).Err(); err != nil {
		t.Fatal(err)
	}
	if ok, err := DecrIfPositive(ctx, rdb, key); err != nil || !ok {
		t.Fatalf("DecrIfPositive lần đầu = %v, %v; mong đợi true, nil", ok, err)
	}

	// Server quên mọi script đã cache (điều này cũng xảy ra khi restart hoặc failover).
	if err := rdb.ScriptFlush(ctx).Err(); err != nil {
		t.Fatal(err)
	}

	// EVALSHA fail với NOSCRIPT bên trong redis.Script.Run, hàm này chuyển sang EVAL.
	if ok, err := DecrIfPositive(ctx, rdb, key); err != nil || !ok {
		t.Fatalf("DecrIfPositive sau SCRIPT FLUSH = %v, %v; mong đợi true, nil", ok, err)
	}
	if got := value(t, rdb, key); got != "0" {
		t.Fatalf("counter = %s, mong đợi 0", got)
	}
}
