package lab

import (
	"context"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nv-minh/mq-redis-handbook/internal/testkit"
	"github.com/redis/go-redis/v9"
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
	// Pool đủ lớn để 20 goroutine đi trên các socket khác nhau.
	opts.PoolSize = 25
	rdb := redis.NewClient(opts)
	t.Cleanup(func() { _ = rdb.Close() })
	return rdb
}

// newMessageID đăng ký id trước khi dùng để Cleanup luôn xóa key, kể cả khi test fail giữa chừng.
func newMessageID(t *testing.T, rdb *redis.Client) string {
	t.Helper()
	id := testkit.UniqueName("lab01-msg")
	t.Cleanup(func() { rdb.Del(context.Background(), DedupKey(id)) })
	return id
}

func exists(t *testing.T, rdb *redis.Client, id string) bool {
	t.Helper()
	n, err := rdb.Exists(context.Background(), DedupKey(id)).Result()
	if err != nil {
		t.Fatalf("EXISTS lỗi: %v", err)
	}
	return n == 1
}

func countingEffect(counter *atomic.Int64) func(context.Context) error {
	return func(context.Context) error {
		counter.Add(1)
		return nil
	}
}

func TestDuplicateDeliveryAppliesEffectOnce(t *testing.T) {
	ctx := context.Background()
	rdb := newClient(t)
	id := newMessageID(t, rdb)
	var applied atomic.Int64

	want := []Result{Applied, Duplicate, Duplicate}
	for i, w := range want {
		got, err := ProcessOnce(ctx, rdb, id, countingEffect(&applied), 60)
		if err != nil {
			t.Fatalf("lần %d: ProcessOnce lỗi: %v", i+1, err)
		}
		if got != w {
			t.Fatalf("lần %d: kết quả %q, mong đợi %q", i+1, got, w)
		}
	}
	if applied.Load() != 1 {
		t.Fatalf("effect chạy %d lần, mong đợi 1", applied.Load())
	}

	// Sau khi thành công key vẫn còn và có TTL: đó chính là bộ nhớ dedup.
	if !exists(t, rdb, id) {
		t.Fatal("key dedup phải còn sau khi effect thành công")
	}
	ttl, err := rdb.TTL(ctx, DedupKey(id)).Result()
	if err != nil {
		t.Fatalf("TTL lỗi: %v", err)
	}
	if ttl <= 0 || ttl > 60*time.Second {
		t.Fatalf("TTL = %s, mong đợi trong (0, 60s]", ttl)
	}
}

func TestDifferentMessageIDsAreIndependent(t *testing.T) {
	ctx := context.Background()
	rdb := newClient(t)
	a, b := newMessageID(t, rdb), newMessageID(t, rdb)
	var applied atomic.Int64
	for _, id := range []string{a, b} {
		got, err := ProcessOnce(ctx, rdb, id, countingEffect(&applied), 60)
		if err != nil || got != Applied {
			t.Fatalf("id %s: got %q, err %v, mong đợi applied", id, got, err)
		}
	}
	if applied.Load() != 2 {
		t.Fatalf("effect chạy %d lần, mong đợi 2", applied.Load())
	}
}

func TestFailedEffectReleasesKeySoRetryCanApply(t *testing.T) {
	ctx := context.Background()
	rdb := newClient(t)
	id := newMessageID(t, rdb)
	boom := errors.New("downstream timeout")

	// Effect lỗi: lỗi gốc được trả lại (errors.Is) và key bị nhả để lần giao lại còn áp dụng được.
	got, err := ProcessOnce(ctx, rdb, id, func(context.Context) error { return boom }, 60)
	if !errors.Is(err, boom) {
		t.Fatalf("lỗi = %v, mong đợi lỗi gốc của effect", err)
	}
	if got != "" {
		t.Fatalf("kết quả khi lỗi = %q, mong đợi rỗng", got)
	}
	if exists(t, rdb, id) {
		t.Fatal("key phải được nhả khi effect lỗi")
	}

	// Redelivery: lần này effect thành công và được áp dụng.
	var successes atomic.Int64
	got, err = ProcessOnce(ctx, rdb, id, countingEffect(&successes), 60)
	if err != nil || got != Applied {
		t.Fatalf("retry: got %q, err %v, mong đợi applied", got, err)
	}
	if successes.Load() != 1 || !exists(t, rdb, id) {
		t.Fatalf("retry: successes = %d, key tồn tại = %v", successes.Load(), exists(t, rdb, id))
	}
}

func TestConcurrentDuplicatesApplyEffectOnce(t *testing.T) {
	ctx := context.Background()
	rdb := newClient(t)
	id := newMessageID(t, rdb)
	const callers = 20
	var effectCalls atomic.Int64
	gate := make(chan struct{})
	effect := func(context.Context) error {
		effectCalls.Add(1)
		// Người thắng giữ effect đang chạy cho tới khi 19 người còn lại đã có kết quả,
		// nên 20 lời gọi chắc chắn chồng lên nhau.
		<-gate
		return nil
	}

	type outcome struct {
		result Result
		err    error
	}
	outcomes := make(chan outcome, callers)
	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, err := ProcessOnce(ctx, rdb, id, effect, 60)
			outcomes <- outcome{r, err}
		}()
	}

	var results []Result
	collect := func() {
		select {
		case o := <-outcomes:
			if o.err != nil {
				t.Errorf("ProcessOnce lỗi: %v", o.err)
			}
			results = append(results, o.result)
		case <-time.After(10 * time.Second):
			t.Error("hết 10s vẫn chưa đủ kết quả")
			close(gate) // không để goroutine treo
			wg.Wait()
			t.FailNow()
		}
	}
	for i := 0; i < callers-1; i++ {
		collect()
	}
	// Mới 19 lời gọi xong, người thắng vẫn còn chờ gate.
	close(gate)
	collect()
	wg.Wait()

	if effectCalls.Load() != 1 {
		t.Fatalf("effect chạy %d lần, mong đợi 1", effectCalls.Load())
	}
	applied, duplicate := 0, 0
	for _, r := range results {
		switch r {
		case Applied:
			applied++
		case Duplicate:
			duplicate++
		}
	}
	if applied != 1 || duplicate != callers-1 {
		t.Fatalf("applied = %d, duplicate = %d, mong đợi 1 và %d", applied, duplicate, callers-1)
	}
}

func TestInFlightDuplicateIsReportedDuplicateAndLostWhenFirstEffectFails(t *testing.T) {
	// Cửa sổ đã biết của pattern: trong lúc effect đầu còn chạy, bản trùng nhận Duplicate
	// (consumer thật sẽ ack nó). Nếu effect đầu rồi lỗi và nhả key thì bản trùng đã ack đó mất luôn.
	ctx := context.Background()
	rdb := newClient(t)
	id := newMessageID(t, rdb)
	boom := errors.New("effect đầu lỗi")
	release := make(chan struct{})

	firstErr := make(chan error, 1)
	go func() {
		_, err := ProcessOnce(ctx, rdb, id, func(context.Context) error {
			<-release
			return boom
		}, 60)
		firstErr <- err
	}()
	// Chờ tới khi lời gọi đầu đã chiếm key (effect đang bay).
	testkit.Eventually(t, 10*time.Second, func() (struct{}, bool) {
		n, err := rdb.Exists(ctx, DedupKey(id)).Result()
		return struct{}{}, err == nil && n == 1
	})

	var successes atomic.Int64
	second, err := ProcessOnce(ctx, rdb, id, countingEffect(&successes), 60)
	if err != nil || second != Duplicate {
		close(release)
		t.Fatalf("lời gọi thứ hai: got %q, err %v, mong đợi duplicate", second, err)
	}

	close(release)
	select {
	case err := <-firstErr:
		if !errors.Is(err, boom) {
			t.Fatalf("lời gọi đầu trả %v, mong đợi lỗi gốc", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("lời gọi đầu không kết thúc")
	}

	// Kết quả: key đã nhả, effect chưa từng được áp dụng, mà bản trùng đã nhận "duplicate" (đã ack).
	if exists(t, rdb, id) {
		t.Fatal("key phải được nhả sau khi effect đầu lỗi")
	}
	if successes.Load() != 0 {
		t.Fatalf("effect của bản trùng chạy %d lần, mong đợi 0", successes.Load())
	}
}

func TestKeyExpiresAfterTTLSoLaterDuplicateIsAppliedAgain(t *testing.T) {
	// Tính chất của pattern: dedup chỉ nhớ trong ttlSeconds. TTL phải lớn hơn cửa sổ redeliver.
	ctx := context.Background()
	rdb := newClient(t)
	id := newMessageID(t, rdb)
	var applied atomic.Int64

	if got, err := ProcessOnce(ctx, rdb, id, countingEffect(&applied), 1); err != nil || got != Applied {
		t.Fatalf("lần đầu: got %q, err %v", got, err)
	}
	testkit.Eventually(t, 10*time.Second, func() (struct{}, bool) {
		n, err := rdb.Exists(ctx, DedupKey(id)).Result()
		return struct{}{}, err == nil && n == 0
	})
	if got, err := ProcessOnce(ctx, rdb, id, countingEffect(&applied), 1); err != nil || got != Applied {
		t.Fatalf("sau TTL: got %q, err %v, mong đợi applied", got, err)
	}
	if applied.Load() != 2 {
		t.Fatalf("effect chạy %d lần, mong đợi 2", applied.Load())
	}
}

func TestInvalidTTLIsRejectedBeforeTouchingRedis(t *testing.T) {
	ctx := context.Background()
	rdb := newClient(t)
	id := newMessageID(t, rdb)
	for _, ttl := range []int{0, -5} {
		if _, err := ProcessOnce(ctx, rdb, id, countingEffect(new(atomic.Int64)), ttl); err == nil {
			t.Fatalf("ttl %d phải bị từ chối", ttl)
		}
	}
	if exists(t, rdb, id) {
		t.Fatal("ttl không hợp lệ không được tạo key")
	}
}
