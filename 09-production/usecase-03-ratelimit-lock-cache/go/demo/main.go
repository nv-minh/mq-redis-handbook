// Demo ba mảnh hạ tầng trên Redis: sliding window rate limiter, distributed lock (kể cả lock hết hạn khi
// đang giữ và fencing token) và cache-aside có chống stampede bằng singleflight.
// Mọi key nằm dưới một prefix riêng và bị xóa ở cuối.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"regexp"
	"sync"
	"sync/atomic"
	"time"

	lab "github.com/nv-minh/mq-redis-handbook/09-production/usecase-03-ratelimit-lock-cache/go"
	"github.com/nv-minh/mq-redis-handbook/internal/testkit"
	"github.com/redis/go-redis/v9"
)

func redisURL() string {
	if url := os.Getenv("REDIS_URL"); url != "" {
		return url
	}
	return "redis://127.0.0.1:6379"
}

// escapeGlob escape các ký tự glob của MATCH: \ * ? [ ].
func escapeGlob(text string) string {
	return regexp.MustCompile(`[\\*?\[\]]`).ReplaceAllString(text, `\$0`)
}

func deleteByPrefix(ctx context.Context, rdb *redis.Client, prefix string) error {
	var cursor uint64
	for {
		keys, next, err := rdb.Scan(ctx, cursor, escapeGlob(prefix)+"*", 200).Result()
		if err != nil {
			return err
		}
		if len(keys) > 0 {
			if err := rdb.Del(ctx, keys...).Err(); err != nil {
				return err
			}
		}
		cursor = next
		if cursor == 0 {
			return nil
		}
	}
}

// burst chạy n lời gọi đồng thời và trả về số lời gọi nhận true.
func burst(n int, call func() (bool, error)) (int, error) {
	var (
		wg       sync.WaitGroup
		accepted atomic.Int64
		firstErr atomic.Pointer[error]
	)
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			ok, err := call()
			if err != nil {
				firstErr.CompareAndSwap(nil, &err)
				return
			}
			if ok {
				accepted.Add(1)
			}
		}()
	}
	close(start)
	wg.Wait()
	if err := firstErr.Load(); err != nil {
		return 0, *err
	}
	return int(accepted.Load()), nil
}

func rateLimiterDemo(ctx context.Context, rdb *redis.Client, prefix string) error {
	fmt.Println("== 1. Rate limiter: tối đa 5 request trong cửa sổ 1000 ms, 20 request đến cùng lúc ==")
	const (
		requests = 20
		limit    = 5
		window   = time.Second
	)
	luaKey, naiveKey := prefix+":rl-lua", prefix+":rl-naive"
	allowed, err := burst(requests, func() (bool, error) {
		return lab.SlidingWindowAllow(ctx, rdb, luaKey, limit, window)
	})
	if err != nil {
		return err
	}
	fmt.Printf("Lua script (atomic): allowed %d, rejected %d\n", allowed, requests-allowed)

	naive, err := burst(requests, func() (bool, error) {
		return lab.SlidingWindowAllowNaive(ctx, rdb, naiveKey, limit, window)
	})
	if err != nil {
		return err
	}
	fmt.Printf("Đếm rồi ghi bằng lệnh riêng lẻ: allowed %d, rejected %d\n", naive, requests-naive)
	fmt.Println("Bản naive cho vượt limit vì nhiều caller cùng đọc 'còn chỗ' rồi cùng ghi (kết quả phụ thuộc may rủi của race).")

	start := time.Now()
	for {
		ok, err := lab.SlidingWindowAllow(ctx, rdb, luaKey, limit, window)
		if err != nil {
			return err
		}
		if ok {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	fmt.Printf("Cửa sổ trượt: request được allow lại sau %d ms, khi các request cũ nhất rời cửa sổ.\n", time.Since(start).Milliseconds())
	ttl, err := rdb.PTTL(ctx, luaKey).Result()
	if err != nil {
		return err
	}
	fmt.Printf("Key có TTL %d ms nên key idle tự biến mất.\n\n", ttl.Milliseconds())
	return nil
}

// storage là resource mô phỏng: một giá trị cùng fencing token lớn nhất từng được chấp nhận.
type storage struct {
	mu        sync.Mutex
	value     string
	lastFence int64
}

// writeUnsafe không kiểm tra gì: ai tới sau thắng.
func (s *storage) writeUnsafe(writer string) { s.value = writer }

// writeFenced từ chối token cũ hơn token đã thấy: đây là phần Redis lock không tự làm được.
func (s *storage) writeFenced(fence int64, writer string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if fence < s.lastFence {
		return false
	}
	s.lastFence = fence
	s.value = writer
	return true
}

// fencedAcquireScript chỉ cho demo: lấy lock và INCR bộ đếm fencing trong cùng MỘT script,
// nên token tăng đúng thứ tự lấy lock.
var fencedAcquireScript = redis.NewScript(`
if redis.call('SET', KEYS[1], ARGV[1], 'NX', 'PX', ARGV[2]) then
  return redis.call('INCR', KEYS[2])
end
return 0
`)

type holder struct {
	token string
	fence int64
}

func fencedAcquire(ctx context.Context, rdb *redis.Client, lockKey, fenceKey string, ttl time.Duration) (*holder, error) {
	buf := make([]byte, 8)
	if _, err := rand.Read(buf); err != nil {
		return nil, err
	}
	token := "token-" + hex.EncodeToString(buf)
	fence, err := fencedAcquireScript.Run(ctx, rdb, []string{lockKey, fenceKey}, token, ttl.Milliseconds()).Int64()
	if err != nil {
		return nil, err
	}
	if fence == 0 {
		return nil, nil
	}
	return &holder{token: token, fence: fence}, nil
}

func lockDemo(ctx context.Context, rdb *redis.Client, prefix string) error {
	fmt.Println("== 2. Distributed lock: 5 worker tranh nhau một lock (SET NX PX + token) ==")
	lockKey := prefix + ":lock"
	workers := []string{"w1", "w2", "w3", "w4", "w5"}
	tokens := make([]string, len(workers))
	oks := make([]bool, len(workers))
	errs := make([]error, len(workers))
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			tokens[i], oks[i], errs[i] = lab.AcquireLock(ctx, rdb, lockKey, 5*time.Second)
		}()
	}
	close(start)
	wg.Wait()
	winner, wins := -1, 0
	for i, name := range workers {
		if errs[i] != nil {
			return errs[i]
		}
		if oks[i] {
			winner = i
			wins++
			fmt.Printf("  %s: LẤY ĐƯỢC lock, token %s\n", name, tokens[i])
		} else {
			fmt.Printf("  %s: không lấy được (lock đang bị giữ)\n", name)
		}
	}
	fmt.Printf("Đúng một worker thắng: %d.\n", wins)
	wrong, err := lab.ReleaseLock(ctx, rdb, lockKey, "token-cua-nguoi-khac")
	if err != nil {
		return err
	}
	left, err := rdb.Exists(ctx, lockKey).Result()
	if err != nil {
		return err
	}
	fmt.Printf("Release với token sai: %v (lock vẫn còn, %d key).\n", wrong, left)
	right, err := lab.ReleaseLock(ctx, rdb, lockKey, tokens[winner])
	if err != nil {
		return err
	}
	fmt.Printf("Release với token đúng của %s: %v.\n\n", workers[winner], right)

	fmt.Println("== 2b. Lock hết hạn khi holder còn đang làm việc (GC pause, network delay) ==")
	fenceKey := prefix + ":lock:fence"
	a, err := fencedAcquire(ctx, rdb, lockKey, fenceKey, 300*time.Millisecond)
	if err != nil || a == nil {
		return fmt.Errorf("holder A không lấy được lock (lỗi: %v)", err)
	}
	fmt.Printf("A lấy lock (TTL 300 ms), fencing token %d. Rồi A bị pause quá TTL...\n", a.fence)
	var b *holder
	for b == nil {
		if b, err = fencedAcquire(ctx, rdb, lockKey, fenceKey, 5*time.Second); err != nil {
			return err
		}
		if b == nil {
			time.Sleep(50 * time.Millisecond)
		}
	}
	fmt.Printf("Lock hết hạn, B lấy được lock, fencing token %d. Giờ A và B đều tin mình đang giữ lock.\n", b.fence)

	unsafe := &storage{value: "(trống)"}
	unsafe.writeUnsafe("B")
	unsafe.writeUnsafe("A (muộn)")
	fmt.Printf("Resource KHÔNG kiểm tra token: B ghi rồi A tỉnh dậy ghi đè, giá trị cuối = %q. Mutual exclusion đã vỡ.\n", unsafe.value)

	fenced := &storage{value: "(trống)"}
	bOK := fenced.writeFenced(b.fence, "B")
	aOK := fenced.writeFenced(a.fence, "A (muộn)")
	fmt.Printf("Resource kiểm tra fencing token: B (token %d) %s, A (token %d) %s, giá trị cuối = %q.\n",
		b.fence, verdict(bOK), a.fence, verdict(aOK), fenced.value)

	late, err := lab.ReleaseLock(ctx, rdb, lockKey, a.token)
	if err != nil {
		return err
	}
	left, err = rdb.Exists(ctx, lockKey).Result()
	if err != nil {
		return err
	}
	fmt.Printf("A release muộn: %v (token ngăn A xóa nhầm lock của B, còn %d key).\n", late, left)
	if _, err := lab.ReleaseLock(ctx, rdb, lockKey, b.token); err != nil {
		return err
	}
	fmt.Println("Kết luận: token chỉ chặn xóa nhầm. Muốn chặn ghi muộn thì resource phải kiểm tra fencing token. Lock này chỉ dùng cho efficiency.")
	fmt.Println()
	return nil
}

func verdict(ok bool) string {
	if ok {
		return "được ghi"
	}
	return "bị từ chối"
}

type user struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

// slowLoader giả lập truy vấn DB chậm 100 ms và đếm số lần được gọi.
func slowLoader(calls *atomic.Int64) func(context.Context) (user, error) {
	return func(context.Context) (user, error) {
		calls.Add(1)
		time.Sleep(100 * time.Millisecond)
		return user{ID: 7, Name: "Linh"}, nil
	}
}

func cacheDemo(ctx context.Context, rdb *redis.Client, prefix string) error {
	fmt.Println("== 3. Cache-aside: 100 request đồng thời cùng miss một key lạnh ==")
	const callers = 100
	var unprotectedCalls atomic.Int64
	if _, err := burst(callers, func() (bool, error) {
		_, err := lab.GetUnprotected(ctx, rdb, prefix+":cache-unprotected", time.Minute, slowLoader(&unprotectedCalls))
		return true, err
	}); err != nil {
		return err
	}
	fmt.Printf("Không chống stampede: loader (DB) bị gọi %d lần.\n", unprotectedCalls.Load())

	var protectedCalls atomic.Int64
	cache := lab.NewCacheAside[user](rdb)
	protectedKey := prefix + ":cache-protected"
	if _, err := burst(callers, func() (bool, error) {
		_, err := cache.Get(ctx, protectedKey, time.Minute, slowLoader(&protectedCalls))
		return true, err
	}); err != nil {
		return err
	}
	fmt.Printf("Có singleflight: loader (DB) bị gọi %d lần.\n", protectedCalls.Load())
	raw, err := rdb.Get(ctx, protectedKey).Result()
	if err != nil {
		return err
	}
	ttl, err := rdb.PTTL(ctx, protectedKey).Result()
	if err != nil {
		return err
	}
	fmt.Printf("Giá trị trong Redis: %s, TTL %d ms.\n", raw, ttl.Milliseconds())

	// Hai instance, mỗi instance một singleflight.Group riêng, cùng miss một key lạnh.
	var twoCalls atomic.Int64
	loader := slowLoader(&twoCalls)
	instances := []*lab.CacheAside[user]{lab.NewCacheAside[user](rdb), lab.NewCacheAside[user](rdb)}
	var wg sync.WaitGroup
	errs := make([]error, len(instances))
	for i, instance := range instances {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, errs[i] = instance.Get(ctx, prefix+":cache-two-instances", time.Minute, loader)
		}()
	}
	wg.Wait()
	if err := errors.Join(errs...); err != nil {
		return err
	}
	fmt.Printf("Hai instance cùng miss một key lạnh: loader bị gọi %d lần (mỗi instance một lần).\n", twoCalls.Load())
	fmt.Println("Chống stampede này chỉ có tác dụng trong MỘT process: N instance vẫn là N lần load (muốn hơn thì kết hợp với lock).")
	fmt.Println()
	return nil
}

func run() error {
	opts, err := redis.ParseURL(redisURL())
	if err != nil {
		return fmt.Errorf("REDIS_URL không hợp lệ: %w", err)
	}
	rdb := redis.NewClient(opts)
	prefix := testkit.UniqueName("demo-uc03")
	ctx := context.Background()

	var errs []error
	for _, step := range []func(context.Context, *redis.Client, string) error{rateLimiterDemo, lockDemo, cacheDemo} {
		if err := step(ctx, rdb, prefix); err != nil {
			errs = append(errs, err)
			break
		}
	}
	// Dọn theo pha tuần tự: xóa key trước, đóng connection sau.
	if err := deleteByPrefix(ctx, rdb, prefix); err != nil {
		errs = append(errs, fmt.Errorf("xóa key của demo: %w", err))
	}
	if err := rdb.Close(); err != nil {
		errs = append(errs, err)
	}
	if err := errors.Join(errs...); err != nil {
		return err
	}
	fmt.Println("Đã dọn mọi key của demo.")
	return nil
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "Demo lỗi:", err)
		os.Exit(1)
	}
}
