package lab

import (
	"context"
	"errors"
	"os"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nv-minh/mq-redis-handbook/internal/testkit"
	"github.com/redis/go-redis/v9"
)

// Đồng hồ của test (time.Since, monotonic) và đồng hồ của server là hai đồng hồ khác nhau,
// nên cận dưới về thời gian chừa dung sai nhỏ cho độ phân giải và sai lệch giữa chúng.
const clockSlack = 50 * time.Millisecond

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

func scanKeys(ctx context.Context, rdb *redis.Client, prefix string) ([]string, error) {
	var (
		found  []string
		cursor uint64
	)
	for {
		keys, next, err := rdb.Scan(ctx, cursor, escapeGlob(prefix)+"*", 200).Result()
		if err != nil {
			return nil, err
		}
		found = append(found, keys...)
		cursor = next
		if cursor == 0 {
			return found, nil
		}
	}
}

// env gom client, prefix duy nhất của test và việc dọn dẹp.
type env struct {
	t      *testing.T
	rdb    *redis.Client
	prefix string
}

// newEnv tạo client và đăng ký dọn dẹp theo pha tuần tự: xóa key bằng SCAN theo prefix,
// kiểm tra không còn key sót, rồi mới đóng client (t.Cleanup chạy ngược thứ tự đăng ký).
func newEnv(t *testing.T, label string) *env {
	t.Helper()
	opts, err := redis.ParseURL(redisURL())
	if err != nil {
		t.Fatalf("REDIS_URL không hợp lệ: %v", err)
	}
	e := &env{t: t, rdb: redis.NewClient(opts), prefix: testkit.UniqueName("uc03-" + label)}
	t.Cleanup(func() {
		if err := e.rdb.Close(); err != nil {
			t.Errorf("đóng client: %v", err)
		}
	})
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		keys, err := scanKeys(ctx, e.rdb, e.prefix)
		if err != nil {
			t.Errorf("SCAN dọn dẹp: %v", err)
			return
		}
		if len(keys) > 0 {
			if err := e.rdb.Del(ctx, keys...).Err(); err != nil {
				t.Errorf("DEL dọn dẹp: %v", err)
			}
		}
		left, err := scanKeys(ctx, e.rdb, e.prefix)
		if err != nil || len(left) > 0 {
			t.Errorf("key còn sót của prefix %s: %v (lỗi %v)", e.prefix, left, err)
		}
	})
	return e
}

func (e *env) key(name string) string { return e.prefix + ":" + name }

func mustAllow(t *testing.T, e *env, key string, limit int, window time.Duration) bool {
	t.Helper()
	ok, err := SlidingWindowAllow(context.Background(), e.rdb, key, limit, window)
	if err != nil {
		t.Fatalf("SlidingWindowAllow: %v", err)
	}
	return ok
}

func mustAcquire(t *testing.T, e *env, key string, ttl time.Duration) (string, bool) {
	t.Helper()
	token, ok, err := AcquireLock(context.Background(), e.rdb, key, ttl)
	if err != nil {
		t.Fatalf("AcquireLock: %v", err)
	}
	return token, ok
}

func mustRelease(t *testing.T, e *env, key, token string) bool {
	t.Helper()
	ok, err := ReleaseLock(context.Background(), e.rdb, key, token)
	if err != nil {
		t.Fatalf("ReleaseLock: %v", err)
	}
	return ok
}

func zcard(t *testing.T, e *env, key string) int64 {
	t.Helper()
	n, err := e.rdb.ZCard(context.Background(), key).Result()
	if err != nil {
		t.Fatalf("ZCARD: %v", err)
	}
	return n
}

func exists(t *testing.T, e *env, key string) bool {
	t.Helper()
	n, err := e.rdb.Exists(context.Background(), key).Result()
	if err != nil {
		t.Fatalf("EXISTS: %v", err)
	}
	return n == 1
}

func get(t *testing.T, e *env, key string) string {
	t.Helper()
	v, err := e.rdb.Get(context.Background(), key).Result()
	if err != nil {
		t.Fatalf("GET %s: %v", key, err)
	}
	return v
}

// waitFor chờ wg xong nhưng không quá timeout, để một goroutine treo không làm treo cả test.
func waitFor(t *testing.T, wg *sync.WaitGroup, timeout time.Duration) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(timeout):
		t.Fatalf("hết %s vẫn còn goroutine chưa xong", timeout)
	}
}

func TestAllowsLimitRequestsThenRejectsWithinWindow(t *testing.T) {
	e := newEnv(t, "rl-seq")
	sequential := e.key("seq")
	for i := 0; i < 3; i++ {
		if !mustAllow(t, e, sequential, 3, time.Minute) {
			t.Fatalf("request thứ %d phải được allow", i+1)
		}
	}
	if mustAllow(t, e, sequential, 3, time.Minute) {
		t.Fatal("request thứ 4 phải bị từ chối")
	}

	// 50 goroutine thật sự đồng thời (connection pool nhiều connection) tranh limit = 10: đúng 10 lần allow.
	concurrent := e.key("50")
	var (
		allowed atomic.Int64
		wg      sync.WaitGroup
	)
	start := make(chan struct{})
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start // thả mọi goroutine cùng một lúc
			ok, err := SlidingWindowAllow(context.Background(), e.rdb, concurrent, 10, time.Minute)
			if err != nil {
				t.Errorf("SlidingWindowAllow lỗi: %v", err)
				return
			}
			if ok {
				allowed.Add(1)
			}
		}()
	}
	close(start)
	waitFor(t, &wg, 10*time.Second)
	if allowed.Load() != 10 {
		t.Fatalf("allowed = %d, mong đợi đúng 10", allowed.Load())
	}
	if n := zcard(t, e, concurrent); n != 10 {
		t.Fatalf("ZCARD = %d, mong đợi 10", n)
	}
}

func TestWindowSlidesSoOldRequestsStopCounting(t *testing.T) {
	e := newEnv(t, "rl-slide")
	key := e.key("slide")
	window := time.Second
	start := time.Now()
	for i := 0; i < 2; i++ {
		if !mustAllow(t, e, key, 2, window) {
			t.Fatalf("request thứ %d phải được allow", i+1)
		}
	}
	// Còn trong cửa sổ thì request thứ ba bị từ chối.
	if mustAllow(t, e, key, 2, window) {
		t.Fatal("request thứ ba trong cửa sổ phải bị từ chối")
	}

	// Poll cho tới khi request cũ nhất rời cửa sổ và allow đổi thành true (không sleep cố định).
	testkit.Eventually(t, 5*time.Second, func() (bool, bool) {
		ok := mustAllow(t, e, key, 2, window)
		return ok, ok
	})
	if elapsed := time.Since(start); elapsed < window-clockSlack {
		t.Fatalf("allow đổi thành true sau %s, sớm hơn cửa sổ %s", elapsed, window)
	}
}

func TestRejectedRequestsAreNotRecorded(t *testing.T) {
	e := newEnv(t, "rl-reject")
	key := e.key("reject")
	ctx := context.Background()
	if !mustAllow(t, e, key, 1, time.Minute) {
		t.Fatal("request đầu phải được allow")
	}
	before, err := e.rdb.ZRangeWithScores(ctx, key, 0, -1).Result()
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		if mustAllow(t, e, key, 1, time.Minute) {
			t.Fatalf("request bị từ chối lần %d lại được allow", i+1)
		}
	}
	// Request bị từ chối không ghi gì: sorted set y nguyên, nên cửa sổ không bị đẩy lùi.
	after, err := e.rdb.ZRangeWithScores(ctx, key, 0, -1).Result()
	if err != nil {
		t.Fatal(err)
	}
	if len(before) != 1 || len(after) != 1 || before[0] != after[0] {
		t.Fatalf("sorted set đổi sau các request bị từ chối: trước %v, sau %v", before, after)
	}
}

func TestLimitZeroAlwaysRejectsAndCreatesNoKey(t *testing.T) {
	e := newEnv(t, "rl-zero")
	key := e.key("zero")
	for i := 0; i < 3; i++ {
		if mustAllow(t, e, key, 0, time.Minute) {
			t.Fatal("limit 0 phải luôn từ chối")
		}
	}
	if exists(t, e, key) {
		t.Fatal("limit 0 không được tạo key")
	}
}

func TestInvalidLimitOrWindowIsRejectedWithAClearError(t *testing.T) {
	e := newEnv(t, "rl-invalid")
	key := e.key("invalid")
	cases := []struct {
		name    string
		limit   int
		window  time.Duration
		mention string
	}{
		{"limit âm", -1, time.Second, "limit"},
		{"window bằng 0", 5, 0, "window"},
		{"window âm", 5, -time.Second, "window"},
		{"window không phải số mili giây nguyên", 5, 1500 * time.Microsecond, "window"},
	}
	for _, c := range cases {
		_, err := SlidingWindowAllow(context.Background(), e.rdb, key, c.limit, c.window)
		if !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("%s: err = %v, mong đợi ErrInvalidArgument", c.name, err)
			continue
		}
		if !strings.Contains(err.Error(), c.mention) {
			t.Errorf("%s: message %q phải nhắc tới %q", c.name, err.Error(), c.mention)
		}
	}
	if exists(t, e, key) {
		t.Fatal("tham số sai không được tạo key")
	}
}

func TestKeyHasATTLSoIdleKeysDoNotLeak(t *testing.T) {
	e := newEnv(t, "rl-ttl")
	key := e.key("ttl")
	if !mustAllow(t, e, key, 5, time.Minute) {
		t.Fatal("request đầu phải được allow")
	}
	pttl, err := e.rdb.PTTL(context.Background(), key).Result()
	if err != nil {
		t.Fatal(err)
	}
	if pttl <= 0 || pttl > time.Minute {
		t.Fatalf("PTTL = %s, mong đợi trong (0, 1m]", pttl)
	}
}

func TestDifferentKeysDoNotInfluenceEachOther(t *testing.T) {
	e := newEnv(t, "rl-keys")
	a, b := e.key("a"), e.key("b")
	if !mustAllow(t, e, a, 1, time.Minute) {
		t.Fatal("key a lần đầu phải được allow")
	}
	if mustAllow(t, e, a, 1, time.Minute) {
		t.Fatal("key a lần hai phải bị từ chối")
	}
	if !mustAllow(t, e, b, 1, time.Minute) {
		t.Fatal("key b không được bị ảnh hưởng bởi key a")
	}
}

func TestOnlyOneOfManyConcurrentCallersGetsTheLock(t *testing.T) {
	e := newEnv(t, "lock-race")
	key := e.key("lock")
	var (
		mu      sync.Mutex
		winners []string
		wg      sync.WaitGroup
	)
	start := make(chan struct{})
	for i := 0; i < 30; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			token, ok, err := AcquireLock(context.Background(), e.rdb, key, 30*time.Second)
			if err != nil {
				t.Errorf("AcquireLock lỗi: %v", err)
				return
			}
			if ok {
				mu.Lock()
				winners = append(winners, token)
				mu.Unlock()
			}
		}()
	}
	close(start)
	waitFor(t, &wg, 10*time.Second)

	if len(winners) != 1 {
		t.Fatalf("có %d caller lấy được lock, mong đợi đúng 1", len(winners))
	}
	// Token 128 bit crypto-random, dạng hex: 32 ký tự.
	if !regexp.MustCompile(`^[0-9a-f]{32}$`).MatchString(winners[0]) {
		t.Fatalf("token %q không phải 32 ký tự hex", winners[0])
	}
	if got := get(t, e, key); got != winners[0] {
		t.Fatalf("giá trị của lock = %q, mong đợi token của người thắng", got)
	}
	if pttl, err := e.rdb.PTTL(context.Background(), key).Result(); err != nil || pttl <= 0 {
		t.Fatalf("PTTL = %s, %v; lock phải có TTL", pttl, err)
	}
}

func TestReleaseWithWrongTokenDoesNotRelease(t *testing.T) {
	e := newEnv(t, "lock-wrong")
	key := e.key("lock")
	token, ok := mustAcquire(t, e, key, 30*time.Second)
	if !ok {
		t.Fatal("lần acquire đầu phải thành công")
	}

	if mustRelease(t, e, key, "not-the-token") {
		t.Fatal("release với token sai phải trả false")
	}
	// Lock vẫn còn, người khác vẫn không lấy được, và chủ thật vẫn release được.
	if got := get(t, e, key); got != token {
		t.Fatalf("lock bị đổi: %q", got)
	}
	if _, other := mustAcquire(t, e, key, 30*time.Second); other {
		t.Fatal("người khác không được lấy lock đang bị giữ")
	}
	if !mustRelease(t, e, key, token) {
		t.Fatal("chủ thật phải release được")
	}
	if exists(t, e, key) {
		t.Fatal("lock phải biến mất sau khi release")
	}
	// Release lại hoặc release lock không tồn tại đều trả false, không trả lỗi.
	if mustRelease(t, e, key, token) {
		t.Fatal("release lần hai phải trả false")
	}
}

func TestLockExpiresAfterTTL(t *testing.T) {
	e := newEnv(t, "lock-ttl")
	key := e.key("lock")
	ttl := 500 * time.Millisecond
	start := time.Now()
	tokenA, ok := mustAcquire(t, e, key, ttl)
	if !ok {
		t.Fatal("lần acquire đầu phải thành công")
	}
	// Còn trong TTL thì người khác chưa lấy được.
	if _, other := mustAcquire(t, e, key, 30*time.Second); other {
		t.Fatal("lock còn hạn mà người khác lại lấy được")
	}

	// Poll cho tới khi lock hết hạn và caller khác lấy được (không sleep cố định).
	tokenB := testkit.Eventually(t, 5*time.Second, func() (string, bool) {
		token, ok := mustAcquire(t, e, key, 30*time.Second)
		return token, ok
	})
	if elapsed := time.Since(start); elapsed < ttl-clockSlack {
		t.Fatalf("lock bị lấy lại sau %s, sớm hơn TTL %s", elapsed, ttl)
	}
	if tokenA == tokenB {
		t.Fatal("hai holder phải có token khác nhau")
	}

	// Holder cũ release muộn: false, và KHÔNG xóa lock của holder mới (lỗi kinh điển mà token ngăn được).
	if mustRelease(t, e, key, tokenA) {
		t.Fatal("holder cũ release muộn phải trả false")
	}
	if got := get(t, e, key); got != tokenB {
		t.Fatalf("lock của holder mới bị đổi: %q", got)
	}
	if !mustRelease(t, e, key, tokenB) {
		t.Fatal("holder mới phải release được")
	}
}

func TestExpiredHolderStillBelievesItHoldsTheLock(t *testing.T) {
	// Lỗi mà lock đơn node KHÔNG tránh được: holder A bị pause (GC, network) lâu hơn TTL.
	// Redis không báo gì cho A. Từ lúc B lấy lock, cả A và B đều tin mình đang giữ lock.
	e := newEnv(t, "lock-two-holders")
	key := e.key("lock")
	tokenA, ok := mustAcquire(t, e, key, 300*time.Millisecond)
	if !ok {
		t.Fatal("lần acquire đầu phải thành công")
	}
	tokenB := testkit.Eventually(t, 5*time.Second, func() (string, bool) {
		return mustAcquire(t, e, key, 30*time.Second)
	})

	// A vẫn cầm token của mình và không có cách nào biết nó đã mất lock, cho tới khi release.
	if tokenA == tokenB {
		t.Fatal("hai holder phải có token khác nhau")
	}
	if got := get(t, e, key); got != tokenB {
		t.Fatalf("Redis phải giữ token của B, nhận %q", got)
	}
	// Token chỉ giúp A không xóa nhầm lock của B, không ngăn A ghi vào resource trong lúc đó.
	// Muốn chặn ghi muộn phải có fencing token ở chính resource (xem demo và README).
	if mustRelease(t, e, key, tokenA) {
		t.Fatal("A release muộn phải trả false")
	}
}

func TestDelexIfeqReleasesWithoutLuaOnRedis84Plus(t *testing.T) {
	// DELEX ... IFEQ có từ Redis 8.4: so sánh rồi xóa trong một lệnh, không cần Lua.
	e := newEnv(t, "lock-delex")
	ctx := context.Background()
	info, err := e.rdb.Do(ctx, "COMMAND", "INFO", "DELEX").Slice()
	if err != nil || len(info) == 0 || info[0] == nil {
		t.Skip("server không có lệnh DELEX (cần Redis 8.4+)")
	}

	key := e.key("lock")
	token, ok := mustAcquire(t, e, key, 30*time.Second)
	if !ok {
		t.Fatal("lần acquire đầu phải thành công")
	}
	if n, err := e.rdb.Do(ctx, "DELEX", key, "IFEQ", "not-the-token").Int(); err != nil || n != 0 {
		t.Fatalf("DELEX với token sai = %d, %v; mong đợi 0", n, err)
	}
	if got := get(t, e, key); got != token {
		t.Fatalf("lock bị đổi: %q", got)
	}
	if n, err := e.rdb.Do(ctx, "DELEX", key, "IFEQ", token).Int(); err != nil || n != 1 {
		t.Fatalf("DELEX với token đúng = %d, %v; mong đợi 1", n, err)
	}
	if exists(t, e, key) {
		t.Fatal("lock phải biến mất sau DELEX")
	}
}

func TestInvalidLockTTLIsRejectedWithAClearError(t *testing.T) {
	e := newEnv(t, "lock-invalid")
	key := e.key("lock")
	for _, ttl := range []time.Duration{0, -time.Second, 1500 * time.Microsecond} {
		if _, _, err := AcquireLock(context.Background(), e.rdb, key, ttl); !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("ttl %s: err = %v, mong đợi ErrInvalidArgument", ttl, err)
		}
	}
	if exists(t, e, key) {
		t.Fatal("tham số sai không được tạo key")
	}
}

// gate là kênh mà loader chờ; test đóng nó để thả loader. Đóng nhiều lần an toàn nhờ sync.Once.
type gate struct {
	ch   chan struct{}
	once sync.Once
}

func newGate(t *testing.T) *gate {
	g := &gate{ch: make(chan struct{})}
	// Cleanup luôn mở gate, để loader còn đang chờ không bị treo khi test fail giữa chừng.
	t.Cleanup(g.open)
	return g
}

func (g *gate) open() { g.once.Do(func() { close(g.ch) }) }

// countingLoader trả về loader đếm số lần được gọi, có thể bị giữ lại bởi gate.
func countingLoader[T any](value T, g *gate) (*atomic.Int64, func(context.Context) (T, error)) {
	var calls atomic.Int64
	return &calls, func(ctx context.Context) (T, error) {
		calls.Add(1)
		if g != nil {
			select {
			case <-g.ch:
			case <-ctx.Done():
				var zero T
				return zero, ctx.Err()
			}
		}
		return value, nil
	}
}

type user struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

// callers là n lời gọi đồng thời đã được khởi chạy; kết quả của lời gọi i nằm ở chỉ số i.
type callers[T any] struct {
	values []T
	errs   []error
	wg     sync.WaitGroup
}

// startCallers khởi chạy n goroutine, thả chúng cùng một lúc rồi trả về ngay (không chờ).
// Goroutine chỉ ghi vào ô riêng của mình; wait mới đọc, sau wg.Wait.
func startCallers[T any](n int, call func(i int) (T, error)) *callers[T] {
	c := &callers[T]{values: make([]T, n), errs: make([]error, n)}
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			<-start
			c.values[i], c.errs[i] = call(i)
		}()
	}
	close(start)
	return c
}

// wait chờ mọi lời gọi xong (có giới hạn thời gian) và fail test nếu có lời gọi trả lỗi.
// Gọi từ goroutine của test, nên được phép dùng Fatalf.
func (c *callers[T]) wait(t *testing.T) []T {
	t.Helper()
	waitFor(t, &c.wg, 15*time.Second)
	for i, err := range c.errs {
		if err != nil {
			t.Fatalf("caller %d lỗi: %v", i, err)
		}
	}
	return c.values
}

func waitCalls(t *testing.T, calls *atomic.Int64, want int64) {
	t.Helper()
	testkit.Eventually(t, 10*time.Second, func() (struct{}, bool) {
		return struct{}{}, calls.Load() >= want
	})
}

func TestHundredConcurrentCacheMissesCallLoaderOnce(t *testing.T) {
	e := newEnv(t, "cache-100")
	key := e.key("cache")
	cache := NewCacheAside[user](e.rdb)
	g := newGate(t)
	calls, loader := countingLoader(user{ID: 7, Name: "Linh"}, g)

	// 100 goroutine đồng thời trên key lạnh. Loader bị giữ ở gate tới khi có ít nhất một lời gọi;
	// goroutine nào vào muộn sau khi load xong sẽ thấy giá trị trong Redis, nên loader vẫn chỉ chạy một lần.
	running := startCallers(100, func(int) (user, error) {
		return cache.Get(context.Background(), key, time.Minute, loader)
	})
	waitCalls(t, calls, 1)
	g.open()
	values := running.wait(t)

	if calls.Load() != 1 {
		t.Fatalf("loader được gọi %d lần, mong đợi 1", calls.Load())
	}
	for i, v := range values {
		if v != (user{ID: 7, Name: "Linh"}) {
			t.Fatalf("caller %d nhận %+v", i, v)
		}
	}
	// Giá trị đã nằm trong Redis dạng JSON và có TTL.
	if got := get(t, e, key); got != `{"id":7,"name":"Linh"}` {
		t.Fatalf("giá trị trong Redis = %s", got)
	}
	if pttl, err := e.rdb.PTTL(context.Background(), key).Result(); err != nil || pttl <= 0 || pttl > time.Minute {
		t.Fatalf("PTTL = %s, %v; mong đợi trong (0, 1m]", pttl, err)
	}
}

func TestWithoutProtectionEveryConcurrentMissCallsLoader(t *testing.T) {
	// Đối chứng: cùng kịch bản nhưng không có singleflight, mỗi caller miss rồi tự gọi loader.
	e := newEnv(t, "cache-unprotected")
	key := e.key("cache")
	g := newGate(t)
	calls, loader := countingLoader("v", g)

	running := startCallers(100, func(int) (string, error) {
		return GetUnprotected(context.Background(), e.rdb, key, time.Minute, loader)
	})
	// Loader bị giữ ở gate nên chưa có SET nào: cả 100 goroutine đều miss và gọi loader.
	waitCalls(t, calls, 100)
	g.open()
	running.wait(t)
	if calls.Load() != 100 {
		t.Fatalf("loader được gọi %d lần, mong đợi 100", calls.Load())
	}
}

func TestCacheHitDoesNotCallLoader(t *testing.T) {
	e := newEnv(t, "cache-hit")
	key := e.key("cache")
	ctx := context.Background()
	cache := NewCacheAside[string](e.rdb)
	calls, loader := countingLoader("giá trị", nil)
	for i := 0; i < 2; i++ {
		got, err := cache.Get(ctx, key, time.Minute, loader)
		if err != nil || got != "giá trị" {
			t.Fatalf("Get = %q, %v", got, err)
		}
	}
	// Một instance khác (singleflight.Group riêng) cũng thấy giá trị trong Redis.
	got, err := NewCacheAside[string](e.rdb).Get(ctx, key, time.Minute, loader)
	if err != nil || got != "giá trị" {
		t.Fatalf("instance khác: Get = %q, %v", got, err)
	}
	if calls.Load() != 1 {
		t.Fatalf("loader được gọi %d lần, mong đợi 1", calls.Load())
	}
}

func TestTTLExpiryMakesTheNextCallReload(t *testing.T) {
	e := newEnv(t, "cache-ttl")
	key := e.key("cache")
	ctx := context.Background()
	cache := NewCacheAside[int](e.rdb)
	// EX tính bằng giây nguyên, nên TTL nhỏ nhất của API này là 1 giây.
	ttl := time.Second
	var version atomic.Int64
	loader := func(context.Context) (int, error) { return int(version.Add(1)), nil }

	start := time.Now()
	for i := 0; i < 2; i++ {
		if got, err := cache.Get(ctx, key, ttl, loader); err != nil || got != 1 {
			t.Fatalf("Get = %d, %v; mong đợi 1", got, err)
		}
	}
	// Poll bằng chính Get cho tới khi TTL hết và loader được gọi lại.
	testkit.Eventually(t, 5*time.Second, func() (int, bool) {
		got, err := cache.Get(ctx, key, ttl, loader)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		return got, got == 2
	})
	if elapsed := time.Since(start); elapsed < ttl-clockSlack {
		t.Fatalf("reload sau %s, sớm hơn TTL %s", elapsed, ttl)
	}
}

func TestLoaderFailureIsNotCachedAndNextCallRetries(t *testing.T) {
	e := newEnv(t, "cache-fail")
	key := e.key("cache")
	ctx := context.Background()
	cache := NewCacheAside[string](e.rdb)
	g := newGate(t)
	boom := errors.New("DB sập")
	var calls atomic.Int64
	failing := func(context.Context) (string, error) {
		calls.Add(1)
		<-g.ch
		return "", boom
	}

	// 5 waiter chung một lần load: cả 5 nhận cùng một lỗi, loader chỉ chạy một lần.
	running := startCallers(5, func(int) (string, error) {
		return cache.Get(ctx, key, time.Minute, failing)
	})
	waitCalls(t, &calls, 1)
	g.open()
	waitFor(t, &running.wg, 15*time.Second)
	for i, err := range running.errs {
		if !errors.Is(err, boom) {
			t.Fatalf("waiter %d: err = %v, mong đợi lỗi gốc", i, err)
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("loader được gọi %d lần, mong đợi 1", calls.Load())
	}
	// Lỗi không bị cache: Redis không có key.
	if exists(t, e, key) {
		t.Fatal("lỗi của loader không được cache")
	}

	// Lần gọi sau thử lại và thành công.
	got, err := cache.Get(ctx, key, time.Minute, func(context.Context) (string, error) { return "đã ổn", nil })
	if err != nil || got != "đã ổn" {
		t.Fatalf("lần thử lại: %q, %v", got, err)
	}
	if !exists(t, e, key) {
		t.Fatal("giá trị thành công phải được cache")
	}
}

func TestConcurrentCallersOnTwoKeysCallLoaderOncePerKey(t *testing.T) {
	e := newEnv(t, "cache-two")
	keyA, keyB := e.key("a"), e.key("b")
	cache := NewCacheAside[string](e.rdb)
	g := newGate(t)
	callsA, loaderA := countingLoader("A", g)
	callsB, loaderB := countingLoader("B", g)

	// 20 goroutine cho mỗi key, đồng thời: chỉ số chẵn dùng key A, chỉ số lẻ dùng key B.
	running := startCallers(40, func(i int) (string, error) {
		if i%2 == 0 {
			return cache.Get(context.Background(), keyA, time.Minute, loaderA)
		}
		return cache.Get(context.Background(), keyB, time.Minute, loaderB)
	})
	waitCalls(t, callsA, 1)
	waitCalls(t, callsB, 1)
	g.open()
	values := running.wait(t)
	for i, v := range values {
		want := "A"
		if i%2 == 1 {
			want = "B"
		}
		if v != want {
			t.Fatalf("caller %d nhận %q, mong đợi %q", i, v, want)
		}
	}
	if callsA.Load() != 1 || callsB.Load() != 1 {
		t.Fatalf("số lần gọi loader: A = %d, B = %d, mong đợi 1 và 1", callsA.Load(), callsB.Load())
	}
}

func TestNilValueIsCachedAndDistinguishedFromAMiss(t *testing.T) {
	e := newEnv(t, "cache-null")
	key := e.key("cache")
	ctx := context.Background()
	cache := NewCacheAside[*string](e.rdb)
	var calls atomic.Int64
	loader := func(context.Context) (*string, error) {
		calls.Add(1)
		return nil, nil
	}
	got, err := cache.Get(ctx, key, time.Minute, loader)
	if err != nil || got != nil {
		t.Fatalf("Get = %v, %v; mong đợi nil", got, err)
	}
	// Miss là key không tồn tại (redis.Nil), còn nil đã cache là chuỗi JSON "null".
	if raw := get(t, e, key); raw != "null" {
		t.Fatalf("giá trị trong Redis = %q, mong đợi \"null\"", raw)
	}
	got, err = cache.Get(ctx, key, time.Minute, loader)
	if err != nil || got != nil {
		t.Fatalf("Get lần hai = %v, %v; mong đợi nil", got, err)
	}
	if calls.Load() != 1 {
		t.Fatalf("loader được gọi %d lần, mong đợi 1", calls.Load())
	}
}

func TestInvalidCacheTTLIsRejectedWithAClearError(t *testing.T) {
	e := newEnv(t, "cache-invalid")
	key := e.key("cache")
	cache := NewCacheAside[string](e.rdb)
	loader := func(context.Context) (string, error) { return "x", nil }
	for _, ttl := range []time.Duration{0, -time.Second, 1500 * time.Millisecond, 500 * time.Millisecond} {
		if _, err := cache.Get(context.Background(), key, ttl, loader); !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("ttl %s: err = %v, mong đợi ErrInvalidArgument", ttl, err)
		}
	}
	if exists(t, e, key) {
		t.Fatal("tham số sai không được tạo key")
	}
}
