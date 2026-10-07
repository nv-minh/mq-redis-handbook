// Package lab gom ba mảnh độc lập dùng Redis làm hạ tầng: rate limiter sliding window,
// distributed lock trên một node và cache-aside có chống stampede bằng singleflight.
// Không có framework và không có HTTP: mỗi mảnh là vài hàm nhỏ nhận một client go-redis.
package lab

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/singleflight"
)

// ErrInvalidArgument được trả về (bọc) khi tham số sai: limit, window, ttl.
var ErrInvalidArgument = errors.New("tham số không hợp lệ")

func invalid(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidArgument, fmt.Sprintf(format, args...))
}

// requireWholeMillis kiểm tra d dương và là số mili giây nguyên (Redis PX/PEXPIRE nhận mili giây nguyên).
func requireWholeMillis(name string, d time.Duration) error {
	if d <= 0 || d%time.Millisecond != 0 {
		return invalid("%s phải là số mili giây nguyên dương, nhận được %s", name, d)
	}
	return nil
}

// slidingWindowScript: read - decide - write phải chạy nguyên tử, nên cả bốn bước nằm trong một script:
// bỏ entry cũ, đếm, nếu còn chỗ thì ghi entry mới và đặt TTL.
//
//   - Thời gian lấy từ TIME của SERVER, nên client lệch đồng hồ vẫn đồng ý với nhau.
//     Score là micro giây (khoảng 1.8e15, nhỏ hơn 2^53 nên double biểu diễn chính xác).
//   - Entry nằm trong cửa sổ khi tuổi của nó nhỏ hơn window, nên entry đúng bằng now - window bị bỏ.
//   - Member là chuỗi ngẫu nhiên do client sinh (ARGV[3]), nên hai request trong cùng một micro giây
//     vẫn là hai entry.
//   - Request bị từ chối KHÔNG ghi gì, nên cửa sổ không bị đẩy lùi vì những lần thử bị từ chối.
//   - PEXPIRE theo window: mọi entry trong key đều già hơn window sau thời gian đó, key idle tự biến mất.
var slidingWindowScript = redis.NewScript(`
local time = redis.call('TIME')
local now = tonumber(time[1]) * 1000000 + tonumber(time[2])
local limit = tonumber(ARGV[1])
local windowMs = tonumber(ARGV[2])
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now - windowMs * 1000)
if redis.call('ZCARD', KEYS[1]) < limit then
  redis.call('ZADD', KEYS[1], now, ARGV[3])
  redis.call('PEXPIRE', KEYS[1], windowMs)
  return 1
end
return 0
`)

// releaseLockScript là compare-and-delete: chỉ xóa lock khi value đúng bằng token của caller.
// Từ Redis 8.4 có thể thay bằng một lệnh DELEX key IFEQ token, không cần Lua.
var releaseLockScript = redis.NewScript(`
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
`)

// randomHex trả về 128 bit crypto-random dạng hex (32 ký tự).
func randomHex() (string, error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}

// SlidingWindowAllow cho phép tối đa limit request trong BẤT KỲ khoảng window nào của key.
// Trả true khi request được chấp nhận (và được ghi lại), false khi bị từ chối (không ghi gì).
// limit = 0 luôn từ chối. limit âm, window <= 0 hoặc window không phải số mili giây nguyên
// trả lỗi bọc ErrInvalidArgument.
//
// Bộ nhớ là O(limit) cho mỗi key, vì sorted set giữ từng request trong cửa sổ.
// redis.Script gửi EVALSHA và chuyển sang EVAL khi gặp NOSCRIPT.
func SlidingWindowAllow(ctx context.Context, rdb *redis.Client, key string, limit int, window time.Duration) (bool, error) {
	if limit < 0 {
		return false, invalid("limit phải >= 0, nhận được %d", limit)
	}
	if err := requireWholeMillis("window", window); err != nil {
		return false, err
	}
	member, err := randomHex()
	if err != nil {
		return false, err
	}
	n, err := slidingWindowScript.Run(ctx, rdb, []string{key}, limit, window.Milliseconds(), member).Int()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

// SlidingWindowAllowNaive là phiên bản sai, chỉ dùng cho demo: đếm rồi mới ghi bằng các lệnh riêng lẻ
// và dùng đồng hồ của client. Hai caller cùng đọc count < limit rồi cùng ghi, nên số request được
// chấp nhận vượt limit.
func SlidingWindowAllowNaive(ctx context.Context, rdb *redis.Client, key string, limit int, window time.Duration) (bool, error) {
	now := time.Now().UnixMilli()
	if err := rdb.ZRemRangeByScore(ctx, key, "-inf", fmt.Sprint(now-window.Milliseconds())).Err(); err != nil {
		return false, err
	}
	count, err := rdb.ZCard(ctx, key).Result()
	if err != nil {
		return false, err
	}
	if count >= int64(limit) {
		return false, nil
	}
	member, err := randomHex()
	if err != nil {
		return false, err
	}
	if err := rdb.ZAdd(ctx, key, redis.Z{Score: float64(now), Member: member}).Err(); err != nil {
		return false, err
	}
	return true, rdb.PExpire(ctx, key, window).Err()
}

// AcquireLock lấy lock bằng SET key token NX PX ttl. Trả token (128 bit crypto-random, hex) và ok = true
// nếu lấy được, ok = false nếu lock đang do người khác giữ. ttl không phải số mili giây nguyên dương
// trả lỗi bọc ErrInvalidArgument.
//
// Lock chỉ đúng trong ttl: sau đó Redis tự xóa key và người khác có thể lấy lock, kể cả khi holder cũ
// vẫn đang làm việc (GC pause, network delay). Đây là lock cho EFFICIENCY, không phải CORRECTNESS.
func AcquireLock(ctx context.Context, rdb *redis.Client, key string, ttl time.Duration) (token string, ok bool, err error) {
	if err := requireWholeMillis("ttl", ttl); err != nil {
		return "", false, err
	}
	token, err = randomHex()
	if err != nil {
		return "", false, err
	}
	err = rdb.SetArgs(ctx, key, token, redis.SetArgs{Mode: "NX", TTL: ttl}).Err()
	if errors.Is(err, redis.Nil) {
		return "", false, nil // key đã tồn tại: lock đang do người khác giữ
	}
	if err != nil {
		return "", false, err
	}
	return token, true, nil
}

// ReleaseLock nhả lock chỉ khi token còn là value của key. Trả true nếu đã xóa, false nếu lock đã
// hết hạn hoặc đã thuộc người khác (khi đó không xóa gì). Chỉ dùng DEL sẽ xóa nhầm lock của người khác.
func ReleaseLock(ctx context.Context, rdb *redis.Client, key, token string) (bool, error) {
	n, err := releaseLockScript.Run(ctx, rdb, []string{key}, token).Int()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

// readThrough đọc cache, miss thì gọi loader rồi ghi lại dạng JSON với TTL.
//
//   - Redis lỗi thì lỗi được trả ra (đơn giản và dễ kiểm chứng). Production có thể fall back sang loader.
//   - Giá trị nil (T là con trỏ, map hoặc slice) được cache: value trong Redis là chuỗi JSON "null",
//     còn miss là key không tồn tại (redis.Nil), nên hai trường hợp không lẫn nhau.
//   - ttl chỉ nhận số giây nguyên (EX), để TS và Go cùng một ngữ nghĩa.
func readThrough[T any](ctx context.Context, rdb *redis.Client, key string, ttl time.Duration, loader func(context.Context) (T, error)) (T, error) {
	var zero T
	if ttl <= 0 || ttl%time.Second != 0 {
		return zero, invalid("ttl phải là số giây nguyên dương, nhận được %s", ttl)
	}
	cached, err := rdb.Get(ctx, key).Result()
	if err == nil {
		var value T
		if err := json.Unmarshal([]byte(cached), &value); err != nil {
			return zero, fmt.Errorf("giá trị cache của %s không giải mã được: %w", key, err)
		}
		return value, nil
	}
	if !errors.Is(err, redis.Nil) {
		return zero, err
	}
	value, err := loader(ctx)
	if err != nil {
		return zero, err // lỗi của loader không được cache
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return zero, fmt.Errorf("giá trị của %s không mã hóa được thành JSON: %w", key, err)
	}
	if err := rdb.Set(ctx, key, encoded, ttl).Err(); err != nil {
		return zero, err
	}
	return value, nil
}

// GetUnprotected là phiên bản không chống stampede, chỉ dùng cho demo và test đối chứng:
// mỗi caller miss đều tự gọi loader.
func GetUnprotected[T any](ctx context.Context, rdb *redis.Client, key string, ttl time.Duration, loader func(context.Context) (T, error)) (T, error) {
	return readThrough(ctx, rdb, key, ttl, loader)
}

// CacheAside là cache-aside với singleflight: trong một process, mọi lời gọi trùng key trong lúc một
// lần load đang chạy đều chờ lần đó và nhận cùng kết quả (kể cả lỗi).
// Giới hạn: chỉ dedup trong MỘT process. N instance vẫn là N lần load cho một key lạnh.
type CacheAside[T any] struct {
	rdb   *redis.Client
	group singleflight.Group
}

// NewCacheAside tạo cache dùng client rdb. Mỗi instance có singleflight.Group riêng.
func NewCacheAside[T any](rdb *redis.Client) *CacheAside[T] {
	return &CacheAside[T]{rdb: rdb}
}

// Get trả giá trị của key từ Redis, hoặc gọi loader đúng một lần cho mọi caller đồng thời rồi ghi lại.
//
// Cả đọc cache, load và ghi cache nằm trong singleflight.Do, nên caller đến muộn hoặc nhập vào lần
// load đang chạy, hoặc thấy giá trị trong Redis: không có khe hở nào cho lần load thứ hai.
// Loader lỗi thì mọi waiter nhận cùng lỗi, không có gì được cache và lần gọi sau thử lại.
//
// Cẩn thận: loader chạy với ctx của caller ĐẦU TIÊN. Nếu caller đó hủy ctx, mọi waiter cùng nhận
// lỗi hủy. Các waiter cũng chia sẻ cùng một giá trị, nên với T là con trỏ thì đừng sửa nó.
func (c *CacheAside[T]) Get(ctx context.Context, key string, ttl time.Duration, loader func(context.Context) (T, error)) (T, error) {
	v, err, _ := c.group.Do(key, func() (any, error) {
		return readThrough(ctx, c.rdb, key, ttl, loader)
	})
	// Comma-ok: nếu T là interface và loader trả nil thì v là nil và v.(T) sẽ panic.
	value, _ := v.(T)
	return value, err
}
