// Package lab tạm thời đổi maxmemory và maxmemory-policy để xem Redis evict key
// (allkeys-lru) hoặc từ chối lệnh ghi (noeviction).
package lab

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strconv"

	"github.com/redis/go-redis/v9"
)

const batch = 50

// Config giữ ba setting mà lab này thay đổi, dưới dạng chuỗi mà CONFIG GET trả về.
type Config struct {
	Maxmemory string
	Policy    string
	Samples   string
}

func configValue(ctx context.Context, rdb *redis.Client, name string) (string, error) {
	// go-redis giải mã reply dạng map RESP3 của CONFIG GET thành map[string]string.
	m, err := rdb.ConfigGet(ctx, name).Result()
	if err != nil {
		return "", err
	}
	v, ok := m[name]
	if !ok {
		return "", fmt.Errorf("CONFIG GET %s trả về %v", name, m)
	}
	return v, nil
}

// ReadConfig đọc các setting hiện tại. Gọi hàm này TRƯỚC lần CONFIG SET đầu tiên để còn có cái mà khôi phục.
func ReadConfig(ctx context.Context, rdb *redis.Client) (Config, error) {
	var c Config
	var err error
	if c.Maxmemory, err = configValue(ctx, rdb, "maxmemory"); err != nil {
		return c, err
	}
	if c.Policy, err = configValue(ctx, rdb, "maxmemory-policy"); err != nil {
		return c, err
	}
	c.Samples, err = configValue(ctx, rdb, "maxmemory-samples")
	return c, err
}

// WriteConfig áp dụng setting. Policy được đặt trước, để maxmemory thấp hơn không bao giờ
// bị áp dụng với policy cũ.
func WriteConfig(ctx context.Context, rdb *redis.Client, c Config) error {
	if err := rdb.ConfigSet(ctx, "maxmemory-policy", c.Policy).Err(); err != nil {
		return err
	}
	if err := rdb.ConfigSet(ctx, "maxmemory-samples", c.Samples).Err(); err != nil {
		return err
	}
	return rdb.ConfigSet(ctx, "maxmemory", c.Maxmemory).Err()
}

func infoNumber(ctx context.Context, rdb *redis.Client, section, field string) (int64, error) {
	info, err := rdb.Info(ctx, section).Result()
	if err != nil {
		return 0, err
	}
	m := regexp.MustCompile(`(?m)^` + field + `:(\d+)`).FindStringSubmatch(info)
	if m == nil {
		return 0, fmt.Errorf("INFO %s không có %s", section, field)
	}
	return strconv.ParseInt(m[1], 10, 64)
}

// UsedMemory trả về used_memory từ INFO memory, tính bằng byte.
func UsedMemory(ctx context.Context, rdb *redis.Client) (int64, error) {
	return infoNumber(ctx, rdb, "memory", "used_memory")
}

// EvictedKeys trả về số key mà server đã evict kể từ lúc khởi động (INFO stats evicted_keys).
func EvictedKeys(ctx context.Context, rdb *redis.Client) (int64, error) {
	return infoNumber(ctx, rdb, "stats", "evicted_keys")
}

// LimitMemory giới hạn bộ nhớ theo mức đang dùng ngay lúc này, để lab chạy được trên mọi baseline:
// maxmemory = used_memory + headroomBytes. samples=10 làm LRU xấp xỉ gần với LRU thật hơn.
// Hàm trả về maxmemory đã đặt, tính bằng byte.
func LimitMemory(ctx context.Context, rdb *redis.Client, policy string, headroomBytes int64) (int64, error) {
	used, err := UsedMemory(ctx, rdb)
	if err != nil {
		return 0, err
	}
	maxmemory := used + headroomBytes
	err = WriteConfig(ctx, rdb, Config{Maxmemory: strconv.FormatInt(maxmemory, 10), Policy: policy, Samples: "10"})
	return maxmemory, err
}

func value(size int) string {
	b := make([]byte, size)
	for i := range b {
		b[i] = 'x'
	}
	return string(b)
}

// writeBatch gửi SET cho các key prefix:start .. prefix:end-1 qua pipeline và trả về lỗi đầu tiên của từng lệnh.
func writeBatch(ctx context.Context, rdb *redis.Client, prefix string, start, end int, val string) error {
	cmds, err := rdb.Pipelined(ctx, func(p redis.Pipeliner) error {
		for i := start; i < end; i++ {
			p.Set(ctx, prefix+":"+strconv.Itoa(i), val, 0)
		}
		return nil
	})
	if err != nil {
		for _, c := range cmds {
			if c.Err() != nil {
				return c.Err()
			}
		}
		return err
	}
	return nil
}

// SeedKeys ghi count key prefix:0 .. prefix:count-1, mỗi key chứa valueSize byte.
func SeedKeys(ctx context.Context, rdb *redis.Client, prefix string, count, valueSize int) error {
	val := value(valueSize)
	for start := 0; start < count; start += batch {
		if err := writeBatch(ctx, rdb, prefix, start, min(start+batch, count), val); err != nil {
			return err
		}
	}
	return nil
}

// FillOptions tinh chỉnh FillUntilEviction. Giá trị zero nghĩa là: value 1024 byte, không có touch key, dừng sau 1 lần evict.
type FillOptions struct {
	ValueSize  int    // số byte của mỗi value, mặc định 1024
	TouchKey   string // một key được GET sau mỗi batch, để nó luôn "vừa được dùng"
	MinEvicted int64  // dừng khi đã có ít nhất chừng này key bị evict, mặc định 1
}

// FillUntilEviction ghi liên tục prefix:0 .. (tối đa maxKeys key, theo batch 50) cho tới khi
// server đã evict ít nhất MinEvicted key. Hàm trả về số key bị evict trong lúc gọi
// (độ chênh của evicted_keys), 0 nếu chưa bao giờ chạm giới hạn.
func FillUntilEviction(ctx context.Context, rdb *redis.Client, prefix string, maxKeys int, opts FillOptions) (int64, error) {
	if opts.ValueSize == 0 {
		opts.ValueSize = 1024
	}
	if opts.MinEvicted == 0 {
		opts.MinEvicted = 1
	}
	val := value(opts.ValueSize)
	before, err := EvictedKeys(ctx, rdb)
	if err != nil {
		return 0, err
	}
	for start := 0; start < maxKeys; start += batch {
		if err := writeBatch(ctx, rdb, prefix, start, min(start+batch, maxKeys), val); err != nil {
			return 0, err
		}
		if opts.TouchKey != "" {
			if err := rdb.Get(ctx, opts.TouchKey).Err(); err != nil {
				return 0, err
			}
		}
		now, err := EvictedKeys(ctx, rdb)
		if err != nil {
			return 0, err
		}
		if now-before >= opts.MinEvicted {
			return now - before, nil
		}
	}
	now, err := EvictedKeys(ctx, rdb)
	return now - before, err
}

// FillUntilRejected ghi liên tục prefix:0 .. cho tới khi server từ chối một lần ghi. Hàm trả về
// message lỗi của lần ghi đầu tiên bị từ chối, hoặc "" nếu cả maxKeys lần ghi đều thành công.
// Các key ghi trước lúc bị từ chối vẫn còn.
func FillUntilRejected(ctx context.Context, rdb *redis.Client, prefix string, maxKeys, valueSize int) (string, error) {
	val := value(valueSize)
	for start := 0; start < maxKeys; start += batch {
		if err := writeBatch(ctx, rdb, prefix, start, min(start+batch, maxKeys), val); err != nil {
			if _, isRedisErr := err.(redis.Error); isRedisErr {
				return err.Error(), nil
			}
			return "", err
		}
	}
	return "", nil
}

// DeleteByPrefix xóa mọi key bắt đầu bằng prefix+":", dùng SCAN (không bao giờ dùng KEYS) và UNLINK.
func DeleteByPrefix(ctx context.Context, rdb *redis.Client, prefix string) error {
	iter := rdb.Scan(ctx, 0, prefix+":*", 1000).Iterator()
	var keys []string
	flush := func() error {
		if len(keys) == 0 {
			return nil
		}
		err := rdb.Unlink(ctx, keys...).Err()
		keys = keys[:0]
		return err
	}
	for iter.Next(ctx) {
		keys = append(keys, iter.Val())
		if len(keys) >= 500 {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	if err := iter.Err(); err != nil {
		return err
	}
	return flush()
}

// OwnRedisMarker là dbfilename mà infra/docker-compose.yml đặt cho Redis riêng của handbook.
const OwnRedisMarker = "mq-handbook.rdb"

// ServerState là những gì guard đọc được từ server.
type ServerState struct {
	DBFilename string
	Maxmemory  string
	Policy     string
}

// GuardProblem là phép kiểm tra thuần: vì sao lab không được dùng server này, hoặc "" khi an toàn.
func GuardProblem(s ServerState, marker string) string {
	if s.DBFilename != marker {
		return fmt.Sprintf("refusing to run: this Redis reports dbfilename %q, not the handbook marker %q, so it is not the compose Redis of this repo (REDIS_URL points elsewhere?). Nothing was changed.", s.DBFilename, marker)
	}
	if s.Maxmemory != "0" || s.Policy != "noeviction" {
		return fmt.Sprintf("refusing to run: maxmemory=%s policy=%s is not pristine (maxmemory 0, noeviction). Leftover config from an earlier run, run `make down && make up`. Nothing was changed.", s.Maxmemory, s.Policy)
	}
	return ""
}

// AssertOwnRedis trả về error trừ khi server là Redis riêng của handbook ở trạng thái nguyên vẹn.
// Hàm chỉ đọc (CONFIG GET): gọi trước lần CONFIG SET đầu tiên, và không bao giờ khôi phục sau khi bị từ chối.
func AssertOwnRedis(ctx context.Context, rdb *redis.Client, marker string) error {
	var s ServerState
	var err error
	if s.DBFilename, err = configValue(ctx, rdb, "dbfilename"); err != nil {
		return err
	}
	if s.Maxmemory, err = configValue(ctx, rdb, "maxmemory"); err != nil {
		return err
	}
	if s.Policy, err = configValue(ctx, rdb, "maxmemory-policy"); err != nil {
		return err
	}
	if msg := GuardProblem(s, marker); msg != "" {
		return errors.New(msg)
	}
	return nil
}
