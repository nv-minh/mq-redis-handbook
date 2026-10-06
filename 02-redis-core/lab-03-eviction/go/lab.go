// Package lab changes maxmemory and maxmemory-policy temporarily to watch Redis evict keys
// (allkeys-lru) or reject writes (noeviction).
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

// Config holds the three settings this lab changes, as the strings CONFIG GET returns.
type Config struct {
	Maxmemory string
	Policy    string
	Samples   string
}

func configValue(ctx context.Context, rdb *redis.Client, name string) (string, error) {
	// go-redis decodes the RESP3 map reply of CONFIG GET into map[string]string.
	m, err := rdb.ConfigGet(ctx, name).Result()
	if err != nil {
		return "", err
	}
	v, ok := m[name]
	if !ok {
		return "", fmt.Errorf("CONFIG GET %s returned %v", name, m)
	}
	return v, nil
}

// ReadConfig reads the current settings. Call it BEFORE the first CONFIG SET so you can put them back.
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

// WriteConfig applies settings. The policy goes first, so a lower maxmemory is never enforced
// with the old policy.
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
		return 0, fmt.Errorf("INFO %s has no %s", section, field)
	}
	return strconv.ParseInt(m[1], 10, 64)
}

// UsedMemory returns used_memory from INFO memory, in bytes.
func UsedMemory(ctx context.Context, rdb *redis.Client) (int64, error) {
	return infoNumber(ctx, rdb, "memory", "used_memory")
}

// EvictedKeys returns the number of keys the server has evicted since it started (INFO stats evicted_keys).
func EvictedKeys(ctx context.Context, rdb *redis.Client) (int64, error) {
	return infoNumber(ctx, rdb, "stats", "evicted_keys")
}

// LimitMemory caps memory relative to what is used right now, so the lab works on any baseline:
// maxmemory = used_memory + headroomBytes. samples=10 makes approximate LRU closer to real LRU.
// It returns the maxmemory it set, in bytes.
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

// writeBatch pipelines SET for keys prefix:start .. prefix:end-1 and returns the first per-command error.
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

// SeedKeys writes count keys prefix:0 .. prefix:count-1, each holding valueSize bytes.
func SeedKeys(ctx context.Context, rdb *redis.Client, prefix string, count, valueSize int) error {
	val := value(valueSize)
	for start := 0; start < count; start += batch {
		if err := writeBatch(ctx, rdb, prefix, start, min(start+batch, count), val); err != nil {
			return err
		}
	}
	return nil
}

// FillOptions tunes FillUntilEviction. Zero values mean: 1024-byte values, no touch key, stop at 1 eviction.
type FillOptions struct {
	ValueSize  int    // bytes per value, default 1024
	TouchKey   string // a key to GET after every batch, so it stays "recently used"
	MinEvicted int64  // stop once at least this many keys were evicted, default 1
}

// FillUntilEviction keeps writing prefix:0 .. (at most maxKeys keys, in batches of 50) until the
// server has evicted at least MinEvicted keys. It returns how many keys were evicted during the
// call (the delta of evicted_keys), 0 if the limit was never reached.
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

// FillUntilRejected keeps writing prefix:0 .. until the server rejects a write. It returns the
// error message of the first rejected write, or "" if all maxKeys writes succeeded.
// Keys written before the rejection stay.
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

// DeleteByPrefix deletes every key that starts with prefix+":", using SCAN (never KEYS) and UNLINK.
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

// OwnRedisMarker is the dbfilename that infra/docker-compose.yml gives the handbook's own Redis.
const OwnRedisMarker = "mq-handbook.rdb"

// ServerState is what the guard reads from the server.
type ServerState struct {
	DBFilename string
	Maxmemory  string
	Policy     string
}

// GuardProblem is a pure check: why this server must not be used by the lab, or "" when it is safe.
func GuardProblem(s ServerState, marker string) string {
	if s.DBFilename != marker {
		return fmt.Sprintf("refusing to run: this Redis reports dbfilename %q, not the handbook marker %q, so it is not the compose Redis of this repo (REDIS_URL points elsewhere?). Nothing was changed.", s.DBFilename, marker)
	}
	if s.Maxmemory != "0" || s.Policy != "noeviction" {
		return fmt.Sprintf("refusing to run: maxmemory=%s policy=%s is not pristine (maxmemory 0, noeviction). Leftover config from an earlier run, run `make down && make up`. Nothing was changed.", s.Maxmemory, s.Policy)
	}
	return ""
}

// AssertOwnRedis returns an error unless the server is the handbook's own Redis in its pristine
// state. It only reads (CONFIG GET): call it before the first CONFIG SET, and never restore after a refusal.
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
