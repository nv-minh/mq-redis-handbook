// Package lab finds big keys with SCAN and MEMORY USAGE, the same idea as redis-cli --memkeys,
// without changing any server configuration.
package lab

import (
	"context"
	"sort"

	"github.com/redis/go-redis/v9"
)

// FindBigKeys returns the keys whose memory footprint is at least thresholdBytes, biggest first.
//
// It walks the keyspace with SCAN (cursor based, never blocks Redis the way KEYS does) and asks
// MEMORY USAGE for the keys of each page in one pipeline. match is a glob that SCAN applies on
// the server, "" or "*" scans every key. Nothing is deleted and no configuration is changed.
//
// MEMORY USAGE counts the key, its value and allocator overhead, so a key is "big" by bytes
// here, not by element count (which is what redis-cli --bigkeys reports).
func FindBigKeys(ctx context.Context, rdb redis.UniversalClient, thresholdBytes int64, match string) ([]string, error) {
	if match == "" {
		match = "*"
	}
	sizes := map[string]int64{}
	var cursor uint64
	for {
		keys, next, err := rdb.Scan(ctx, cursor, match, 200).Result()
		if err != nil {
			return nil, err
		}
		if len(keys) > 0 {
			cmds, err := rdb.Pipelined(ctx, func(pipe redis.Pipeliner) error {
				for _, key := range keys {
					pipe.MemoryUsage(ctx, key)
				}
				return nil
			})
			// redis.Nil: a key expired or was deleted between SCAN and MEMORY USAGE.
			if err != nil && err != redis.Nil {
				return nil, err
			}
			for i, cmd := range cmds {
				bytes, err := cmd.(*redis.IntCmd).Result()
				if err == nil && bytes >= thresholdBytes {
					sizes[keys[i]] = bytes
				}
			}
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}
	found := make([]string, 0, len(sizes))
	for key := range sizes {
		found = append(found, key)
	}
	sort.Slice(found, func(i, j int) bool {
		if sizes[found[i]] != sizes[found[j]] {
			return sizes[found[i]] > sizes[found[j]]
		}
		return found[i] < found[j]
	})
	return found, nil
}
