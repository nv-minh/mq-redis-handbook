// Package lab tìm big key bằng SCAN và MEMORY USAGE, cùng ý tưởng với redis-cli --memkeys,
// mà không đổi bất kỳ cấu hình server nào.
package lab

import (
	"context"
	"sort"

	"github.com/redis/go-redis/v9"
)

// FindBigKeys trả về các key có kích thước bộ nhớ từ thresholdBytes trở lên, key lớn nhất đứng đầu.
//
// Hàm duyệt keyspace bằng SCAN (theo cursor, không chặn Redis như KEYS) và hỏi MEMORY USAGE
// cho các key của mỗi trang trong một pipeline.
// match là glob mà SCAN áp dụng ngay trên server, "" hoặc "*" là quét mọi key.
// Hàm không xóa gì và không đổi cấu hình server nào.
//
// MEMORY USAGE tính cả key, value và overhead của allocator, nên "big" ở đây là theo byte,
// không phải theo số phần tử (cái mà redis-cli --bigkeys báo).
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
			// redis.Nil: key đã hết hạn hoặc bị xóa giữa lúc SCAN và MEMORY USAGE, bỏ qua thay vì báo lỗi.
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
