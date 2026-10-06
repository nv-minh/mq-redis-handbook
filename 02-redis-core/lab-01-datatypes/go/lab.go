// Package lab là bảng xếp hạng trên một Redis sorted set.
package lab

import (
	"context"

	"github.com/redis/go-redis/v9"
)

// Entry là một dòng của bảng xếp hạng.
type Entry struct {
	Name  string
	Score float64
}

// Leaderboard giữ điểm của người chơi trong một sorted set: member = tên, score = điểm.
type Leaderboard struct {
	rdb *redis.Client
	key string
}

// NewLeaderboard trả về bảng xếp hạng lưu dưới key.
func NewLeaderboard(rdb *redis.Client, key string) *Leaderboard {
	return &Leaderboard{rdb: rdb, key: key}
}

// Add đặt score của name. Thêm một tên đã có sẽ thay score của nó (không sinh member trùng).
func (l *Leaderboard) Add(ctx context.Context, name string, score float64) error {
	return l.rdb.ZAdd(ctx, l.key, redis.Z{Score: score, Member: name}).Err()
}

// Top trả về tên của n điểm cao nhất, cao nhất trước.
// Hàm trả về slice rỗng khác nil nếu bảng rỗng hoặc n <= 0.
func (l *Leaderboard) Top(ctx context.Context, n int) ([]string, error) {
	// ZRANGE key 0 -1 REV nghĩa là "tất cả", nên n không dương không được gửi tới Redis.
	if n <= 0 {
		return []string{}, nil
	}
	names, err := l.rdb.ZRangeArgs(ctx, redis.ZRangeArgs{Key: l.key, Start: 0, Stop: int64(n - 1), Rev: true}).Result()
	if err != nil {
		return nil, err
	}
	if names == nil {
		names = []string{}
	}
	return names, nil
}

// TopWithScores là Top kèm score. go-redis tự giải mã reply RESP3 (mảng lồng các cặp
// [member, score], score là double) thành []redis.Z.
func (l *Leaderboard) TopWithScores(ctx context.Context, n int) ([]Entry, error) {
	if n <= 0 {
		return []Entry{}, nil
	}
	zs, err := l.rdb.ZRangeArgsWithScores(ctx, redis.ZRangeArgs{Key: l.key, Start: 0, Stop: int64(n - 1), Rev: true}).Result()
	if err != nil {
		return nil, err
	}
	entries := make([]Entry, 0, len(zs))
	for _, z := range zs {
		entries = append(entries, Entry{Name: z.Member.(string), Score: z.Score})
	}
	return entries, nil
}
