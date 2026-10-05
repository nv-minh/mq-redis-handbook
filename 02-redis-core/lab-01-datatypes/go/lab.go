// Package lab is a leaderboard on a Redis sorted set.
package lab

import (
	"context"

	"github.com/redis/go-redis/v9"
)

// Entry is one leaderboard row.
type Entry struct {
	Name  string
	Score float64
}

// Leaderboard keeps player scores in one sorted set: member = name, score = points.
type Leaderboard struct {
	rdb *redis.Client
	key string
}

// NewLeaderboard returns a leaderboard stored under key.
func NewLeaderboard(rdb *redis.Client, key string) *Leaderboard {
	return &Leaderboard{rdb: rdb, key: key}
}

// Add sets the score of name. Adding an existing name replaces its score (no duplicate member).
func (l *Leaderboard) Add(ctx context.Context, name string, score float64) error {
	return l.rdb.ZAdd(ctx, l.key, redis.Z{Score: score, Member: name}).Err()
}

// Top returns the names of the n highest scores, best first.
// It returns an empty non-nil slice for an empty board or n <= 0.
func (l *Leaderboard) Top(ctx context.Context, n int) ([]string, error) {
	// ZRANGE key 0 -1 REV means "everything", so a non-positive n must not reach Redis.
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

// TopWithScores is Top with scores. go-redis decodes the RESP3 reply (a nested array of
// [member, score] pairs, scores as doubles) into []redis.Z for us.
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
