package lab

import (
	"context"
	"os"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/nv-minh/mq-redis-handbook/internal/testkit"
	"github.com/redis/go-redis/v9"
)

func redisURL() string {
	if url := os.Getenv("REDIS_URL"); url != "" {
		return url
	}
	return "redis://127.0.0.1:6379"
}

// newBoard trả về bảng xếp hạng trên một key duy nhất, key này bị xóa khi test kết thúc.
func newBoard(t *testing.T) *Leaderboard {
	t.Helper()
	opts, err := redis.ParseURL(redisURL())
	if err != nil {
		t.Fatalf("REDIS_URL không hợp lệ: %v", err)
	}
	rdb := redis.NewClient(opts)
	key := testkit.UniqueName("lab02-leaderboard")
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		rdb.Del(ctx, key)
		_ = rdb.Close()
	})
	return NewLeaderboard(rdb, key)
}

func mustAdd(t *testing.T, b *Leaderboard, name string, score float64) {
	t.Helper()
	if err := b.Add(context.Background(), name, score); err != nil {
		t.Fatalf("Add(%q): %v", name, err)
	}
}

func TestTop3ReturnsHighestScoresInOrder(t *testing.T) {
	b := newBoard(t)
	mustAdd(t, b, "alice", 50)
	mustAdd(t, b, "bob", 90)
	mustAdd(t, b, "carol", 70)
	mustAdd(t, b, "dave", 10)
	mustAdd(t, b, "erin", 80)

	got, err := b.Top(context.Background(), 3)
	if err != nil {
		t.Fatal(err)
	}
	// Điểm cao nhất trước, và chỉ có ba entry được yêu cầu.
	if want := []string{"bob", "erin", "carol"}; !slices.Equal(got, want) {
		t.Fatalf("Top(3) = %v, mong đợi %v", got, want)
	}
}

func TestTopOnEmptyBoardReturnsEmptyList(t *testing.T) {
	b := newBoard(t)
	got, err := b.Top(context.Background(), 3)
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || len(got) != 0 {
		t.Fatalf("Top(3) trên bảng rỗng = %#v, mong đợi slice rỗng khác nil", got)
	}
}

func TestTopReturnsFewerNamesWhenTheBoardIsSmallerThanN(t *testing.T) {
	b := newBoard(t)
	mustAdd(t, b, "alice", 1)
	mustAdd(t, b, "bob", 2)
	got, err := b.Top(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"bob", "alice"}; !slices.Equal(got, want) {
		t.Fatalf("Top(10) = %v, mong đợi %v", got, want)
	}
}

func TestTopWithNonPositiveNReturnsEmptyList(t *testing.T) {
	// ZRANGE key 0 -1 REV trả về TẤT CẢ, nên n = 0 không được đổi thành stop = -1.
	b := newBoard(t)
	mustAdd(t, b, "alice", 1)
	got, err := b.Top(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("Top(0) = %v, mong đợi rỗng", got)
	}
}

func TestAddingAnExistingNameUpdatesItsScoreInsteadOfDuplicating(t *testing.T) {
	b := newBoard(t)
	mustAdd(t, b, "alice", 10)
	mustAdd(t, b, "bob", 20)
	mustAdd(t, b, "alice", 30)
	got, err := b.Top(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"alice", "bob"}; !slices.Equal(got, want) {
		t.Fatalf("Top(10) = %v, mong đợi %v", got, want)
	}
}

func TestTopWithScoresParsesTheReplyShapeOfTheClient(t *testing.T) {
	b := newBoard(t)
	mustAdd(t, b, "alice", 10.5)
	mustAdd(t, b, "bob", 20)
	got, err := b.TopWithScores(context.Background(), 2)
	if err != nil {
		t.Fatal(err)
	}
	want := []Entry{{Name: "bob", Score: 20}, {Name: "alice", Score: 10.5}}
	if !slices.Equal(got, want) {
		t.Fatalf("TopWithScores(2) = %v, mong đợi %v", got, want)
	}
}

func TestResp3ReplyShapeOfZrangeWithscoresIsMeasuredNotAssumed(t *testing.T) {
	// go-redis 9 mặc định nói RESP3. Cố định dạng reply thô thật để nâng cấp client không đổi chúng một cách âm thầm.
	opts, err := redis.ParseURL(redisURL())
	if err != nil {
		t.Fatal(err)
	}
	rdb := redis.NewClient(opts)
	ctx := context.Background()
	key := testkit.UniqueName("lab02-shape")
	t.Cleanup(func() {
		rdb.Del(context.Background(), key)
		_ = rdb.Close()
	})
	if err := rdb.ZAdd(ctx, key, redis.Z{Score: 10, Member: "a"}, redis.Z{Score: 20.5, Member: "b"}).Err(); err != nil {
		t.Fatal(err)
	}

	info, err := rdb.Do(ctx, "CLIENT", "INFO").Text()
	if err != nil || !strings.Contains(info, "resp=3") {
		t.Fatalf("CLIENT INFO = %q, %v; mong đợi resp=3", info, err)
	}
	// Mảng lồng các cặp [member string, score float64] (RESP3 có kiểu double gốc).
	raw, err := rdb.Do(ctx, "ZRANGE", key, 0, -1, "REV", "WITHSCORES").Result()
	if err != nil {
		t.Fatal(err)
	}
	want := []any{[]any{"b", 20.5}, []any{"a", float64(10)}}
	if !reflect.DeepEqual(raw, want) {
		t.Fatalf("reply thô = %#v, mong đợi %#v", raw, want)
	}
}
