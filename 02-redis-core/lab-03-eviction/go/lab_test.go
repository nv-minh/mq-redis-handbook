package lab

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/nv-minh/mq-redis-handbook/internal/testkit"
	"github.com/redis/go-redis/v9"
)

const (
	headroom  = 256 * 1024
	coldKeys  = 1500
	valueSize = 1024
)

func redisURL() string {
	if url := os.Getenv("REDIS_URL"); url != "" {
		return url
	}
	return "redis://127.0.0.1:6379"
}

// setup kết nối, lưu config eviction TRƯỚC lần CONFIG SET đầu tiên, và đăng ký các cleanup.
// t.Cleanup chạy theo thứ tự vào sau ra trước: config được khôi phục trước (để lệnh xóa không bao giờ
// bị chặn vì đầy bộ nhớ), rồi xóa các key của test, rồi đóng client.
//
// Lab này THAY ĐỔI setting maxmemory của server mà nó kết nối tới (và khôi phục lại).
// Chỉ chạy nó với Redis của compose stack trong handbook này (make up).
func setup(t *testing.T) (*redis.Client, string) {
	t.Helper()
	opts, err := redis.ParseURL(redisURL())
	if err != nil {
		t.Fatalf("REDIS_URL không hợp lệ: %v", err)
	}
	rdb := redis.NewClient(opts)
	t.Cleanup(func() { _ = rdb.Close() })

	ctx := context.Background()
	// Từ chối trước mọi lệnh CONFIG SET: sai server hoặc còn config cũ. Chưa đăng ký cleanup khôi phục
	// nào, nên khi bị từ chối sẽ không bao giờ ghi config ngược lại.
	if err := AssertOwnRedis(ctx, rdb, OwnRedisMarker); err != nil {
		t.Fatal(err)
	}
	original, err := ReadConfig(ctx, rdb)
	if err != nil {
		t.Fatalf("đọc config lỗi: %v", err)
	}
	prefix := testkit.UniqueName("lab02-evict")
	t.Cleanup(func() {
		cctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := DeleteByPrefix(cctx, rdb, prefix); err != nil {
			t.Errorf("xóa key lỗi: %v", err)
		}
	})
	t.Cleanup(func() {
		cctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := WriteConfig(cctx, rdb, original); err != nil {
			t.Errorf("KHÔI PHỤC CONFIG REDIS THẤT BẠI, hãy sửa bằng tay (maxmemory=%s policy=%s): %v",
				original.Maxmemory, original.Policy, err)
		}
	})
	return rdb, prefix
}

func evicted(t *testing.T, rdb *redis.Client) int64 {
	t.Helper()
	n, err := EvictedKeys(context.Background(), rdb)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func TestAllkeysLruEvictsColdKeysAndKeepsHotKey(t *testing.T) {
	ctx := context.Background()
	rdb, prefix := setup(t)
	hot := prefix + ":hot"
	coldPrefix := prefix + ":cold"

	// 1. Nạp các key lạnh và key nóng khi chưa có giới hạn.
	if err := SeedKeys(ctx, rdb, coldPrefix, coldKeys, valueSize); err != nil {
		t.Fatal(err)
	}
	if err := rdb.Set(ctx, hot, "hot", 0).Err(); err != nil {
		t.Fatal(err)
	}

	// 2. Idle time của LRU có độ phân giải một giây: chờ tới khi các key lạnh trông đã cũ.
	testkit.Eventually(t, 10*time.Second, func() (struct{}, bool) {
		idle, err := rdb.ObjectIdleTime(ctx, coldPrefix+":0").Result()
		return struct{}{}, err == nil && idle >= 2*time.Second
	})
	rdb.Get(ctx, hot) // đọc key nóng đặt lại idle time của nó về 0

	// 3. Giới hạn bộ nhớ ngay trên mức đang dùng, rồi tiếp tục ghi trong khi chạm vào key nóng.
	if _, err := LimitMemory(ctx, rdb, "allkeys-lru", headroom); err != nil {
		t.Fatal(err)
	}
	n, err := FillUntilEviction(ctx, rdb, prefix+":fill", 20_000, FillOptions{TouchKey: hot, MinEvicted: 900})
	if err != nil {
		t.Fatal(err)
	}

	// Không bao giờ assert một con số chính xác: eviction chỉ là xấp xỉ (dựa trên sampling).
	if n <= 0 {
		t.Fatalf("evicted = %d, mong đợi > 0", n)
	}
	if exists, _ := rdb.Exists(ctx, hot).Result(); exists != 1 {
		t.Fatal("key nóng đã bị evict")
	}
	cold := make([]string, coldKeys)
	for i := range cold {
		cold[i] = fmt.Sprintf("%s:%d", coldPrefix, i)
	}
	surviving, err := rdb.Exists(ctx, cold...).Result()
	if err != nil {
		t.Fatal(err)
	}
	if surviving >= coldKeys {
		t.Fatalf("cả %d key lạnh đều còn sống, mong đợi một số bị evict", surviving)
	}
}

func TestNoevictionPolicyRejectsWritesWhenFull(t *testing.T) {
	ctx := context.Background()
	rdb, prefix := setup(t)
	if err := rdb.Set(ctx, prefix+":existing", "still readable", 0).Err(); err != nil {
		t.Fatal(err)
	}
	before := evicted(t, rdb)

	if _, err := LimitMemory(ctx, rdb, "noeviction", headroom); err != nil {
		t.Fatal(err)
	}
	msg, err := FillUntilRejected(ctx, rdb, prefix+":fill", 20_000, valueSize)
	if err != nil {
		t.Fatal(err)
	}

	if !strings.HasPrefix(msg, "OOM") {
		t.Fatalf("lỗi khi ghi = %q, mong đợi bắt đầu bằng OOM", msg)
	}
	// Lệnh đọc vẫn chạy, không có gì bị evict, và dữ liệu ghi trước giới hạn vẫn còn.
	if got, err := rdb.Get(ctx, prefix+":existing").Result(); err != nil || got != "still readable" {
		t.Fatalf("GET existing = %q, %v", got, err)
	}
	if after := evicted(t, rdb); after != before {
		t.Fatalf("evicted_keys đổi từ %d sang %d dưới noeviction", before, after)
	}
	if exists, _ := rdb.Exists(ctx, prefix+":fill:0").Result(); exists != 1 {
		t.Fatal("dữ liệu ghi trước giới hạn đã bị mất")
	}
}

func TestVolatilePolicyRejectsWritesWhenNoKeyHasATTL(t *testing.T) {
	ctx := context.Background()
	rdb, prefix := setup(t)
	before := evicted(t, rdb)

	// volatile-lru chỉ được evict các key có TTL. Không key nào của ta có, nên nó hành xử như noeviction.
	if _, err := LimitMemory(ctx, rdb, "volatile-lru", headroom); err != nil {
		t.Fatal(err)
	}
	msg, err := FillUntilRejected(ctx, rdb, prefix+":fill", 20_000, valueSize)
	if err != nil {
		t.Fatal(err)
	}

	if !strings.HasPrefix(msg, "OOM") {
		t.Fatalf("lỗi khi ghi = %q, mong đợi bắt đầu bằng OOM", msg)
	}
	if after := evicted(t, rdb); after != before {
		t.Fatalf("evicted_keys đổi từ %d sang %d", before, after)
	}
}

func TestConfigIsRestoredToTheSavedValues(t *testing.T) {
	ctx := context.Background()
	rdb, _ := setup(t)
	original, err := ReadConfig(ctx, rdb)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := LimitMemory(ctx, rdb, "allkeys-lru", headroom); err != nil {
		t.Fatal(err)
	}
	if got, _ := ReadConfig(ctx, rdb); got.Policy != "allkeys-lru" {
		t.Fatalf("policy = %q, mong đợi allkeys-lru", got.Policy)
	}

	if err := WriteConfig(ctx, rdb, original); err != nil {
		t.Fatal(err)
	}
	if got, _ := ReadConfig(ctx, rdb); got != original {
		t.Fatalf("config = %+v, mong đợi %+v", got, original)
	}
}

func TestGuardRefusesARedisWithoutTheHandbookMarkerAndChangesNothing(t *testing.T) {
	ctx := context.Background()
	rdb, _ := setup(t)
	before, err := ReadConfig(ctx, rdb)
	if err != nil {
		t.Fatal(err)
	}
	err = AssertOwnRedis(ctx, rdb, "some-other-marker.rdb")
	if err == nil || !strings.Contains(err.Error(), "không phải marker của handbook") {
		t.Fatalf("AssertOwnRedis = %v, mong đợi lời từ chối nêu rõ marker", err)
	}
	if after, _ := ReadConfig(ctx, rdb); after != before { // chỉ đọc: không có CONFIG SET nào xảy ra
		t.Fatalf("config đổi từ %+v sang %+v", before, after)
	}
}

func TestGuardRefusesLeftoverConfigWithAHintToRecreateTheStack(t *testing.T) {
	state := ServerState{DBFilename: OwnRedisMarker, Maxmemory: "4000000", Policy: "allkeys-lru"}
	if msg := GuardProblem(state, OwnRedisMarker); !strings.Contains(msg, "make down && make up") {
		t.Fatalf("GuardProblem = %q, mong đợi gợi ý tạo lại stack", msg)
	}
	state.Maxmemory, state.Policy = "0", "noeviction"
	if msg := GuardProblem(state, OwnRedisMarker); msg != "" {
		t.Fatalf("GuardProblem trên server nguyên vẹn = %q, mong đợi rỗng", msg)
	}
}
