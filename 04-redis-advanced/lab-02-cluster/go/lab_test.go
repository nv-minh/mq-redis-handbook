package lab

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/nv-minh/mq-redis-handbook/internal/testkit"
	"github.com/redis/go-redis/v9"
)

const hint = `Redis Cluster chưa chạy. Hãy bật cả hai topology bằng lệnh: make up PROFILE="sentinel cluster"`

// requireClusterProfile dừng test ngay với thông báo rõ ràng nếu profile cluster chưa chạy,
// thay vì để test treo rồi mới lỗi khó hiểu. Chủ đề 04 cần cả sentinel lẫn cluster.
func requireClusterProfile(t *testing.T) {
	t.Helper()
	// Gợi ý chỉ được in khi test lỗi, nằm ngay cạnh thông báo lỗi của WaitForPort.
	t.Cleanup(func() {
		if t.Failed() {
			t.Log(hint)
		}
	})
	for _, port := range ClusterPorts {
		testkit.WaitForPort(t, fmt.Sprintf("127.0.0.1:%d", port), 3*time.Second)
	}
}

// direct mở connection trực tiếp tới MỘT node, không đi qua cluster client nên không tự theo MOVED.
// Protocol 2 giữ reply của CLUSTER SLOTS ở dạng mảng thuần.
func direct(t *testing.T, port int) *redis.Client {
	t.Helper()
	c := redis.NewClient(&redis.Options{Addr: fmt.Sprintf("127.0.0.1:%d", port), Protocol: 2, MaxRetries: -1})
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// trackKeys xóa các key qua cluster client khi test kết thúc.
// DEL nhiều key khác slot sẽ lỗi CROSSSLOT nên xóa từng key một.
func trackKeys(t *testing.T) func(keys ...string) {
	t.Helper()
	var keys []string
	t.Cleanup(func() {
		cluster := ConnectCluster()
		defer func() { _ = cluster.Close() }()
		for _, k := range keys {
			cluster.Del(context.Background(), k)
		}
	})
	return func(add ...string) { keys = append(keys, add...) }
}

func TestSlotForMatchesClusterKeyslot(t *testing.T) {
	requireClusterProfile(t)
	ctx := context.Background()
	node := direct(t, 7001)
	keys := []string{
		"foo", "bar", "123456789", "user:1000", "", "a",
		// quy tắc hash tag theo cluster spec
		"{user1000}.following", "{user1000}.followers",
		"foo{}{bar}",    // tag rỗng: băm cả key
		"foo{{bar}}zap", // tag là "{bar"
		"foo{bar}{zap}", // tag đầu tiên thắng: "bar"
		"{}foo",         // key bắt đầu bằng {} thì băm cả key
		"foo{bar",       // không có dấu } đóng: băm cả key
		"foo}bar{",      // dấu } đứng trước dấu {
		"{", "}", "{}",
		"{{}}", // tag là "{"
		"{a}", "a{b}c{d}e",
		// không phải ASCII: hash chạy trên byte UTF-8
		"khóa", "日本語", "{日本}語", "café:{ünï}", "🔑", "k🔑{🔑}",
		// byte không hợp lệ UTF-8 (string Go là chuỗi byte nên biểu diễn được, TypeScript thì không)
		"\xff\xfe", "{\xff}x",
	}
	for _, key := range keys {
		want, err := node.Do(ctx, "CLUSTER", "KEYSLOT", key).Int()
		if err != nil {
			t.Fatalf("CLUSTER KEYSLOT %q lỗi: %v", key, err)
		}
		if got := SlotFor(key); got != want {
			t.Errorf("SlotFor(%q) = %d, còn CLUSTER KEYSLOT = %d", key, got, want)
		}
	}
	// Check value của CRC16/XMODEM theo cluster spec: CRC16("123456789") = 0x31C3.
	if got, want := SlotFor("123456789"), 0x31c3%16384; got != want {
		t.Errorf("SlotFor(123456789) = %d, mong đợi %d", got, want)
	}
}

func TestHashTagRulesFollowTheClusterSpec(t *testing.T) {
	requireClusterProfile(t)
	same := [][2]string{
		{"{user1000}.following", "{user1000}.followers"},
		{"{user1000}.following", "user1000"},
		{"foo{bar}{zap}", "bar"},
		{"foo{{bar}}zap", "{bar"},
	}
	for _, p := range same {
		if SlotFor(p[0]) != SlotFor(p[1]) {
			t.Errorf("SlotFor(%q) != SlotFor(%q), mong đợi bằng nhau", p[0], p[1])
		}
	}
	// Tag rỗng, tag không đóng và key bắt đầu bằng {} đều băm cả key, nên slot khác slot của phần trong ngoặc.
	different := [][2]string{{"foo{}{bar}", "bar"}, {"foo{bar", "bar"}, {"{}foo", "foo"}}
	for _, p := range different {
		if SlotFor(p[0]) == SlotFor(p[1]) {
			t.Errorf("SlotFor(%q) == SlotFor(%q), mong đợi khác nhau", p[0], p[1])
		}
	}
}

func TestKeysWithSameHashTagShareASlot(t *testing.T) {
	requireClusterProfile(t)
	ctx := context.Background()
	node := direct(t, 7001)
	track := trackKeys(t)
	tag := testkit.UniqueName("lab04-tag")
	sameTag := []string{"{" + tag + "}:pending", "{" + tag + "}:processing", "{" + tag + "}:dead"}
	for _, key := range sameTag {
		got, err := node.Do(ctx, "CLUSTER", "KEYSLOT", key).Int()
		if err != nil || got != SlotFor("{"+tag+"}") || SlotFor(key) != SlotFor("{"+tag+"}") {
			t.Fatalf("key %q: KEYSLOT=%d err=%v SlotFor=%d, mong đợi %d", key, got, err, SlotFor(key), SlotFor("{"+tag+"}"))
		}
	}
	// Không có hash tag thì các key cùng hậu tố vẫn rải ra nhiều slot (tên cố định, đã biết là khác nhau).
	if SlotFor("{alpha}:pending") == SlotFor("{beta}:pending") || SlotFor("alpha:pending") == SlotFor("alpha:processing") {
		t.Fatal("mong đợi các key không có hash tag nằm ở slot khác nhau")
	}

	// Lệnh multi-key trên các key cùng một slot chạy được qua cluster client.
	cluster := ConnectCluster()
	defer func() { _ = cluster.Close() }()
	track(sameTag...)
	if err := cluster.MSet(ctx, sameTag[0], "1", sameTag[1], "2", sameTag[2], "3").Err(); err != nil {
		t.Fatalf("MSET với chung một hash tag lỗi: %v", err)
	}
	got, err := cluster.MGet(ctx, sameTag...).Result()
	if err != nil || fmt.Sprint(got) != "[1 2 3]" {
		t.Fatalf("MGET = %v err=%v, mong đợi [1 2 3]", got, err)
	}
}

func TestMultiKeyCommandAcrossSlotsFailsWithCrossslot(t *testing.T) {
	requireClusterProfile(t)
	ctx := context.Background()
	track := trackKeys(t)

	prefix := testkit.UniqueName("lab04-cross")
	a, b := prefix+"-a", prefix+"-b"
	for i := 0; SlotFor(a) == SlotFor(b); i++ {
		b = prefix + "-b" + strconv.Itoa(i)
	}
	track(a, b)

	// Gửi MSET tới MỘT node qua connection trực tiếp: server kiểm tra CROSSSLOT trước MOVED,
	// nên lỗi này đến từ bất kỳ node nào kể cả node không sở hữu slot của key.
	err := direct(t, 7001).MSet(ctx, a, "1", b, "2").Err()
	if err == nil || !strings.HasPrefix(err.Error(), "CROSSSLOT") {
		t.Fatalf("MSET khác slot: err = %v, mong đợi lỗi CROSSSLOT", err)
	}

	// Cùng kiểu key nhưng chung một hash tag thì thành công trên node sở hữu slot đó.
	tag := testkit.UniqueName("lab04-tagged")
	tagged := []string{"{" + tag + "}:a", "{" + tag + "}:b"}
	track(tagged...)
	owner, err := SlotOwner(ctx, SlotFor(tagged[0]))
	if err != nil {
		t.Fatalf("không tìm được master sở hữu slot: %v", err)
	}
	ownerPort := portOf(t, HostAddr(owner))
	ownerNode := direct(t, ownerPort)
	if err := ownerNode.MSet(ctx, tagged[0], "1", tagged[1], "2").Err(); err != nil {
		t.Fatalf("MSET trên node sở hữu %s lỗi: %v", owner, err)
	}
	if got, _ := ownerNode.MGet(ctx, tagged...).Result(); fmt.Sprint(got) != "[1 2]" {
		t.Fatalf("MGET trên node sở hữu = %v, mong đợi [1 2]", got)
	}

	// Node khác (master của shard khác hoặc replica) trả MOVED cho lệnh ghi vào slot này,
	// kèm địa chỉ Docker DNS của node sở hữu.
	otherPort := ClusterPorts[0]
	if otherPort == ownerPort {
		otherPort = ClusterPorts[1]
	}
	err = direct(t, otherPort).MSet(ctx, tagged[0], "1", tagged[1], "2").Err()
	if err == nil || !strings.HasPrefix(err.Error(), "MOVED") {
		t.Fatalf("MSET trên node không sở hữu: err = %v, mong đợi MOVED", err)
	}
}

func TestClusterClientWritesKeysSpreadOverAllMasters(t *testing.T) {
	requireClusterProfile(t)
	ctx := context.Background()
	track := trackKeys(t)
	// Chứng minh Dialer: sau CLUSTER SLOTS client dial redis-cluster-N:700N, tên này phải được đổi về localhost.
	// 60 key rải trên cả 3 master nên client buộc phải nối tới đủ ba node.
	cluster := ConnectCluster()
	defer func() { _ = cluster.Close() }()
	prefix := testkit.UniqueName("lab04-spread")
	owners := map[string]bool{}
	for i := 0; i < 60; i++ {
		key := fmt.Sprintf("%s:%d", prefix, i)
		track(key)
		if err := cluster.Set(ctx, key, strconv.Itoa(i), 0).Err(); err != nil {
			t.Fatalf("SET %s lỗi: %v", key, err)
		}
		owner, err := SlotOwner(ctx, SlotFor(key))
		if err != nil {
			t.Fatalf("không tìm được master sở hữu slot: %v", err)
		}
		owners[owner] = true
	}
	for i := 0; i < 60; i++ {
		got, err := cluster.Get(ctx, fmt.Sprintf("%s:%d", prefix, i)).Result()
		if err != nil || got != strconv.Itoa(i) {
			t.Fatalf("GET %d = %q err=%v (không khớp giá trị đã ghi)", i, got, err)
		}
	}
	if len(owners) != 3 {
		t.Fatalf("key rơi vào %d master (%v), mong đợi 3", len(owners), owners)
	}
}

func portOf(t *testing.T, addr string) int {
	t.Helper()
	_, p, ok := strings.Cut(addr, ":")
	if !ok {
		t.Fatalf("không có port trong %q", addr)
	}
	port, err := strconv.Atoi(p)
	if err != nil {
		t.Fatalf("port của %q lỗi: %v", addr, err)
	}
	return port
}
