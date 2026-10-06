package lab

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/nv-minh/mq-redis-handbook/internal/testkit"
	"github.com/redis/go-redis/v9"
)

const hint = `Redis Sentinel chưa chạy. Hãy bật cả hai topology bằng lệnh: make up PROFILE="sentinel cluster"`

// requireSentinelProfile dừng test ngay với thông báo rõ ràng nếu profile sentinel chưa chạy,
// thay vì để test treo rồi mới lỗi khó hiểu. Chủ đề 04 cần cả sentinel lẫn cluster.
func requireSentinelProfile(t *testing.T) {
	t.Helper()
	// Gợi ý chỉ được in khi test lỗi, nằm ngay cạnh thông báo lỗi của WaitForPort.
	t.Cleanup(func() {
		if t.Failed() {
			t.Log(hint)
		}
	})
	for _, addr := range []string{"127.0.0.1:26379", "127.0.0.1:26380", "127.0.0.1:26381", "127.0.0.1:6380", "127.0.0.1:6381", "127.0.0.1:6382"} {
		testkit.WaitForPort(t, addr, 3*time.Second)
	}
}

func waitForHealthyTopology(t *testing.T, timeout time.Duration) {
	t.Helper()
	testkit.Eventually(t, timeout, func() (struct{}, bool) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		topology, err := ReadTopology(ctx)
		return struct{}{}, err == nil && topology.Healthy()
	})
}

func TestClientWritesThroughTheMasterThatSentinelReports(t *testing.T) {
	requireSentinelProfile(t)
	ctx := context.Background()
	client := ConnectViaSentinel()
	defer func() { _ = client.Close() }()
	key := testkit.UniqueName("lab04-sentinel")
	defer client.Del(ctx, key)

	if err := client.Set(ctx, key, "hello", 0).Err(); err != nil {
		t.Fatalf("SET lỗi: %v", err)
	}
	if got, _ := client.Get(ctx, key).Result(); got != "hello" {
		t.Fatalf("GET = %q, mong đợi hello", got)
	}

	master, err := CurrentMaster(ctx)
	if err != nil {
		t.Fatalf("không tìm được master hiện tại: %v", err)
	}
	direct := redis.NewClient(&redis.Options{Addr: HostAddr(master)})
	defer func() { _ = direct.Close() }()
	if got, _ := direct.Get(ctx, key).Result(); got != "hello" {
		t.Fatalf("giá trị trên master mà Sentinel báo = %q, mong đợi hello", got)
	}
}

func TestSentinelSeesOneMasterAndTwoReplicas(t *testing.T) {
	requireSentinelProfile(t)
	waitForHealthyTopology(t, 90*time.Second)
	topology, err := ReadTopology(context.Background())
	if err != nil {
		t.Fatalf("không đọc được topology: %v", err)
	}
	if len(topology.Replicas) != 2 || topology.Sentinels != 3 {
		t.Fatalf("topology = %+v, mong đợi 2 replica và 3 sentinel", topology)
	}
	seen := map[string]bool{topology.Master: true}
	for _, r := range topology.Replicas {
		seen[r] = true
	}
	if len(seen) != 3 {
		t.Fatalf("mong đợi 3 node khác nhau, nhận được %v", seen)
	}
}

// stopCurrentMaster dừng master mà Sentinel đang báo và đăng ký việc khôi phục nó.
// Việc khôi phục chạy cả khi test lỗi, và chờ tới khi topology khỏe lại (1 master, 2 replica, 3 sentinel).
// Master được tìm bằng Sentinel chứ không giả định là redis-master, vì một lần failover trước đó có thể đã dời master.
func stopCurrentMaster(t *testing.T, restore *[]string) string {
	t.Helper()
	ctx := context.Background()
	master, err := CurrentMaster(ctx)
	if err != nil {
		t.Fatalf("không tìm được master hiện tại: %v", err)
	}
	service := ServiceOf(master) // host mà Sentinel báo chính là tên service trong compose
	*restore = append(*restore, service)
	if err := StopService(ctx, service); err != nil {
		t.Fatalf("dừng %s lỗi: %v", service, err)
	}
	return master
}

func registerRestore(t *testing.T, restore *[]string) {
	t.Helper()
	t.Cleanup(func() {
		ctx := context.Background()
		for i := len(*restore) - 1; i >= 0; i-- {
			if err := StartService(ctx, (*restore)[i]); err != nil {
				t.Errorf("khôi phục %s lỗi: %v", (*restore)[i], err)
			}
		}
		*restore = nil
		waitForHealthyTopology(t, 90*time.Second)
	})
}

func TestChaosClientWritesSucceedWithin30sAfterMasterIsStopped(t *testing.T) {
	requireSentinelProfile(t)
	ctx := context.Background()
	var restore []string
	registerRestore(t, &restore)

	client := ConnectViaSentinel()
	key := testkit.UniqueName("lab04-sentinel-chaos")
	// t.Cleanup chạy ngược thứ tự đăng ký: xóa key trước (client đi theo Sentinel nên tìm được master hiện tại),
	// rồi đóng client, cuối cùng restore đã đăng ký ở trên mới bật lại node đã dừng.
	// Việc đóng client cũng phải là cleanup: nếu dùng defer thì client bị đóng TRƯỚC khi xóa key và key bị bỏ lại.
	t.Cleanup(func() { _ = client.Close() })
	t.Cleanup(func() {
		cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		if err := client.Del(cctx, key).Err(); err != nil {
			t.Errorf("xóa %s lỗi: %v", key, err)
		}
	})
	if err := client.Set(ctx, key, "before", 0).Err(); err != nil {
		t.Fatalf("SET trước failover lỗi: %v", err)
	}

	oldMaster := stopCurrentMaster(t, &restore)
	stoppedAt := time.Now()
	testkit.Eventually(t, 30*time.Second, func() (struct{}, bool) {
		attempt, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		return struct{}{}, client.Set(attempt, key, "after", 0).Err() == nil
	})
	elapsed := time.Since(stoppedAt)
	t.Logf("Đo failover: lần ghi thành công đầu tiên sau %s kể từ khi dừng master", elapsed.Round(time.Millisecond))
	if elapsed >= 30*time.Second {
		t.Fatalf("ghi mất %s, mong đợi dưới 30s", elapsed)
	}

	newMaster := testkit.Eventually(t, 10*time.Second, func() (string, bool) {
		m, err := CurrentMaster(ctx)
		return m, err == nil && m != oldMaster
	})
	direct := redis.NewClient(&redis.Options{Addr: HostAddr(newMaster)})
	defer func() { _ = direct.Close() }()
	if got, _ := direct.Get(ctx, key).Result(); got != "after" {
		t.Fatalf("giá trị trên master mới %s = %q, mong đợi after", newMaster, got)
	}
}

func TestChaosOldMasterRejoinsAsReplica(t *testing.T) {
	requireSentinelProfile(t)
	ctx := context.Background()
	var restore []string
	registerRestore(t, &restore)

	oldMaster := stopCurrentMaster(t, &restore)
	newMaster := testkit.Eventually(t, 30*time.Second, func() (string, bool) {
		m, err := CurrentMaster(ctx)
		return m, err == nil && m != oldMaster
	})

	// Bật master cũ lại.
	// redis-master khởi động như một master (không có --replicaof) và Sentinel hạ nó xuống làm replica.
	// Container replica khởi động với --replicaof redis-master và Sentinel trỏ nó sang master mới.
	// Cuối cùng node đó phải là replica của master MỚI, nên test chờ đúng điều kiện này
	// thay vì tin vào trạng thái "slave" nhất thời (có thể vẫn đang trỏ về master cũ).
	if err := StartService(ctx, ServiceOf(oldMaster)); err != nil {
		t.Fatalf("bật %s lỗi: %v", oldMaster, err)
	}
	restore = nil
	masterHost, _, _ := net.SplitHostPort(newMaster)
	testkit.Eventually(t, 60*time.Second, func() (struct{}, bool) {
		node := redis.NewClient(&redis.Options{Addr: HostAddr(oldMaster), DialTimeout: 2 * time.Second, MaxRetries: -1})
		defer func() { _ = node.Close() }()
		attempt, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		role, err := node.Do(attempt, "ROLE").Slice()
		if err != nil || len(role) < 2 {
			return struct{}{}, false
		}
		// ROLE của replica trả về: ["slave", masterHost, masterPort, state, offset]
		return struct{}{}, fmt.Sprint(role[0]) == "slave" && fmt.Sprint(role[1]) == masterHost
	})

	waitForHealthyTopology(t, 90*time.Second)
	topology, err := ReadTopology(ctx)
	if err != nil {
		t.Fatalf("không đọc được topology: %v", err)
	}
	if topology.Master != newMaster {
		t.Fatalf("master = %s, mong đợi %s", topology.Master, newMaster)
	}
	found := false
	for _, r := range topology.Replicas {
		found = found || r == oldMaster
	}
	if !found {
		t.Fatalf("master cũ %s không nằm trong danh sách replica %v", oldMaster, topology.Replicas)
	}
}
