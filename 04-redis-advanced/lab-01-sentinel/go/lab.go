// Package lab kết nối client Go tới Redis Sentinel từ máy host và đọc điều Sentinel tin về topology.
// Các node tự khai địa chỉ bằng tên Docker DNS, nên client cần một Dialer đổi tên đó
// sang port đã publish trên 127.0.0.1.
package lab

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// MasterName là tên master mà Sentinel giám sát, đặt trong cấu hình sentinel của infra/docker-compose.yml.
const MasterName = "mymaster"

// SentinelAddrs là ba sentinel như máy host nhìn thấy (port được publish 1:1 nên trùng với port trong container).
var SentinelAddrs = []string{"127.0.0.1:26379", "127.0.0.1:26380", "127.0.0.1:26381"}

// natMap đổi mọi địa chỉ mà Sentinel báo (tên Docker DNS) sang port đã publish trên localhost.
// go-redis không có option natMap, nên việc đổi địa chỉ nằm trong Dialer bên dưới.
// Bảng liệt kê cả sentinel: go-redis học danh sách sentinel còn lại từ SENTINEL SENTINELS
// và dial chúng bằng chính Dialer này.
var natMap = map[string]string{
	"redis-master:6380":    "127.0.0.1:6380",
	"redis-replica-1:6381": "127.0.0.1:6381",
	"redis-replica-2:6382": "127.0.0.1:6382",
	"sentinel-1:26379":     "127.0.0.1:26379",
	"sentinel-2:26380":     "127.0.0.1:26380",
	"sentinel-3:26381":     "127.0.0.1:26381",
}

// HostAddr đổi địa chỉ node tự khai (redis-master:6380) sang địa chỉ mà máy host dial được.
func HostAddr(announced string) string {
	if mapped, ok := natMap[announced]; ok {
		return mapped
	}
	return announced
}

// Dialer là cách go-redis thay cho natMap của ioredis: FailoverOptions.Dialer nhận địa chỉ mà Sentinel đã báo
// và dial địa chỉ host tương ứng thay vào đó.
func Dialer(ctx context.Context, network, addr string) (net.Conn, error) {
	d := net.Dialer{Timeout: 2 * time.Second}
	return d.DialContext(ctx, network, HostAddr(addr))
}

// ConnectViaSentinel trả về client luôn nói chuyện với master hiện tại.
// Nó hỏi các sentinel master đang ở đâu, kết nối, và hỏi lại ở mỗi connection mới,
// nên sau failover nó tự đi theo node vừa được promote mà không cần khởi động lại.
func ConnectViaSentinel() *redis.Client {
	return redis.NewFailoverClient(&redis.FailoverOptions{
		MasterName:    MasterName,
		SentinelAddrs: SentinelAddrs,
		Dialer:        Dialer,
		DialTimeout:   2 * time.Second,
		// Retry xuyên qua failover thay vì trả lỗi ngay cho lệnh đang chạy.
		MaxRetries:      10,
		MinRetryBackoff: 100 * time.Millisecond,
		MaxRetryBackoff: 500 * time.Millisecond,
	})
}

// withSentinel chạy fn trên sentinel đầu tiên trả lời được, bỏ qua sentinel đang chết.
func withSentinel[T any](ctx context.Context, fn func(*redis.Client) (T, error)) (T, error) {
	var zero T
	lastErr := errors.New("chưa cấu hình sentinel nào")
	for _, addr := range SentinelAddrs {
		// Protocol 2: reply của SENTINEL là mảng phẳng các cặp field/value (RESP3 trả dạng khác).
		client := redis.NewClient(&redis.Options{
			Addr:        addr,
			Protocol:    2,
			DialTimeout: time.Second,
			MaxRetries:  -1,
		})
		v, err := fn(client.WithTimeout(2 * time.Second))
		_ = client.Close()
		if err == nil {
			return v, nil
		}
		lastErr = err
	}
	return zero, lastErr
}

func pairs(reply any) (map[string]string, error) {
	flat, ok := reply.([]any)
	if !ok {
		return nil, fmt.Errorf("reply không như mong đợi: %T", reply)
	}
	out := map[string]string{}
	for i := 0; i+1 < len(flat); i += 2 {
		out[fmt.Sprint(flat[i])] = fmt.Sprint(flat[i+1])
	}
	return out, nil
}

// CurrentMaster trả về master theo lời Sentinel báo ("redis-master:6380"), chưa đổi sang localhost.
func CurrentMaster(ctx context.Context) (string, error) {
	return withSentinel(ctx, func(s *redis.Client) (string, error) {
		reply, err := s.Do(ctx, "SENTINEL", "get-master-addr-by-name", MasterName).Slice()
		if err != nil {
			return "", err
		}
		if len(reply) != 2 {
			return "", fmt.Errorf("sentinel không biết master %s", MasterName)
		}
		return net.JoinHostPort(fmt.Sprint(reply[0]), fmt.Sprint(reply[1])), nil
	})
}

// ReplicaState là điều Sentinel báo về một replica.
type ReplicaState struct {
	// Addr là địa chỉ replica tự khai, ví dụ "redis-replica-1:6381".
	Addr string
	// Flags là cờ Sentinel, ví dụ "slave" hoặc "slave,s_down,disconnected".
	Flags string
	// LinkStatus là trạng thái link replication tới master do chính replica báo ("ok" là khỏe).
	LinkStatus string
	// MasterHost là host của master mà replica này nói là nó đang theo.
	MasterHost string
}

// Topology là điều một sentinel tin về master, các replica và các sentinel khác.
type Topology struct {
	Master      string
	MasterFlags string
	// Replicas liệt kê địa chỉ tự khai của từng replica.
	Replicas []string
	States   []ReplicaState
	// Sentinels là số sentinel thấy nhau, tính cả sentinel được hỏi.
	Sentinels int
}

// ReadTopology hỏi sentinel đầu tiên trả lời được.
func ReadTopology(ctx context.Context) (Topology, error) {
	return withSentinel(ctx, func(s *redis.Client) (Topology, error) {
		var t Topology
		masterReply, err := s.Do(ctx, "SENTINEL", "master", MasterName).Result()
		if err != nil {
			return t, err
		}
		master, err := pairs(masterReply)
		if err != nil {
			return t, err
		}
		others, _ := strconv.Atoi(master["num-other-sentinels"])
		t.Master = net.JoinHostPort(master["ip"], master["port"])
		t.MasterFlags = master["flags"]
		t.Sentinels = others + 1

		replicas, err := s.Do(ctx, "SENTINEL", "replicas", MasterName).Slice()
		if err != nil {
			return t, err
		}
		for _, r := range replicas {
			fields, err := pairs(r)
			if err != nil {
				return t, err
			}
			addr := net.JoinHostPort(fields["ip"], fields["port"])
			t.Replicas = append(t.Replicas, addr)
			t.States = append(t.States, ReplicaState{
				Addr:       addr,
				Flags:      fields["flags"],
				LinkStatus: fields["master-link-status"],
				MasterHost: fields["master-host"],
			})
		}
		return t, nil
	})
}

// Healthy nghĩa là: một master bình thường, hai replica bình thường đã nối tới đúng master đó, và đủ ba sentinel.
func (t Topology) Healthy() bool {
	if t.MasterFlags != "master" || t.Sentinels != 3 || len(t.States) != 2 {
		return false
	}
	masterHost, _, _ := net.SplitHostPort(t.Master)
	for _, r := range t.States {
		if r.Flags != "slave" || r.LinkStatus != "ok" || r.MasterHost != masterHost {
			return false
		}
	}
	return true
}

// ServiceOf trả về compose service của một node: host mà node tự khai chính là tên service.
func ServiceOf(announced string) string {
	host, _, err := net.SplitHostPort(announced)
	if err != nil {
		return announced
	}
	return host
}
