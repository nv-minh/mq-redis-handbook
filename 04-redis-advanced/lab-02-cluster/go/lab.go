// Package lab tính hash slot của Redis Cluster (CRC16 kèm hash tag) và kết nối client Go
// tới cluster Docker từ máy host. Các node tự khai địa chỉ bằng tên Docker DNS,
// nên client cần một Dialer đổi tên đó sang port đã publish trên 127.0.0.1.
package lab

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/redis/go-redis/v9"
)

// ClusterPorts là sáu node cluster như máy host nhìn thấy (port được publish 1:1 nên trùng với port trong container).
var ClusterPorts = []int{7001, 7002, 7003, 7004, 7005, 7006}

// natMap đổi mọi địa chỉ node mà cluster báo (redis-cluster-1:7001) sang port đã publish trên localhost.
// go-redis không có option natMap, nên việc đổi địa chỉ nằm trong Dialer.
var natMap = func() map[string]string {
	m := map[string]string{}
	for i, port := range ClusterPorts {
		m[fmt.Sprintf("redis-cluster-%d:%d", i+1, port)] = fmt.Sprintf("127.0.0.1:%d", port)
	}
	return m
}()

// HostAddr đổi địa chỉ node tự khai (redis-cluster-2:7002) sang địa chỉ mà máy host dial được.
func HostAddr(announced string) string {
	if mapped, ok := natMap[announced]; ok {
		return mapped
	}
	return announced
}

// Dialer là cách go-redis thay cho natMap của ioredis: ClusterOptions.Dialer nhận địa chỉ lấy từ
// CLUSTER SLOTS hoặc MOVED và dial địa chỉ host tương ứng thay vào đó.
func Dialer(ctx context.Context, network, addr string) (net.Conn, error) {
	d := net.Dialer{Timeout: 2 * time.Second}
	return d.DialContext(ctx, network, HostAddr(addr))
}

// ConnectCluster trả về cluster client tự theo MOVED và làm mới slot map.
func ConnectCluster() *redis.ClusterClient {
	addrs := make([]string, 0, 3)
	for _, port := range ClusterPorts[:3] {
		addrs = append(addrs, fmt.Sprintf("127.0.0.1:%d", port))
	}
	return redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:       addrs,
		Dialer:      Dialer,
		DialTimeout: 2 * time.Second,
	})
}

const slots = 16384

// crcTable là bảng của CRC16/XMODEM (poly 0x1021, init 0, không reflect, xorout 0), biến thể mà Redis Cluster dùng.
var crcTable = func() [256]uint16 {
	var table [256]uint16
	for b := range table {
		crc := uint16(b) << 8
		for range 8 {
			if crc&0x8000 != 0 {
				crc = crc<<1 ^ 0x1021
			} else {
				crc <<= 1
			}
		}
		table[b] = crc
	}
	return table
}()

func crc16(data []byte) uint16 {
	var crc uint16
	for _, b := range data {
		crc = crc<<8 ^ crcTable[byte(crc>>8)^b]
	}
	return crc
}

// hashPart trả về phần của key được đem đi băm.
// Nếu key có `{`, có `}` đứng sau nó, và có ít nhất một byte giữa `{` ĐẦU TIÊN và `}` đầu tiên sau nó
// thì chỉ phần đó được băm.
// Ngược lại (không có `{`, không có `}` phía sau, hoặc tag rỗng `{}`) thì băm cả key.
func hashPart(key []byte) []byte {
	open := bytes.IndexByte(key, '{')
	if open < 0 {
		return key
	}
	closeAt := bytes.IndexByte(key[open+1:], '}')
	if closeAt <= 0 { // không có dấu } đóng, hoặc tag rỗng "{}"
		return key
	}
	return key[open+1 : open+1+closeAt]
}

// SlotFor là hash slot của một key: CRC16(phần được băm) mod 16384, cùng giá trị với CLUSTER KEYSLOT.
// Hàm làm việc trên byte của string như Redis, nên key không phải ASCII hay không hợp lệ UTF-8 vẫn đúng.
func SlotFor(key string) int {
	return int(crc16(hashPart([]byte(key))) % slots)
}

// SlotOwner trả về master sở hữu slot, theo lời cluster báo ("redis-cluster-2:7002"), chưa đổi sang localhost.
func SlotOwner(ctx context.Context, slot int) (string, error) {
	lastErr := errors.New("chưa cấu hình node cluster nào")
	for _, port := range ClusterPorts {
		// Protocol 2: CLUSTER SLOTS trả mảng lồng nhau thuần.
		node := redis.NewClient(&redis.Options{
			Addr:        fmt.Sprintf("127.0.0.1:%d", port),
			Protocol:    2,
			DialTimeout: time.Second,
			MaxRetries:  -1,
		})
		// Dùng lệnh thô vì CLUSTER SLOTS có kiểu typed đã deprecated: [[start, end, [host, port, id, ...], replica...], ...]
		ranges, err := node.WithTimeout(2*time.Second).Do(ctx, "CLUSTER", "SLOTS").Slice()
		_ = node.Close()
		if err != nil {
			lastErr = err
			continue
		}
		for _, r := range ranges {
			entry, ok := r.([]any)
			if !ok || len(entry) < 3 {
				continue
			}
			start, _ := entry[0].(int64)
			end, _ := entry[1].(int64)
			master, ok := entry[2].([]any)
			if !ok || len(master) < 2 || int64(slot) < start || int64(slot) > end {
				continue
			}
			return net.JoinHostPort(fmt.Sprint(master[0]), fmt.Sprint(master[1])), nil
		}
		return "", fmt.Errorf("slot %d chưa có master nào sở hữu", slot)
	}
	return "", lastErr
}
