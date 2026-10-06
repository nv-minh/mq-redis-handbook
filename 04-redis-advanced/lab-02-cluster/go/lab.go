// Package lab computes Redis Cluster hash slots (CRC16 with hash tags) and connects a Go client
// to the Docker cluster from the host. The nodes announce Docker DNS names, so the client needs a
// Dialer that maps them to the ports published on 127.0.0.1.
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

// ClusterPorts are the six cluster nodes as the host reaches them (ports are published 1:1).
var ClusterPorts = []int{7001, 7002, 7003, 7004, 7005, 7006}

// natMap rewrites every node address the cluster announces (redis-cluster-1:7001) to the
// published port on localhost. go-redis has no natMap option, so the rewrite lives in Dialer.
var natMap = func() map[string]string {
	m := map[string]string{}
	for i, port := range ClusterPorts {
		m[fmt.Sprintf("redis-cluster-%d:%d", i+1, port)] = fmt.Sprintf("127.0.0.1:%d", port)
	}
	return m
}()

// HostAddr maps an announced address (redis-cluster-2:7002) to the address the host can dial.
func HostAddr(announced string) string {
	if mapped, ok := natMap[announced]; ok {
		return mapped
	}
	return announced
}

// Dialer is the go-redis substitute for ioredis natMap: ClusterOptions.Dialer receives the
// address from CLUSTER SLOTS or MOVED and dials its mapped host address instead.
func Dialer(ctx context.Context, network, addr string) (net.Conn, error) {
	d := net.Dialer{Timeout: 2 * time.Second}
	return d.DialContext(ctx, network, HostAddr(addr))
}

// ConnectCluster returns a cluster client that follows MOVED and refreshes its slot map.
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

// crcTable is CRC16/XMODEM (poly 0x1021, init 0, no reflection, xorout 0), the variant Redis Cluster uses.
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

// hashPart returns the part of the key that is hashed. If the key has a `{`, a `}` after it,
// and at least one byte between the FIRST `{` and the first `}` after it, only that part is
// hashed. Otherwise (no `{`, no `}` after it, or `{}`) the whole key is hashed.
func hashPart(key []byte) []byte {
	open := bytes.IndexByte(key, '{')
	if open < 0 {
		return key
	}
	closeAt := bytes.IndexByte(key[open+1:], '}')
	if closeAt <= 0 { // no closing brace, or "{}"
		return key
	}
	return key[open+1 : open+1+closeAt]
}

// SlotFor is the hash slot of a key: CRC16(hashed part) mod 16384, the same value as CLUSTER KEYSLOT.
// It works on the bytes of the string, like Redis.
func SlotFor(key string) int {
	return int(crc16(hashPart([]byte(key))) % slots)
}

// SlotOwner returns the master that owns slot, as the cluster announces it ("redis-cluster-2:7002").
func SlotOwner(ctx context.Context, slot int) (string, error) {
	lastErr := errors.New("no cluster node configured")
	for _, port := range ClusterPorts {
		// Protocol 2: CLUSTER SLOTS replies as plain nested arrays.
		node := redis.NewClient(&redis.Options{
			Addr:        fmt.Sprintf("127.0.0.1:%d", port),
			Protocol:    2,
			DialTimeout: time.Second,
			MaxRetries:  -1,
		})
		// Raw CLUSTER SLOTS: [[start, end, [host, port, id, ...], replica...], ...]
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
		return "", fmt.Errorf("slot %d is not covered by any master", slot)
	}
	return "", lastErr
}
