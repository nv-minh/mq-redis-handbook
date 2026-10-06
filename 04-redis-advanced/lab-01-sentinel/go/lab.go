// Package lab connects a Go client to Redis Sentinel from the host and reads what Sentinel
// believes about the topology. The nodes announce Docker DNS names, so the client needs a Dialer
// that maps them to the ports published on 127.0.0.1.
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

// MasterName is the name of the monitored master in the sentinel config of infra/docker-compose.yml.
const MasterName = "mymaster"

// SentinelAddrs are the three sentinels as the host reaches them (ports are published 1:1).
var SentinelAddrs = []string{"127.0.0.1:26379", "127.0.0.1:26380", "127.0.0.1:26381"}

// natMap rewrites every address Sentinel announces (a Docker DNS name) to the published port on
// localhost. go-redis has no natMap option, so the rewrite lives in Dialer below. It lists the
// sentinels too: go-redis learns the other sentinels from SENTINEL SENTINELS and dials them with
// the same Dialer.
var natMap = map[string]string{
	"redis-master:6380":    "127.0.0.1:6380",
	"redis-replica-1:6381": "127.0.0.1:6381",
	"redis-replica-2:6382": "127.0.0.1:6382",
	"sentinel-1:26379":     "127.0.0.1:26379",
	"sentinel-2:26380":     "127.0.0.1:26380",
	"sentinel-3:26381":     "127.0.0.1:26381",
}

// HostAddr maps an announced address (redis-master:6380) to the address the host can dial.
func HostAddr(announced string) string {
	if mapped, ok := natMap[announced]; ok {
		return mapped
	}
	return announced
}

// Dialer is the go-redis substitute for ioredis natMap: FailoverOptions.Dialer receives the
// address that Sentinel announced and dials its mapped host address instead.
func Dialer(ctx context.Context, network, addr string) (net.Conn, error) {
	d := net.Dialer{Timeout: 2 * time.Second}
	return d.DialContext(ctx, network, HostAddr(addr))
}

// ConnectViaSentinel returns a client that always talks to the current master: it asks the
// sentinels where the master is, connects, and asks again on every new connection, so after a
// failover it follows the promoted node without a restart.
func ConnectViaSentinel() *redis.Client {
	return redis.NewFailoverClient(&redis.FailoverOptions{
		MasterName:    MasterName,
		SentinelAddrs: SentinelAddrs,
		Dialer:        Dialer,
		DialTimeout:   2 * time.Second,
		// Keep retrying through the failover instead of failing the command at once.
		MaxRetries:      10,
		MinRetryBackoff: 100 * time.Millisecond,
		MaxRetryBackoff: 500 * time.Millisecond,
	})
}

// withSentinel runs fn against the first sentinel that answers.
func withSentinel[T any](ctx context.Context, fn func(*redis.Client) (T, error)) (T, error) {
	var zero T
	lastErr := errors.New("no sentinel configured")
	for _, addr := range SentinelAddrs {
		// Protocol 2: SENTINEL replies are flat arrays of field/value pairs.
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
		return nil, fmt.Errorf("unexpected reply %T", reply)
	}
	out := map[string]string{}
	for i := 0; i+1 < len(flat); i += 2 {
		out[fmt.Sprint(flat[i])] = fmt.Sprint(flat[i+1])
	}
	return out, nil
}

// CurrentMaster returns the master as Sentinel announces it ("redis-master:6380"), not mapped.
func CurrentMaster(ctx context.Context) (string, error) {
	return withSentinel(ctx, func(s *redis.Client) (string, error) {
		reply, err := s.Do(ctx, "SENTINEL", "get-master-addr-by-name", MasterName).Slice()
		if err != nil {
			return "", err
		}
		if len(reply) != 2 {
			return "", fmt.Errorf("sentinel does not know master %s", MasterName)
		}
		return net.JoinHostPort(fmt.Sprint(reply[0]), fmt.Sprint(reply[1])), nil
	})
}

// ReplicaState is what Sentinel reports about one replica.
type ReplicaState struct {
	// Addr is the announced address, "redis-replica-1:6381".
	Addr string
	// Flags are the Sentinel flags, for example "slave" or "slave,s_down,disconnected".
	Flags string
	// LinkStatus is the replication link to the master as the replica reports it.
	LinkStatus string
	// MasterHost is the host of the master this replica says it follows.
	MasterHost string
}

// Topology is what one sentinel believes about the master, its replicas and the other sentinels.
type Topology struct {
	Master      string
	MasterFlags string
	// Replicas lists the announced address of every replica.
	Replicas []string
	States   []ReplicaState
	// Sentinels counts the sentinels that see each other, including the one asked.
	Sentinels int
}

// ReadTopology asks the first sentinel that answers.
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

// Healthy means: one plain master, two plain replicas linked to it, three sentinels.
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

// ServiceOf returns the compose service of a node: the announced host is the service name.
func ServiceOf(announced string) string {
	host, _, err := net.SplitHostPort(announced)
	if err != nil {
		return announced
	}
	return host
}
