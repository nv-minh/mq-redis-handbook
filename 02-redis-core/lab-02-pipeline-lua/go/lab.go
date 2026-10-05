// Package lab compares sequential commands with a pipeline and makes check-and-decrement atomic
// with a Lua script.
package lab

import (
	"context"
	"fmt"
	"net"
	"sync/atomic"

	"github.com/redis/go-redis/v9"
)

// decrIfPositive checks and decrements in ONE script, so no other client can run between the
// check and the DECR. It returns 1 when it decremented and 0 when the key is missing or not above zero.
var decrIfPositive = redis.NewScript(`
local value = tonumber(redis.call('GET', KEYS[1]))
if value and value > 0 then
  redis.call('DECR', KEYS[1])
  return 1
end
return 0
`)

// DecrIfPositive atomically decrements key only when it holds a number above zero.
// It returns true when this call took one unit. It never creates the key and never lets it go
// below zero, however many callers race. redis.Script sends EVALSHA and falls back to EVAL on NOSCRIPT.
func DecrIfPositive(ctx context.Context, rdb *redis.Client, key string) (bool, error) {
	n, err := decrIfPositive.Run(ctx, rdb, []string{key}).Int()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

// DecrIfPositiveNaive is the broken version, for the demo only: GET, decide in the client, then DECR.
// Another client can DECR between the GET and the DECR, so the counter can go below zero.
func DecrIfPositiveNaive(ctx context.Context, rdb *redis.Client, key string) (bool, error) {
	value, err := rdb.Get(ctx, key).Int64()
	if err != nil && err != redis.Nil {
		return false, err
	}
	if value > 0 {
		return true, rdb.Decr(ctx, key).Err()
	}
	return false, nil
}

// SetSequential sends n SET commands, each waiting for its reply before the next: n round trips.
func SetSequential(ctx context.Context, rdb *redis.Client, prefix string, n int) error {
	for i := 0; i < n; i++ {
		if err := rdb.Set(ctx, fmt.Sprintf("%s:%d", prefix, i), "1", 0).Err(); err != nil {
			return err
		}
	}
	return nil
}

// SetPipelined sends the same n SET commands in one pipeline: 1 round trip.
func SetPipelined(ctx context.Context, rdb *redis.Client, prefix string, n int) error {
	_, err := rdb.Pipelined(ctx, func(p redis.Pipeliner) error {
		for i := 0; i < n; i++ {
			p.Set(ctx, fmt.Sprintf("%s:%d", prefix, i), "1", 0)
		}
		return nil
	})
	return err
}

// WriteCounter counts Write calls on every connection a client opens.
type WriteCounter struct{ n atomic.Int64 }

// Writes returns the number of socket writes since the last Reset.
func (c *WriteCounter) Writes() int64 { return c.n.Load() }

// Reset sets the count back to zero.
func (c *WriteCounter) Reset() { c.n.Store(0) }

type countingConn struct {
	net.Conn
	counter *WriteCounter
}

func (c *countingConn) Write(p []byte) (int, error) {
	c.counter.n.Add(1)
	return c.Conn.Write(p)
}

// NewCountingClient returns a client whose connections count their socket writes.
// This is how the lab measures round trips without a stopwatch: a command sent alone is one
// write, and a pipeline is flushed as a single write, so writes equal request/response round trips.
func NewCountingClient(opts *redis.Options) (*redis.Client, *WriteCounter) {
	counter := &WriteCounter{}
	var dialer net.Dialer
	opts.Dialer = func(ctx context.Context, network, addr string) (net.Conn, error) {
		conn, err := dialer.DialContext(ctx, network, addr)
		if err != nil {
			return nil, err
		}
		return &countingConn{Conn: conn, counter: counter}, nil
	}
	return redis.NewClient(opts), counter
}
