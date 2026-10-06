// Package lab so sánh lệnh tuần tự với pipeline và làm check-and-decrement atomic
// bằng một Lua script.
package lab

import (
	"context"
	"fmt"
	"net"
	"sync/atomic"

	"github.com/redis/go-redis/v9"
)

// decrIfPositive kiểm tra và giảm trong MỘT script, nên không client nào chen vào được giữa
// lúc kiểm tra và DECR. Script trả về 1 khi đã giảm và 0 khi key không tồn tại hoặc không lớn hơn 0.
var decrIfPositive = redis.NewScript(`
local value = tonumber(redis.call('GET', KEYS[1]))
if value and value > 0 then
  redis.call('DECR', KEYS[1])
  return 1
end
return 0
`)

// DecrIfPositive giảm key một cách atomic, chỉ khi nó đang chứa một số lớn hơn 0.
// Hàm trả về true khi lần gọi này lấy được một đơn vị. Không bao giờ tạo key và không bao giờ
// để nó xuống dưới 0, dù có bao nhiêu caller tranh nhau. redis.Script gửi EVALSHA và chuyển sang EVAL khi gặp NOSCRIPT.
func DecrIfPositive(ctx context.Context, rdb *redis.Client, key string) (bool, error) {
	n, err := decrIfPositive.Run(ctx, rdb, []string{key}).Int()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

// DecrIfPositiveNaive là phiên bản sai, chỉ dùng cho demo: GET, quyết định ở client, rồi DECR.
// Client khác có thể DECR giữa lúc GET và DECR, nên counter có thể xuống dưới 0.
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

// SetSequential gửi n lệnh SET, mỗi lệnh chờ reply xong mới gửi lệnh tiếp theo: n round trip.
func SetSequential(ctx context.Context, rdb *redis.Client, prefix string, n int) error {
	for i := 0; i < n; i++ {
		if err := rdb.Set(ctx, fmt.Sprintf("%s:%d", prefix, i), "1", 0).Err(); err != nil {
			return err
		}
	}
	return nil
}

// SetPipelined gửi cũng n lệnh SET đó trong một pipeline: 1 round trip.
func SetPipelined(ctx context.Context, rdb *redis.Client, prefix string, n int) error {
	_, err := rdb.Pipelined(ctx, func(p redis.Pipeliner) error {
		for i := 0; i < n; i++ {
			p.Set(ctx, fmt.Sprintf("%s:%d", prefix, i), "1", 0)
		}
		return nil
	})
	return err
}

// WriteCounter đếm số lần gọi Write trên mọi connection mà một client mở.
type WriteCounter struct{ n atomic.Int64 }

// Writes trả về số lần write trên socket kể từ lần Reset gần nhất.
func (c *WriteCounter) Writes() int64 { return c.n.Load() }

// Reset đặt bộ đếm về 0.
func (c *WriteCounter) Reset() { c.n.Store(0) }

type countingConn struct {
	net.Conn
	counter *WriteCounter
}

func (c *countingConn) Write(p []byte) (int, error) {
	c.counter.n.Add(1)
	return c.Conn.Write(p)
}

// NewCountingClient trả về một client mà các connection của nó đếm số lần write trên socket.
// Đây là cách lab đo round trip mà không cần đồng hồ bấm giờ: một lệnh gửi riêng là một
// write, còn một pipeline được flush bằng một write duy nhất, nên số write bằng số round trip request/response.
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
