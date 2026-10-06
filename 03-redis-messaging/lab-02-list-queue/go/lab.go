// Package lab cài đặt queue đáng tin cậy trên Redis list: BLMOVE vào processing list riêng của từng
// consumer, LREM để ack, và một lượt recovery cho các consumer đã chết.
package lab

import (
	"context"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
)

// Config nối các thành phần của một ReliableQueue.
type Config struct {
	// Commands chạy các lệnh thường (LPUSH, LREM, LMOVE).
	Commands *redis.Client
	// Blocking chỉ chạy BLMOVE: connection đang bị chặn không phục vụ được lệnh khác, nên hãy tách nó
	// khỏi Commands. go-redis giữ một pool cho mỗi client, và một BLMOVE đang chặn giữ một
	// connection của pool cho tới khi nó return.
	Blocking *redis.Client
	// Queue là key của list queue. Các processing list nằm dưới "<Queue>:processing:<consumerID>".
	Queue string
	// BlockTimeout là thời gian Dequeue chờ trên queue rỗng. Giá trị 0 sẽ chặn vĩnh viễn, nên
	// NewReliableQueue thay nó bằng một giây.
	BlockTimeout time.Duration
}

// ReliableQueue là queue mà message do consumer lấy sẽ không bao giờ mất cho tới khi được ack.
//
// Enqueue: LPUSH (message mới vào bên trái, consumer lấy từ bên phải: FIFO).
// Dequeue: BLMOVE queue processing RIGHT LEFT, một bước atomic vừa lấy message cũ nhất vừa
// cất nó vào processing list của consumer.
// Ack: LREM khỏi processing list. RecoverStale: chuyển list của consumer đã chết về lại queue.
// Delivery là at-least-once: message được recover từ một consumer chỉ chậm chứ chưa chết sẽ chạy hai lần.
type ReliableQueue struct {
	cfg Config
}

// NewReliableQueue trả về một queue cho các client và key đã cho.
func NewReliableQueue(cfg Config) *ReliableQueue {
	if cfg.BlockTimeout <= 0 {
		cfg.BlockTimeout = time.Second
	}
	return &ReliableQueue{cfg: cfg}
}

// ProcessingKey là list giữ các message mà consumerID đã lấy nhưng chưa ack.
func (q *ReliableQueue) ProcessingKey(consumerID string) string {
	return q.cfg.Queue + ":processing:" + consumerID
}

// Enqueue thêm msg vào queue.
func (q *ReliableQueue) Enqueue(ctx context.Context, msg string) error {
	return q.cfg.Commands.LPush(ctx, q.cfg.Queue, msg).Err()
}

// Dequeue lấy message cũ nhất, chặn tối đa bằng BlockTimeout. ok là false (và err nil) khi
// queue rỗng suốt thời gian timeout, tức là BLMOVE trả về nil. Message nằm trong processing
// list của consumerID cho tới khi Ack.
//
// Lệnh BLMove có kiểu khiến go-redis kéo dài read deadline của socket thêm đúng thời gian block, nên
// ReadTimeout của client ngắn hơn BlockTimeout vẫn ổn ở đây (còn Do("BLMOVE", ...) thô thì không).
func (q *ReliableQueue) Dequeue(ctx context.Context, consumerID string) (msg string, ok bool, err error) {
	msg, err = q.cfg.Blocking.BLMove(ctx, q.cfg.Queue, q.ProcessingKey(consumerID), "RIGHT", "LEFT", q.cfg.BlockTimeout).Result()
	if errors.Is(err, redis.Nil) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return msg, true, nil
}

// Ack xóa msg khỏi processing list của consumerID. Hàm trả về false nếu msg không có ở đó.
func (q *ReliableQueue) Ack(ctx context.Context, consumerID, msg string) (bool, error) {
	n, err := q.cfg.Commands.LRem(ctx, q.ProcessingKey(consumerID), 1, msg).Result()
	return n == 1, err
}

// RecoverStale đưa mọi message trong processing list của consumerID về lại queue và
// trả về số message đã chuyển (0 nếu list rỗng). Gọi hàm này cho consumer mà bạn biết đã chết.
//
// Mỗi LMOVE là atomic, nên tiến trình recover có crash cũng không làm mất message.
// LEFT sang RIGHT: processing list giữ message mới nhất ở đầu bên trái, nên chuyển từ đầu bên trái
// của nó sang đầu bên phải của queue (đầu mà consumer đọc) khiến message cũ nhất được push
// sau cùng, do đó được consume đầu tiên trở lại: giữ nguyên thứ tự ban đầu.
func (q *ReliableQueue) RecoverStale(ctx context.Context, consumerID string) (int64, error) {
	var moved int64
	for {
		_, err := q.cfg.Commands.LMove(ctx, q.ProcessingKey(consumerID), q.cfg.Queue, "LEFT", "RIGHT").Result()
		if errors.Is(err, redis.Nil) {
			return moved, nil
		}
		if err != nil {
			return moved, err
		}
		moved++
	}
}
