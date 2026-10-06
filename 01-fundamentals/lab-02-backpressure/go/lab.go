// Package lab là queue có giới hạn trong bộ nhớ, với backpressure: khi queue đầy, Publish
// chặn tới khi consumer nhường chỗ hoặc context kết thúc. Producer bị làm chậm theo tốc độ
// của consumer thay vì để queue phình ra không giới hạn.
package lab

import (
	"context"
	"errors"
	"sync"
)

// ErrClosed được Publish trả về sau khi Close.
var ErrClosed = errors.New("lab: queue đã đóng")

// BoundedQueue giữ tối đa capacity message. An toàn khi dùng đồng thời từ nhiều goroutine.
type BoundedQueue[T any] struct {
	items     chan T
	done      chan struct{}
	closeOnce sync.Once
}

// NewBoundedQueue trả về queue giữ tối đa capacity message. capacity phải >= 1.
func NewBoundedQueue[T any](capacity int) *BoundedQueue[T] {
	if capacity < 1 {
		panic("lab: capacity phải >= 1")
	}
	return &BoundedQueue[T]{items: make(chan T, capacity), done: make(chan struct{})}
}

// Publish chặn khi queue đang đầy. Hàm trả về nil khi message đã vào queue,
// ctx.Err() nếu ctx kết thúc trước, hoặc ErrClosed nếu queue đã đóng.
func (q *BoundedQueue[T]) Publish(ctx context.Context, msg T) error {
	select {
	case <-q.done:
		return ErrClosed
	default:
	}
	select {
	case q.items <- msg:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-q.done:
		return ErrClosed
	}
}

// Consume khởi động một goroutine consumer tuần tự chạy tới khi Close. Message đang được
// xử lý đã rời queue, nên không tính vào Size.
func (q *BoundedQueue[T]) Consume(handler func(T)) {
	go func() {
		for {
			select {
			case msg := <-q.items:
				handler(msg)
			case <-q.done:
				return
			}
		}
	}()
}

// Size là số message đang chờ trong queue, không bao giờ vượt quá capacity.
func (q *BoundedQueue[T]) Size() int { return len(q.items) }

// Close dừng consumer và làm các lời gọi Publish đang chờ và sau này fail với ErrClosed.
// Gọi nhiều lần vẫn an toàn.
func (q *BoundedQueue[T]) Close() { q.closeOnce.Do(func() { close(q.done) }) }
