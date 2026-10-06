// Package lab là broker trong bộ nhớ để cho thấy delivery semantics nghĩa là gì, không dùng mạng.
// Một message đi qua pending -> được giao cho handler -> (có thể) được ack. Việc mất message được
// tạo ra qua Options.LossRate và Options.Rand, nên mọi lỗi trong lab đều tái hiện được.
package lab

import (
	"math/rand"
	"sync"
)

// Mode chọn mức bảo đảm khi giao message.
type Mode string

const (
	AtMostOnce  Mode = "at-most-once"
	AtLeastOnce Mode = "at-least-once"
)

const defaultMaxDeliveries = 5

// Options cấu hình một Queue.
type Options struct {
	Mode Mode
	// LossRate là xác suất (0..1) có sự cố trên đường truyền.
	// AtMostOnce: lần giao bị mất, handler không bao giờ thấy message.
	// AtLeastOnce: ack của consumer bị mất, nên broker giao lại (sinh ra duplicate).
	// 1 nghĩa là luôn xảy ra, 0 nghĩa là không bao giờ.
	LossRate float64
	// Rand là nguồn ngẫu nhiên trong [0, 1). Mặc định là rand.Float64.
	Rand func() float64
	// MaxDeliveries (chỉ AtLeastOnce) là số lần giao tối đa trước khi message bị cho vào dead.
	// Mặc định là 5.
	MaxDeliveries int
}

// Handler xử lý một message. Trả về error mô phỏng consumer bị crash:
// AtMostOnce đã quên message, AtLeastOnce không nhận được ack nên thử giao lại.
type Handler func(id string) error

// Queue là broker trong bộ nhớ. An toàn khi dùng đồng thời từ nhiều goroutine.
type Queue struct {
	opts Options

	mu         sync.Mutex
	cond       *sync.Cond
	pending    []string
	deliveries map[string]int
	dead       []string
	handler    Handler
	running    bool
}

// NewQueue trả về một Queue rỗng.
func NewQueue(opts Options) *Queue {
	if opts.Rand == nil {
		opts.Rand = rand.Float64
	}
	if opts.MaxDeliveries <= 0 {
		opts.MaxDeliveries = defaultMaxDeliveries
	}
	q := &Queue{opts: opts, deliveries: map[string]int{}}
	q.cond = sync.NewCond(&q.mu)
	return q
}

// Publish đưa một message vào hàng đợi. Việc giao diễn ra bất đồng bộ sau khi có consumer đăng ký.
func (q *Queue) Publish(id string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.pending = append(q.pending, id)
	q.kick()
}

// Consume đăng ký consumer và bắt đầu giao các message đang pending.
func (q *Queue) Consume(h Handler) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.handler = h
	q.kick()
}

// Drain chặn tới khi mọi message có thể giao đã ngã ngũ (được ack, bị mất hoặc vào dead).
// Nếu chưa có consumer nào đăng ký thì hàm return ngay.
func (q *Queue) Drain() {
	q.mu.Lock()
	defer q.mu.Unlock()
	for q.running {
		q.cond.Wait()
	}
}

// DeadLetters trả về các id đã dùng hết MaxDeliveries mà vẫn chưa có ack.
func (q *Queue) DeadLetters() []string {
	q.mu.Lock()
	defer q.mu.Unlock()
	return append([]string(nil), q.dead...)
}

// kick khởi động goroutine giao message nếu còn việc và chưa có goroutine nào chạy. Caller phải giữ q.mu.
func (q *Queue) kick() {
	if !q.running && q.handler != nil && len(q.pending) > 0 {
		q.running = true
		go q.run()
	}
}

func (q *Queue) run() {
	q.mu.Lock()
	defer q.mu.Unlock()
	for q.handler != nil && len(q.pending) > 0 {
		id := q.pending[0]
		q.pending = q.pending[1:]
		h := q.handler
		q.mu.Unlock()
		q.deliver(id, h)
		q.mu.Lock()
	}
	q.running = false
	q.cond.Broadcast()
}

// deliver chỉ được gọi từ một goroutine run duy nhất, nên có thể dùng rand mà không cần khóa,
// nhưng phải lấy q.mu trước khi đụng vào state dùng chung.
func (q *Queue) deliver(id string, h Handler) {
	if q.opts.Mode == AtMostOnce {
		// Gửi rồi quên: broker làm rơi message trước hoặc trong lúc giao.
		if q.opts.Rand() < q.opts.LossRate {
			return
		}
		_ = h(id) // crash cũng không đổi được gì: message đã mất rồi
		return
	}

	err := h(id)
	acked := err == nil && q.opts.Rand() >= q.opts.LossRate // ack đi ngược về broker và có thể bị mất

	q.mu.Lock()
	defer q.mu.Unlock()
	attempt := q.deliveries[id] + 1
	switch {
	case acked:
		delete(q.deliveries, id)
	case attempt >= q.opts.MaxDeliveries:
		delete(q.deliveries, id)
		q.dead = append(q.dead, id)
	default:
		q.deliveries[id] = attempt
		q.pending = append(q.pending, id) // giao lại, xếp sau các message đang chờ
	}
}

// NewIdempotentHandler bọc apply để bỏ qua các id được giao lại. Id được ghi lại trước khi
// apply chạy và bị xóa nếu apply lỗi, nếu không một lần thử thất bại sẽ bị coi là duplicate
// mãi mãi. Chỉ trong bộ nhớ: consumer thật phải lưu marker nguyên tử cùng với side effect.
func NewIdempotentHandler(apply Handler) Handler {
	var mu sync.Mutex
	seen := map[string]struct{}{}
	return func(id string) error {
		mu.Lock()
		if _, dup := seen[id]; dup {
			mu.Unlock()
			return nil
		}
		seen[id] = struct{}{}
		mu.Unlock()

		if err := apply(id); err != nil {
			mu.Lock()
			delete(seen, id)
			mu.Unlock()
			return err
		}
		return nil
	}
}
