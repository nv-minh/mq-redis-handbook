// Package lab cài đặt competing consumers trên Redis Streams: n worker cùng một consumer group,
// mỗi message chỉ giao cho một worker. Worker ack sau khi handler thành công, nên delivery là
// at-least-once: handler lỗi hoặc worker chết giữa chừng thì message nằm lại trong pending list (PEL)
// và một worker còn sống tiếp quản bằng XAUTOCLAIM.
package lab

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// ErrInvalidArgument bọc mọi lỗi do tham số không hợp lệ (errors.Is khớp được).
var ErrInvalidArgument = errors.New("tham số không hợp lệ")

// Config nối các thành phần của một WorkerGroup.
type Config struct {
	// Commands chạy các lệnh thường (XADD, XACK, XAUTOCLAIM, XPENDING, XGROUP), do caller sở hữu.
	Commands *redis.Client
	// Options dùng để tạo một client blocking riêng cho mỗi worker (connection đang bị chặn không phục vụ
	// được lệnh khác). Các client này do Handle.Stop đóng. ReadTimeout (mặc định 3 giây) không cần lớn hơn BlockTime,
	// vì XReadGroup có kiểu tự cộng thời gian block vào read deadline (chương 03 đã đo); chỉ lệnh thô qua Do mới cần.
	Options *redis.Options
	// Stream là stream chứa công việc, Group là consumer group mà các worker cùng đọc.
	Stream, Group string
	// MinIdle: XAUTOCLAIM chỉ tiếp quản entry đã pending lâu hơn ngưỡng này.
	MinIdle time.Duration
	// BlockTime là thời gian mỗi worker chờ entry mới trong một lần XREADGROUP, cũng là độ trễ tối đa của Stop.
	// Giá trị 0 hoặc nhỏ hơn thành một giây.
	BlockTime time.Duration
	// OnError, nếu khác nil, được gọi khi handler lỗi. Mặc định bỏ qua: message nằm lại PEL và sẽ được claim lại.
	OnError func(err error, d Delivery)
}

// Delivery là một lần giao message cho handler. DeliveryCount là số lần entry đã được giao (lần đầu là 1).
type Delivery struct {
	ID     string
	Fields map[string]string
	// Consumer là tên consumer (worker) đang giữ message này.
	Consumer      string
	DeliveryCount int64
}

// Handler xử lý một message. Trả lỗi nghĩa là lần xử lý này thất bại và message không được ack.
// ctx không bị hủy khi Stop: Stop chờ handler đang chạy xong thay vì ngắt nó.
type Handler func(ctx context.Context, d Delivery) error

// PendingEntry là một dòng của pending list (XPENDING dạng extended).
type PendingEntry struct {
	ID            string
	Consumer      string
	Idle          time.Duration
	DeliveryCount int64
}

// WorkerGroup là một consumer group trên một stream.
type WorkerGroup struct {
	cfg Config
}

// NewWorkerGroup trả về một WorkerGroup cho cấu hình đã cho.
func NewWorkerGroup(cfg Config) *WorkerGroup {
	if cfg.BlockTime <= 0 {
		cfg.BlockTime = time.Second
	}
	return &WorkerGroup{cfg: cfg}
}

// EnsureGroup chạy XGROUP CREATE ... MKSTREAM từ id 0. Idempotent: lỗi BUSYGROUP bị nuốt.
func (g *WorkerGroup) EnsureGroup(ctx context.Context) error {
	err := g.cfg.Commands.XGroupCreateMkStream(ctx, g.cfg.Stream, g.cfg.Group, "0").Err()
	if err != nil && !strings.HasPrefix(err.Error(), "BUSYGROUP") {
		return err
	}
	return nil
}

// Publish chạy XADD vào stream và trả về id của entry.
func (g *WorkerGroup) Publish(ctx context.Context, fields map[string]string) (string, error) {
	values := make(map[string]interface{}, len(fields))
	for k, v := range fields {
		values[k] = v
	}
	return g.cfg.Commands.XAdd(ctx, &redis.XAddArgs{Stream: g.cfg.Stream, Values: values}).Result()
}

// PendingEntries trả về pending list của group, cũ nhất trước (XPENDING dạng extended).
func (g *WorkerGroup) PendingEntries(ctx context.Context) ([]PendingEntry, error) {
	rows, err := g.cfg.Commands.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: g.cfg.Stream, Group: g.cfg.Group, Start: "-", End: "+", Count: 1000,
	}).Result()
	if err != nil {
		return nil, err
	}
	entries := make([]PendingEntry, len(rows))
	for i, row := range rows {
		entries[i] = PendingEntry{ID: row.ID, Consumer: row.Consumer, Idle: row.Idle, DeliveryCount: row.RetryCount}
	}
	return entries, nil
}

// Handle điều khiển các worker đã khởi động.
type Handle struct {
	names    []string
	clients  []*redis.Client
	stopping chan struct{}
	wg       sync.WaitGroup

	stopOnce sync.Once
	mu       sync.Mutex
	failures []error
	stopErr  error
}

// ConsumerNames trả về tên consumer của từng worker (duy nhất, mỗi worker một tên).
func (h *Handle) ConsumerNames() []string { return append([]string(nil), h.names...) }

// Stop dừng nhận việc mới, chờ handler đang chạy xong (và ack nó), rồi đóng các client blocking.
// Hàm idempotent (các lần gọi sau trả cùng kết quả) và trả lỗi gộp nếu có worker đã chết vì lỗi Redis,
// sau khi đã dọn xong mọi client. Thời gian chờ tối đa là BlockTime cộng thời gian handler còn lại.
func (h *Handle) Stop() error {
	h.stopOnce.Do(func() {
		close(h.stopping)
		h.wg.Wait()
		var closeErrs []error
		for _, c := range h.clients {
			closeErrs = append(closeErrs, c.Close())
		}
		h.mu.Lock()
		defer h.mu.Unlock()
		h.stopErr = errors.Join(append(h.failures, closeErrs...)...)
	})
	return h.stopErr
}

// StartWorkers khởi động n worker. Mỗi worker có tên consumer duy nhất và một client riêng cho
// XREADGROUP BLOCK. Mỗi vòng lặp của worker:
//
//  1. XAUTOCLAIM tiếp quản một entry đã pending quá MinIdle (của worker đã chết, hoặc lần xử lý trước lỗi).
//  2. Nếu không có, XREADGROUP ... > đọc entry mới, chặn tối đa BlockTime.
//  3. Chạy handler. Thành công thì XACK; lỗi thì không ack.
//
// Handler phải chạy xong trong khoảng ngắn hơn MinIdle, nếu không worker khác sẽ tiếp quản message khi
// handler đầu còn đang chạy. Lab này không giới hạn số lần thử: message lỗi mãi sẽ bị thử lại mãi
// (xem lab 03 cho DLQ).
func (g *WorkerGroup) StartWorkers(n int, handler Handler) (*Handle, error) {
	if n < 1 {
		return nil, fmt.Errorf("%w: n phải là số nguyên dương, nhận được %d", ErrInvalidArgument, n)
	}
	suffix := make([]byte, 4)
	if _, err := rand.Read(suffix); err != nil {
		return nil, err
	}
	prefix := "worker-" + hex.EncodeToString(suffix)
	h := &Handle{stopping: make(chan struct{})}
	for i := 0; i < n; i++ {
		h.names = append(h.names, fmt.Sprintf("%s-%d", prefix, i))
		h.clients = append(h.clients, redis.NewClient(g.cfg.Options))
	}
	for i := range h.names {
		h.wg.Add(1)
		go func() {
			defer h.wg.Done()
			if err := g.runWorker(h, h.names[i], h.clients[i], handler); err != nil {
				h.mu.Lock()
				h.failures = append(h.failures, fmt.Errorf("%s: %w", h.names[i], err))
				h.mu.Unlock()
			}
		}()
	}
	return h, nil
}

func (g *WorkerGroup) runWorker(h *Handle, consumer string, blocking *redis.Client, handler Handler) error {
	// ctx của lệnh Redis và của handler không bị hủy khi Stop: Stop đợi vòng lặp tự thoát để không
	// bỏ dở một message đã đọc mà chưa ack.
	ctx := context.Background()
	for {
		select {
		case <-h.stopping:
			return nil
		default:
		}
		d, err := g.claimOne(ctx, consumer)
		if err != nil {
			return err
		}
		if d == nil {
			if d, err = g.readNew(ctx, blocking, consumer); err != nil {
				return err
			}
		}
		if d == nil {
			continue
		}
		if err := handler(ctx, *d); err != nil {
			// Không ack: message nằm lại PEL cho tới khi một worker claim lại sau MinIdle.
			if g.cfg.OnError != nil {
				g.cfg.OnError(err, *d)
			}
			continue
		}
		if err := g.cfg.Commands.XAck(ctx, g.cfg.Stream, g.cfg.Group, d.ID).Err(); err != nil {
			return err
		}
	}
}

// claimOne chạy XAUTOCLAIM theo cursor cho tới khi claim được một entry (hoặc cursor về 0-0). Mỗi lần chỉ claim
// một entry, để các entry còn lại không bị reset idle time rồi nằm chờ trong lúc handler chạy.
// Delivery count đọc từ XPENDING sau khi claim (claim cộng 1). XAutoClaim có kiểu của go-redis bỏ danh sách
// id đã bị xóa khỏi stream, nhưng ở đây không cần nó: server tự gỡ các id đó khỏi PEL.
func (g *WorkerGroup) claimOne(ctx context.Context, consumer string) (*Delivery, error) {
	cursor := "0-0"
	for {
		messages, next, err := g.cfg.Commands.XAutoClaim(ctx, &redis.XAutoClaimArgs{
			Stream: g.cfg.Stream, Group: g.cfg.Group, Consumer: consumer,
			MinIdle: g.cfg.MinIdle, Start: cursor, Count: 1,
		}).Result()
		if err != nil {
			return nil, err
		}
		if len(messages) > 0 {
			count, found, err := g.deliveryCount(ctx, messages[0].ID)
			if err != nil {
				return nil, err
			}
			if found {
				return &Delivery{ID: messages[0].ID, Fields: toStrings(messages[0].Values), Consumer: consumer, DeliveryCount: count}, nil
			}
		}
		if next == "0-0" {
			return nil, nil
		}
		cursor = next
	}
}

// readNew chạy XREADGROUP ... > : entry chưa từng được giao cho group nên delivery count là 1.
// XReadGroup có kiểu cũng kéo dài read deadline của socket thêm đúng thời gian block.
func (g *WorkerGroup) readNew(ctx context.Context, blocking *redis.Client, consumer string) (*Delivery, error) {
	streams, err := blocking.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group: g.cfg.Group, Consumer: consumer,
		Streams: []string{g.cfg.Stream, ">"}, Count: 1, Block: g.cfg.BlockTime,
	}).Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	for _, stream := range streams {
		for _, m := range stream.Messages {
			return &Delivery{ID: m.ID, Fields: toStrings(m.Values), Consumer: consumer, DeliveryCount: 1}, nil
		}
	}
	return nil, nil
}

// deliveryCount chạy XPENDING dạng extended cho đúng một id. found là false khi entry không còn trong PEL.
func (g *WorkerGroup) deliveryCount(ctx context.Context, id string) (count int64, found bool, err error) {
	rows, err := g.cfg.Commands.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: g.cfg.Stream, Group: g.cfg.Group, Start: id, End: id, Count: 1,
	}).Result()
	if err != nil || len(rows) == 0 {
		return 0, false, err
	}
	return rows[0].RetryCount, true, nil
}

func toStrings(values map[string]interface{}) map[string]string {
	fields := make(map[string]string, len(values))
	for k, v := range values {
		fields[k] = fmt.Sprint(v)
	}
	return fields
}
