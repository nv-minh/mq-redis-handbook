// Package lab cài đặt consumer có retry và dead-letter queue (DLQ) trên Redis Streams.
//
// Redis Streams không có DLQ sẵn (suy ra từ danh sách lệnh trong docs), nên pattern tự cài:
// message lỗi nằm lại trong pending list (PEL), được tiếp quản lại bằng XAUTOCLAIM sau MinIdle,
// delivery count lấy từ XPENDING (dạng extended). Khi count chạm MaxAttempts thì consumer
// XADD message vào DLQ stream kèm metadata rồi XACK entry gốc.
package lab

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// ErrInvalidArgument bọc mọi lỗi do tham số không hợp lệ (errors.Is khớp được).
var ErrInvalidArgument = errors.New("tham số không hợp lệ")

// readCount: đọc từng message một, để message đang chờ trong batch không bị tính idle
// trong lúc handler trước còn chạy.
const readCount = 1

// claimPage là số entry mỗi lần XAUTOCLAIM quét và claim (mặc định của server).
const claimPage = 100

// Config nối các thành phần của một Queue.
type Config struct {
	// Commands chạy các lệnh thường.
	Commands *redis.Client
	// Blocking chỉ chạy XREADGROUP ... BLOCK, tách khỏi Commands.
	Blocking *redis.Client
	// Stream là stream chính, Group là consumer group, DLQStream chứa các message đã bị loại.
	Stream, Group, DLQStream string
	// Consumer là tên consumer của instance này (duy nhất trong group).
	Consumer string
	// MinIdle: XAUTOCLAIM chỉ tiếp quản entry đã pending lâu hơn ngưỡng này.
	MinIdle time.Duration
	// BlockTime là thời gian chờ entry mới mỗi vòng. Giá trị 0 hoặc nhỏ hơn thành một giây.
	// ReadTimeout của client Blocking không cần lớn hơn BlockTime, vì XReadGroup có kiểu của go-redis tự cộng
	// thời gian block vào read deadline (chương 03 đã đo). Chỉ lệnh thô qua Do mới cần ReadTimeout đủ lớn.
	BlockTime time.Duration
}

// Message là một entry của stream.
type Message struct {
	ID     string
	Fields map[string]string
}

// Delivery là một lần giao message cho handler. DeliveryCount là số lần entry đã được giao (lần đầu là 1).
type Delivery struct {
	ID            string
	Fields        map[string]string
	DeliveryCount int64
}

// Handler xử lý một message. Trả lỗi nghĩa là lần xử lý này thất bại.
type Handler func(ctx context.Context, d Delivery) error

// PendingEntry là một dòng của pending list (XPENDING dạng extended).
type PendingEntry struct {
	ID            string
	Consumer      string
	Idle          time.Duration
	DeliveryCount int64
}

// Options của ConsumeWithDLQ.
type Options struct {
	// MaxAttempts là tổng số lần handler được chạy cho một message trước khi nó bị chuyển vào DLQ. Phải lớn hơn 0.
	MaxAttempts int
}

// Stats là kết quả của một lần ConsumeWithDLQ.
type Stats struct {
	// Succeeded là số lần handler thành công (entry đã được XACK).
	Succeeded int
	// Failed là số lần handler thất bại.
	Failed int
	// DeadLettered là số message đã chuyển vào DLQ.
	DeadLettered int
}

// Queue là một consumer trên stream với retry và DLQ.
type Queue struct {
	cfg Config
}

// NewQueue trả về một Queue cho cấu hình đã cho.
func NewQueue(cfg Config) *Queue {
	if cfg.BlockTime <= 0 {
		cfg.BlockTime = time.Second
	}
	return &Queue{cfg: cfg}
}

// EnsureGroup chạy XGROUP CREATE ... MKSTREAM từ id 0. Idempotent: lỗi BUSYGROUP bị nuốt.
func (q *Queue) EnsureGroup(ctx context.Context) error {
	err := q.cfg.Commands.XGroupCreateMkStream(ctx, q.cfg.Stream, q.cfg.Group, "0").Err()
	if err != nil && !strings.HasPrefix(err.Error(), "BUSYGROUP") {
		return err
	}
	return nil
}

// Publish chạy XADD vào stream chính và trả về id của entry.
func (q *Queue) Publish(ctx context.Context, fields map[string]string) (string, error) {
	return q.cfg.Commands.XAdd(ctx, &redis.XAddArgs{Stream: q.cfg.Stream, Values: toValues(fields)}).Result()
}

// ConsumeWithDLQ chạy vòng consume cho tới khi hết việc: stream không còn entry mới VÀ pending list
// của group rỗng. Nếu ctx bị hủy, hàm trả về thống kê tới lúc đó cùng ctx.Err(). Mỗi vòng:
//
//  1. XAUTOCLAIM tiếp quản entry pending quá MinIdle (của consumer đã chết, hoặc lần thử trước đã lỗi).
//     Lần claim cộng 1 vào delivery count. Entry có count lớn hơn MaxAttempts đã bị giao đủ số lần mà không ai
//     báo kết quả (consumer chết giữa chừng), nên vào DLQ mà không chạy handler, để một message làm chết
//     consumer không lặp vô hạn.
//  2. XREADGROUP ... > đọc entry mới (delivery count 1), chặn tối đa BlockTime.
//  3. Handler thành công thì XACK. Handler lỗi thì entry nằm lại trong PEL; nếu delivery count (đọc từ
//     XPENDING) đã chạm MaxAttempts thì chuyển vào DLQ rồi XACK.
//
// Handler phải chạy xong trong khoảng ngắn hơn MinIdle, nếu không consumer khác sẽ tiếp quản message khi
// handler đầu còn đang chạy (cùng một đánh đổi với visibility timeout của SQS).
func (q *Queue) ConsumeWithDLQ(ctx context.Context, opts Options, handler Handler) (Stats, error) {
	if opts.MaxAttempts < 1 {
		return Stats{}, fmt.Errorf("%w: MaxAttempts phải là số nguyên dương, nhận được %d", ErrInvalidArgument, opts.MaxAttempts)
	}
	var stats Stats
	for {
		if err := ctx.Err(); err != nil {
			return stats, err
		}
		worked := false

		stale, err := q.claimStale(ctx)
		if err != nil {
			return stats, err
		}
		for _, d := range stale {
			worked = true
			if d.DeliveryCount > int64(opts.MaxAttempts) {
				reason := fmt.Sprintf("consumer đã chết hoặc bị kill trước khi báo lỗi (delivery count %d vượt maxAttempts %d)",
					d.DeliveryCount, opts.MaxAttempts)
				if err := q.deadLetter(ctx, d, reason, d.DeliveryCount); err != nil {
					return stats, err
				}
				stats.DeadLettered++
				continue
			}
			if err := q.handle(ctx, d, opts.MaxAttempts, handler, &stats); err != nil {
				return stats, err
			}
		}

		fresh, err := q.readNew(ctx)
		if err != nil {
			return stats, err
		}
		for _, d := range fresh {
			worked = true
			if err := q.handle(ctx, d, opts.MaxAttempts, handler, &stats); err != nil {
				return stats, err
			}
		}

		if !worked {
			pending, err := q.pendingCount(ctx)
			if err != nil {
				return stats, err
			}
			if pending == 0 {
				return stats, nil
			}
		}
	}
}

// ReplayDLQ chuyển tối đa limit entry cũ nhất từ DLQ về stream chính (field gốc, entry mới có id mới) và
// trả về số entry thực sự đã chuyển (0 khi DLQ rỗng, kể cả khi limit lớn hơn kích thước DLQ).
//
// Thứ tự an toàn: XADD vào stream chính TRƯỚC, XDEL khỏi DLQ SAU. Nếu process chết giữa hai lệnh thì entry nằm
// ở cả hai nơi và lần replay sau sẽ chuyển nó lần nữa (trùng, at-least-once), chứ không mất. Đảo thứ tự
// (XDEL trước) thì crash giữa chừng làm mất message. Vì vậy consumer phải idempotent theo business id nằm
// trong payload, không theo id của stream.
func (q *Queue) ReplayDLQ(ctx context.Context, limit int) (int, error) {
	if limit < 1 {
		return 0, fmt.Errorf("%w: limit phải là số nguyên dương, nhận được %d", ErrInvalidArgument, limit)
	}
	entries, err := q.dlqEntries(ctx, int64(limit))
	if err != nil {
		return 0, err
	}
	moved := 0
	for _, entry := range entries {
		var payload map[string]string
		if err := json.Unmarshal([]byte(entry.Fields["payload"]), &payload); err != nil {
			return moved, fmt.Errorf("DLQ entry %s có field payload không hợp lệ, không replay được: %w", entry.ID, err)
		}
		if err := q.cfg.Commands.XAdd(ctx, &redis.XAddArgs{Stream: q.cfg.Stream, Values: toValues(payload)}).Err(); err != nil {
			return moved, err
		}
		if err := q.cfg.Commands.XDel(ctx, q.cfg.DLQStream, entry.ID).Err(); err != nil {
			return moved, err
		}
		moved++
	}
	return moved, nil
}

// DLQEntries trả về các entry của DLQ, cũ nhất trước (XRANGE - +).
func (q *Queue) DLQEntries(ctx context.Context) ([]Message, error) {
	return q.dlqEntries(ctx, 1000)
}

func (q *Queue) dlqEntries(ctx context.Context, count int64) ([]Message, error) {
	rows, err := q.cfg.Commands.XRangeN(ctx, q.cfg.DLQStream, "-", "+", count).Result()
	if err != nil {
		return nil, err
	}
	return toMessages(rows), nil
}

// PendingEntries trả về pending list của group, cũ nhất trước (XPENDING dạng extended).
func (q *Queue) PendingEntries(ctx context.Context) ([]PendingEntry, error) {
	rows, err := q.cfg.Commands.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: q.cfg.Stream, Group: q.cfg.Group, Start: "-", End: "+", Count: 1000,
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

func (q *Queue) handle(ctx context.Context, d Delivery, maxAttempts int, handler Handler, stats *Stats) error {
	if handlerErr := handler(ctx, d); handlerErr != nil {
		if ctx.Err() != nil {
			// Handler lỗi vì ctx bị hủy: không tính là một lần thất bại của message.
			return ctx.Err()
		}
		stats.Failed++
		// Delivery count lấy từ server (XPENDING), không tự đếm ở client.
		count, found, err := q.deliveryCount(ctx, d.ID)
		if err != nil {
			return err
		}
		if found && count >= int64(maxAttempts) {
			if err := q.deadLetter(ctx, d, handlerErr.Error(), count); err != nil {
				return err
			}
			stats.DeadLettered++
		}
		return nil
	}
	if err := q.cfg.Commands.XAck(ctx, q.cfg.Stream, q.cfg.Group, d.ID).Err(); err != nil {
		return err
	}
	stats.Succeeded++
	return nil
}

// deadLetter chạy XADD vào DLQ rồi XACK entry gốc, hai lệnh riêng. Nếu process chết giữa chúng thì entry gốc
// vẫn pending, được claim lại và vào DLQ lần nữa: DLQ có thể có bản trùng (at-least-once), không mất message.
func (q *Queue) deadLetter(ctx context.Context, d Delivery, reason string, attempts int64) error {
	payload, err := json.Marshal(d.Fields)
	if err != nil {
		return err
	}
	err = q.cfg.Commands.XAdd(ctx, &redis.XAddArgs{
		Stream: q.cfg.DLQStream,
		Values: map[string]interface{}{
			"original_id": d.ID,
			"error":       reason,
			"attempts":    strconv.FormatInt(attempts, 10),
			"failed_at":   time.Now().UTC().Format("2006-01-02T15:04:05.000Z07:00"),
			"payload":     string(payload),
		},
	}).Err()
	if err != nil {
		return err
	}
	return q.cfg.Commands.XAck(ctx, q.cfg.Stream, q.cfg.Group, d.ID).Err()
}

// claimStale chạy XAUTOCLAIM theo cursor cho tới 0-0. Delivery count của từng entry lấy từ XPENDING sau khi claim.
// XAutoClaim có kiểu của go-redis bỏ danh sách id đã bị xóa khỏi stream, nhưng ở đây không cần nó:
// server tự gỡ các id đó khỏi PEL khi chạy XAUTOCLAIM.
func (q *Queue) claimStale(ctx context.Context) ([]Delivery, error) {
	var claimed []redis.XMessage
	cursor := "0-0"
	for {
		messages, next, err := q.cfg.Commands.XAutoClaim(ctx, &redis.XAutoClaimArgs{
			Stream: q.cfg.Stream, Group: q.cfg.Group, Consumer: q.cfg.Consumer,
			MinIdle: q.cfg.MinIdle, Start: cursor, Count: claimPage,
		}).Result()
		if err != nil {
			return nil, err
		}
		claimed = append(claimed, messages...)
		if next == "0-0" {
			break
		}
		cursor = next
	}
	var deliveries []Delivery
	for _, m := range claimed {
		count, found, err := q.deliveryCount(ctx, m.ID)
		if err != nil {
			return nil, err
		}
		if found {
			deliveries = append(deliveries, Delivery{ID: m.ID, Fields: toStrings(m.Values), DeliveryCount: count})
		}
	}
	return deliveries, nil
}

// readNew chạy XREADGROUP ... > : entry chưa từng được giao cho group nên delivery count là 1.
// XReadGroup có kiểu cũng kéo dài read deadline của socket thêm đúng thời gian block.
func (q *Queue) readNew(ctx context.Context) ([]Delivery, error) {
	streams, err := q.cfg.Blocking.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group: q.cfg.Group, Consumer: q.cfg.Consumer,
		Streams: []string{q.cfg.Stream, ">"}, Count: readCount, Block: q.cfg.BlockTime,
	}).Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var deliveries []Delivery
	for _, stream := range streams {
		for _, m := range stream.Messages {
			deliveries = append(deliveries, Delivery{ID: m.ID, Fields: toStrings(m.Values), DeliveryCount: 1})
		}
	}
	return deliveries, nil
}

// deliveryCount chạy XPENDING dạng extended cho đúng một id. found là false khi entry không còn trong PEL.
func (q *Queue) deliveryCount(ctx context.Context, id string) (count int64, found bool, err error) {
	rows, err := q.cfg.Commands.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: q.cfg.Stream, Group: q.cfg.Group, Start: id, End: id, Count: 1,
	}).Result()
	if err != nil || len(rows) == 0 {
		return 0, false, err
	}
	return rows[0].RetryCount, true, nil
}

// pendingCount trả về số entry trong PEL của group (XPENDING dạng summary).
func (q *Queue) pendingCount(ctx context.Context) (int64, error) {
	summary, err := q.cfg.Commands.XPending(ctx, q.cfg.Stream, q.cfg.Group).Result()
	if err != nil {
		return 0, err
	}
	return summary.Count, nil
}

func toValues(fields map[string]string) map[string]interface{} {
	values := make(map[string]interface{}, len(fields))
	for k, v := range fields {
		values[k] = v
	}
	return values
}

func toStrings(values map[string]interface{}) map[string]string {
	fields := make(map[string]string, len(values))
	for k, v := range values {
		fields[k] = fmt.Sprint(v)
	}
	return fields
}

func toMessages(rows []redis.XMessage) []Message {
	messages := make([]Message, len(rows))
	for i, row := range rows {
		messages[i] = Message{ID: row.ID, Fields: toStrings(row.Values)}
	}
	return messages
}
