// Package lab cài đặt work queue trên một Redis Stream với consumer group: XADD, XREADGROUP,
// XACK và XAUTOCLAIM để tiếp quản những gì consumer đã chết để lại ở trạng thái pending.
package lab

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// claimPage là số entry mà một lần gọi XAUTOCLAIM quét và claim. Đây là mặc định của server.
const claimPage = 100

// Config nối các thành phần của một StreamQueue.
type Config struct {
	// Commands chạy các lệnh thường (XADD, XACK, XAUTOCLAIM, XPENDING, XGROUP).
	Commands *redis.Client
	// Blocking chỉ chạy XREADGROUP ... BLOCK: connection đang bị chặn không phục vụ được lệnh khác,
	// nên hãy tách nó khỏi Commands.
	Blocking *redis.Client
	// Stream là key của stream.
	Stream string
	// BlockTime là thời gian Consume chờ entry mới. Giá trị 0 hoặc nhỏ hơn sẽ thành một giây.
	BlockTime time.Duration
}

// Message là một entry của stream.
type Message struct {
	ID     string
	Fields map[string]string
}

// PendingEntry là một dòng của pending list (PEL) của một group.
type PendingEntry struct {
	ID string
	// Consumer đang sở hữu entry.
	Consumer string
	// Idle là thời gian kể từ lần entry được giao gần nhất.
	Idle time.Duration
	// DeliveryCount là số lần entry đã được giao (một lần claim cũng tính là một lần giao).
	DeliveryCount int64
}

// ClaimResult là những gì ClaimStale đã tiếp quản.
type ClaimResult struct {
	// Messages là các entry hiện thuộc về consumer vừa claim.
	Messages []Message
	// DeletedIDs là các id còn trong pending list nhưng không còn tồn tại trong stream (Redis 7.0+).
	DeletedIDs []string
}

// StreamQueue là work queue trên một stream với consumer group. Delivery là at-least-once:
// một entry nằm trong pending list của group cho tới khi được ack.
type StreamQueue struct {
	cfg Config
}

// NewStreamQueue trả về một queue cho các client và stream đã cho.
func NewStreamQueue(cfg Config) *StreamQueue {
	if cfg.BlockTime <= 0 {
		cfg.BlockTime = time.Second
	}
	return &StreamQueue{cfg: cfg}
}

// Publish thêm một entry (XADD stream * field value ...) và trả về id của nó.
func (q *StreamQueue) Publish(ctx context.Context, fields map[string]string) (string, error) {
	values := make(map[string]interface{}, len(fields))
	for k, v := range fields {
		values[k] = v
	}
	return q.cfg.Commands.XAdd(ctx, &redis.XAddArgs{Stream: q.cfg.Stream, Values: values}).Result()
}

// CreateGroup chạy XGROUP CREATE ... MKSTREAM với start id "0": group thấy toàn bộ stream,
// và stream được tạo nếu chưa có. Hàm idempotent: lỗi BUSYGROUP (group đã tồn tại)
// bị nuốt và mọi lỗi khác được trả về.
func (q *StreamQueue) CreateGroup(ctx context.Context, group string) error {
	err := q.cfg.Commands.XGroupCreateMkStream(ctx, q.cfg.Stream, group, "0").Err()
	if err != nil && !strings.HasPrefix(err.Error(), "BUSYGROUP") {
		return err
	}
	return nil
}

// Consume đọc tối đa count entry chưa từng được giao cho group, chặn tối đa bằng BlockTime. Chúng
// vào pending list của consumer. Hàm trả về slice rỗng khi không có gì tới (reply là nil,
// go-redis báo là redis.Nil). XReadGroup có kiểu cũng kéo dài read deadline của socket
// thêm đúng thời gian block.
func (q *StreamQueue) Consume(ctx context.Context, group, consumer string, count int) ([]Message, error) {
	streams, err := q.cfg.Blocking.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    group,
		Consumer: consumer,
		Streams:  []string{q.cfg.Stream, ">"},
		Count:    int64(count),
		Block:    q.cfg.BlockTime,
	}).Result()
	if errors.Is(err, redis.Nil) {
		return []Message{}, nil
	}
	if err != nil {
		return nil, err
	}
	var messages []Message
	for _, stream := range streams {
		for _, m := range stream.Messages {
			fields := make(map[string]string, len(m.Values))
			for k, v := range m.Values {
				fields[k] = fmt.Sprint(v)
			}
			messages = append(messages, Message{ID: m.ID, Fields: fields})
		}
	}
	return messages, nil
}

// Ack chạy XACK và trả về số id đã được xóa khỏi pending list (0 nếu đã ack từ trước).
func (q *StreamQueue) Ack(ctx context.Context, group, id string) (int64, error) {
	return q.cfg.Commands.XAck(ctx, q.cfg.Stream, group, id).Result()
}

// ClaimStale tiếp quản mọi entry của group đã pending lâu hơn minIdle, bất kể ai đang sở hữu,
// và biến consumer thành chủ sở hữu. Hàm đi theo cursor của XAUTOCLAIM cho tới khi nó
// trả về 0-0. Claim đặt lại idle time và cộng thêm một vào delivery count.
// Hàm trả về kết quả rỗng khi không có entry nào đủ điều kiện.
//
// Reply được parse từ lệnh thô một cách có chủ đích. rdb.XAutoClaim có kiểu của go-redis
// v9.23.0 chỉ trả về (messages, nextCursor) và bỏ phần tử thứ ba, tức các id không còn
// tồn tại trong stream, khiến chúng nằm lại trong pending list mà không ai biết.
func (q *StreamQueue) ClaimStale(ctx context.Context, group, consumer string, minIdle time.Duration) (ClaimResult, error) {
	result := ClaimResult{Messages: []Message{}, DeletedIDs: []string{}}
	cursor := "0-0"
	for {
		reply, err := q.cfg.Commands.Do(ctx, "XAUTOCLAIM", q.cfg.Stream, group, consumer,
			minIdle.Milliseconds(), cursor, "COUNT", claimPage).Slice()
		if err != nil {
			return ClaimResult{}, err
		}
		next, messages, deleted, err := parseAutoClaim(reply)
		if err != nil {
			return ClaimResult{}, err
		}
		result.Messages = append(result.Messages, messages...)
		result.DeletedIDs = append(result.DeletedIDs, deleted...)
		if next == "0-0" {
			return result, nil
		}
		cursor = next
	}
}

// parseAutoClaim giải mã reply của XAUTOCLAIM: [cursor kế tiếp, [[id, [field, value, ...]], ...], [id đã xóa, ...]].
// RESP2 và RESP3 cho cùng một dạng với lệnh này (toàn mảng, id và field là string).
func parseAutoClaim(reply []interface{}) (next string, messages []Message, deleted []string, err error) {
	if len(reply) < 2 {
		return "", nil, nil, fmt.Errorf("reply XAUTOCLAIM có %d phần tử, mong đợi 3", len(reply))
	}
	next, ok := reply[0].(string)
	if !ok {
		return "", nil, nil, fmt.Errorf("cursor của XAUTOCLAIM là %T, mong đợi string", reply[0])
	}
	entries, _ := reply[1].([]interface{})
	for _, raw := range entries {
		entry, ok := raw.([]interface{})
		if !ok || len(entry) != 2 {
			return "", nil, nil, fmt.Errorf("entry của XAUTOCLAIM là %#v, mong đợi [id, fields]", raw)
		}
		id, _ := entry[0].(string)
		flat, isList := entry[1].([]interface{})
		if !isList {
			continue // entry không có field: đã bị xóa khỏi stream (server trước 7.0)
		}
		fields := make(map[string]string, len(flat)/2)
		for i := 0; i+1 < len(flat); i += 2 {
			fields[fmt.Sprint(flat[i])] = fmt.Sprint(flat[i+1])
		}
		messages = append(messages, Message{ID: id, Fields: fields})
	}
	if len(reply) >= 3 {
		ids, _ := reply[2].([]interface{})
		for _, raw := range ids {
			deleted = append(deleted, fmt.Sprint(raw))
		}
	}
	return next, messages, deleted, nil
}

// PendingCount trả về số entry trong pending list của group (XPENDING dạng summary).
func (q *StreamQueue) PendingCount(ctx context.Context, group string) (int64, error) {
	summary, err := q.cfg.Commands.XPending(ctx, q.cfg.Stream, group).Result()
	if err != nil {
		return 0, err
	}
	return summary.Count, nil
}

// PendingEntries trả về pending list của group, cũ nhất trước (XPENDING dạng extended).
func (q *StreamQueue) PendingEntries(ctx context.Context, group string) ([]PendingEntry, error) {
	rows, err := q.cfg.Commands.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: q.cfg.Stream, Group: group, Start: "-", End: "+", Count: 1000,
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
