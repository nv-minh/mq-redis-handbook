// Package lab implements a work queue on a Redis Stream with consumer groups: XADD, XREADGROUP,
// XACK and XAUTOCLAIM for taking over what a dead consumer left pending.
package lab

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// claimPage is how many entries one XAUTOCLAIM call scans and claims. It is the server default.
const claimPage = 100

// Config wires a StreamQueue.
type Config struct {
	// Commands runs the normal commands (XADD, XACK, XAUTOCLAIM, XPENDING, XGROUP).
	Commands *redis.Client
	// Blocking runs only XREADGROUP ... BLOCK: a blocked connection cannot serve other commands,
	// so keep it apart from Commands.
	Blocking *redis.Client
	// Stream is the key of the stream.
	Stream string
	// BlockTime is how long Consume waits for a new entry. Zero or less becomes one second.
	BlockTime time.Duration
}

// Message is one stream entry.
type Message struct {
	ID     string
	Fields map[string]string
}

// PendingEntry is one row of the pending list (PEL) of a group.
type PendingEntry struct {
	ID string
	// Consumer currently owns the entry.
	Consumer string
	// Idle is the time since the entry was last delivered.
	Idle time.Duration
	// DeliveryCount is how many times the entry was delivered (a claim counts as a delivery).
	DeliveryCount int64
}

// ClaimResult is what ClaimStale took over.
type ClaimResult struct {
	// Messages are the entries now owned by the claiming consumer.
	Messages []Message
	// DeletedIDs were in the pending list but no longer exist in the stream (Redis 7.0+).
	DeletedIDs []string
}

// StreamQueue is a work queue on one stream with consumer groups. Delivery is at-least-once:
// an entry stays in the pending list of its group until it is acked.
type StreamQueue struct {
	cfg Config
}

// NewStreamQueue returns a queue for the given clients and stream.
func NewStreamQueue(cfg Config) *StreamQueue {
	if cfg.BlockTime <= 0 {
		cfg.BlockTime = time.Second
	}
	return &StreamQueue{cfg: cfg}
}

// Publish appends an entry (XADD stream * field value ...) and returns its id.
func (q *StreamQueue) Publish(ctx context.Context, fields map[string]string) (string, error) {
	values := make(map[string]interface{}, len(fields))
	for k, v := range fields {
		values[k] = v
	}
	return q.cfg.Commands.XAdd(ctx, &redis.XAddArgs{Stream: q.cfg.Stream, Values: values}).Result()
}

// CreateGroup runs XGROUP CREATE ... MKSTREAM with start id "0": the group sees the whole stream,
// and the stream is created when missing. It is idempotent: a BUSYGROUP error (the group exists)
// is swallowed and every other error is returned.
func (q *StreamQueue) CreateGroup(ctx context.Context, group string) error {
	err := q.cfg.Commands.XGroupCreateMkStream(ctx, q.cfg.Stream, group, "0").Err()
	if err != nil && !strings.HasPrefix(err.Error(), "BUSYGROUP") {
		return err
	}
	return nil
}

// Consume reads up to count entries never delivered to the group, blocking up to BlockTime. They
// enter the pending list of consumer. It returns an empty slice when nothing arrived (the reply
// is nil, which go-redis reports as redis.Nil). The typed XReadGroup also extends the socket read
// deadline by the block time.
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

// Ack runs XACK and returns how many ids were removed from the pending list (0 if already acked).
func (q *StreamQueue) Ack(ctx context.Context, group, id string) (int64, error) {
	return q.cfg.Commands.XAck(ctx, q.cfg.Stream, group, id).Result()
}

// ClaimStale takes over every entry of the group that has been pending for longer than minIdle,
// whoever owns it, and makes consumer the owner. It follows the XAUTOCLAIM cursor until it
// returns 0-0. Claiming resets the idle time and adds one to the delivery count.
// It returns an empty result when nothing qualifies.
//
// The reply is parsed from the raw command on purpose. The typed rdb.XAutoClaim of go-redis
// v9.23.0 returns only (messages, nextCursor) and drops the third element, the ids that no longer
// exist in the stream, which would leave them in the pending list unnoticed.
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

// parseAutoClaim decodes the XAUTOCLAIM reply: [next cursor, [[id, [field, value, ...]], ...], [deleted id, ...]].
// RESP2 and RESP3 give the same shape for this command (all arrays, strings for ids and fields).
func parseAutoClaim(reply []interface{}) (next string, messages []Message, deleted []string, err error) {
	if len(reply) < 2 {
		return "", nil, nil, fmt.Errorf("XAUTOCLAIM reply has %d elements, want 3", len(reply))
	}
	next, ok := reply[0].(string)
	if !ok {
		return "", nil, nil, fmt.Errorf("XAUTOCLAIM cursor is %T, want string", reply[0])
	}
	entries, _ := reply[1].([]interface{})
	for _, raw := range entries {
		entry, ok := raw.([]interface{})
		if !ok || len(entry) != 2 {
			return "", nil, nil, fmt.Errorf("XAUTOCLAIM entry is %#v, want [id, fields]", raw)
		}
		id, _ := entry[0].(string)
		flat, isList := entry[1].([]interface{})
		if !isList {
			continue // entry without fields: deleted from the stream (pre 7.0 servers)
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

// PendingCount returns the number of entries in the pending list of the group (XPENDING summary).
func (q *StreamQueue) PendingCount(ctx context.Context, group string) (int64, error) {
	summary, err := q.cfg.Commands.XPending(ctx, q.cfg.Stream, group).Result()
	if err != nil {
		return 0, err
	}
	return summary.Count, nil
}

// PendingEntries returns the pending list of the group, oldest first (XPENDING extended form).
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
