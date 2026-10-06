// Package lab implements a reliable queue on Redis lists: BLMOVE into a per-consumer processing
// list, LREM to ack, and a recovery pass for consumers that died.
package lab

import (
	"context"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
)

// Config wires a ReliableQueue.
type Config struct {
	// Commands runs the normal commands (LPUSH, LREM, LMOVE).
	Commands *redis.Client
	// Blocking runs only BLMOVE: a blocked connection cannot serve other commands, so keep it
	// apart from Commands. go-redis keeps a pool per client, and one blocked BLMOVE holds a
	// pooled connection until it returns.
	Blocking *redis.Client
	// Queue is the key of the queue list. Processing lists live under "<Queue>:processing:<consumerID>".
	Queue string
	// BlockTimeout is how long Dequeue waits on an empty queue. Zero would block forever, so
	// NewReliableQueue replaces it with one second.
	BlockTimeout time.Duration
}

// ReliableQueue is a queue where a message taken by a consumer is never lost until it is acked.
//
// Enqueue: LPUSH (new messages enter at the left, consumers take from the right: FIFO).
// Dequeue: BLMOVE queue processing RIGHT LEFT, one atomic step that takes the oldest message and
// parks it in the consumer's processing list.
// Ack: LREM from the processing list. RecoverStale: move a dead consumer's list back to the queue.
// Delivery is at-least-once: a message recovered from a consumer that was only slow runs twice.
type ReliableQueue struct {
	cfg Config
}

// NewReliableQueue returns a queue for the given clients and keys.
func NewReliableQueue(cfg Config) *ReliableQueue {
	if cfg.BlockTimeout <= 0 {
		cfg.BlockTimeout = time.Second
	}
	return &ReliableQueue{cfg: cfg}
}

// ProcessingKey is the list that holds the messages consumerID has taken but not acked.
func (q *ReliableQueue) ProcessingKey(consumerID string) string {
	return q.cfg.Queue + ":processing:" + consumerID
}

// Enqueue appends msg to the queue.
func (q *ReliableQueue) Enqueue(ctx context.Context, msg string) error {
	return q.cfg.Commands.LPush(ctx, q.cfg.Queue, msg).Err()
}

// Dequeue takes the oldest message, blocking up to BlockTimeout. ok is false (and err nil) when
// the queue stayed empty for the whole timeout, which is BLMOVE replying nil. The message stays
// in the processing list of consumerID until Ack.
//
// The typed BLMove command makes go-redis extend the socket read deadline by the block time, so
// a client ReadTimeout shorter than BlockTimeout is fine here (a raw Do("BLMOVE", ...) is not).
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

// Ack removes msg from the processing list of consumerID. It returns false if msg was not there.
func (q *ReliableQueue) Ack(ctx context.Context, consumerID, msg string) (bool, error) {
	n, err := q.cfg.Commands.LRem(ctx, q.ProcessingKey(consumerID), 1, msg).Result()
	return n == 1, err
}

// RecoverStale puts every message in the processing list of consumerID back on the queue and
// returns how many it moved (0 for an empty list). Call it for a consumer you know is dead.
//
// Each LMOVE is atomic, so a crash of the recovering process cannot lose a message.
// LEFT to RIGHT: the processing list holds the newest message at its left end, so moving from
// its left end to the right end of the queue (the end consumers read from) leaves the oldest
// message pushed last, therefore consumed first again: original order kept.
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
