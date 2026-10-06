// Package lab shows Redis Pub/Sub: fire and forget fan-out with no history.
package lab

import (
	"context"
	"sync"

	"github.com/redis/go-redis/v9"
)

// Publish sends message on channel and returns the number of subscribers that received it.
// 0 means nobody was listening and the message is gone: Pub/Sub keeps no history.
func Publish(ctx context.Context, rdb *redis.Client, channel, message string) (int64, error) {
	return rdb.Publish(ctx, channel, message).Result()
}

// NumSubscribers returns the number of clients subscribed to channel with SUBSCRIBE
// (PUBSUB NUMSUB). go-redis parses the reply into a map of channel to count.
func NumSubscribers(ctx context.Context, rdb *redis.Client, channel string) (int64, error) {
	counts, err := rdb.PubSubNumSub(ctx, channel).Result()
	if err != nil {
		return 0, err
	}
	return counts[channel], nil
}

// Subscription collects the messages that arrive on one channel.
type Subscription struct {
	pubsub *redis.PubSub

	mu       sync.Mutex
	received []string
	done     chan struct{}
}

// Subscribe opens a subscription on channel. go-redis gives every PubSub its own dedicated
// connection, so it never shares a socket with the normal commands of rdb.
// It waits for the subscribe confirmation before returning, so once it returns every later
// Publish on the channel reaches this subscriber.
func Subscribe(ctx context.Context, rdb *redis.Client, channel string) (*Subscription, error) {
	pubsub := rdb.Subscribe(ctx, channel)
	// The first Receive is the confirmation of the subscription (a *redis.Subscription).
	if _, err := pubsub.Receive(ctx); err != nil {
		_ = pubsub.Close()
		return nil, err
	}
	s := &Subscription{pubsub: pubsub, done: make(chan struct{})}
	messages := pubsub.Channel()
	go func() {
		defer close(s.done)
		for message := range messages {
			s.mu.Lock()
			s.received = append(s.received, message.Payload)
			s.mu.Unlock()
		}
	}()
	return s, nil
}

// Messages returns the messages received so far, in arrival order.
func (s *Subscription) Messages() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.received...)
}

// Close unsubscribes and releases the dedicated connection.
func (s *Subscription) Close() error {
	err := s.pubsub.Close()
	<-s.done
	return err
}
