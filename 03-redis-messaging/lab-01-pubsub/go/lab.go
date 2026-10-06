// Package lab minh họa Redis Pub/Sub: fan-out kiểu gửi rồi quên, không có lịch sử.
package lab

import (
	"context"
	"sync"

	"github.com/redis/go-redis/v9"
)

// Publish gửi message lên channel và trả về số subscriber đã nhận được nó.
// 0 nghĩa là không ai đang lắng nghe và message đã mất: Pub/Sub không giữ lịch sử.
func Publish(ctx context.Context, rdb *redis.Client, channel, message string) (int64, error) {
	return rdb.Publish(ctx, channel, message).Result()
}

// NumSubscribers trả về số client đang subscribe channel bằng SUBSCRIBE
// (PUBSUB NUMSUB). go-redis parse reply thành map từ channel sang số đếm.
func NumSubscribers(ctx context.Context, rdb *redis.Client, channel string) (int64, error) {
	counts, err := rdb.PubSubNumSub(ctx, channel).Result()
	if err != nil {
		return 0, err
	}
	return counts[channel], nil
}

// Subscription gom các message tới trên một channel.
type Subscription struct {
	pubsub *redis.PubSub

	mu       sync.Mutex
	received []string
	done     chan struct{}
}

// Subscribe mở một subscription trên channel. go-redis cấp cho mỗi PubSub một connection
// riêng, nên nó không bao giờ dùng chung socket với các lệnh thường của rdb.
// Hàm chờ xác nhận subscribe rồi mới return, nên khi hàm return, mọi Publish về sau
// trên channel đều tới được subscriber này.
func Subscribe(ctx context.Context, rdb *redis.Client, channel string) (*Subscription, error) {
	pubsub := rdb.Subscribe(ctx, channel)
	// Receive đầu tiên là xác nhận của subscription (một *redis.Subscription).
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

// Messages trả về các message đã nhận tới giờ, theo thứ tự đến.
func (s *Subscription) Messages() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.received...)
}

// Close hủy subscribe và giải phóng connection riêng.
func (s *Subscription) Close() error {
	err := s.pubsub.Close()
	<-s.done
	return err
}
