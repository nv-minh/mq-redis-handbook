package lab

import (
	"context"
	"fmt"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/nv-minh/mq-redis-handbook/internal/testkit"
	"github.com/redis/go-redis/v9"
)

func redisURL() string {
	if url := os.Getenv("REDIS_URL"); url != "" {
		return url
	}
	return "redis://127.0.0.1:6379"
}

func newClient(t *testing.T) *redis.Client {
	t.Helper()
	opts, err := redis.ParseURL(redisURL())
	if err != nil {
		t.Fatalf("REDIS_URL không hợp lệ: %v", err)
	}
	rdb := redis.NewClient(opts)
	t.Cleanup(func() { _ = rdb.Close() })
	return rdb
}

// newSubscriber subscribe trên connection riêng của nó (go-redis cấp cho mỗi PubSub một
// connection riêng) và đóng nó khi test kết thúc.
func newSubscriber(t *testing.T, rdb *redis.Client, channel string) *Subscription {
	t.Helper()
	sub, err := Subscribe(context.Background(), rdb, channel)
	if err != nil {
		t.Fatalf("Subscribe lỗi: %v", err)
	}
	t.Cleanup(func() { _ = sub.Close() })
	return sub
}

func mustPublish(t *testing.T, rdb *redis.Client, channel, message string) int64 {
	t.Helper()
	receivers, err := Publish(context.Background(), rdb, channel, message)
	if err != nil {
		t.Fatalf("Publish %q lỗi: %v", message, err)
	}
	return receivers
}

func waitForSubscribers(t *testing.T, rdb *redis.Client, channel string, want int64) {
	t.Helper()
	testkit.Eventually(t, 5*time.Second, func() (struct{}, bool) {
		n, err := NumSubscribers(context.Background(), rdb, channel)
		return struct{}{}, err == nil && n == want
	})
}

func waitForMessages(t *testing.T, sub *Subscription, want int) []string {
	t.Helper()
	return testkit.Eventually(t, 5*time.Second, func() ([]string, bool) {
		got := sub.Messages()
		return got, len(got) >= want
	})
}

func TestSubscriberMissesMessagesPublishedWhileOffline(t *testing.T) {
	rdb := newClient(t)
	channel := testkit.UniqueName("lab03-offline")

	// Chưa ai subscribe: PUBLISH trả về số receiver, tức là 0,
	// và Redis không giữ message ở đâu cả.
	for i := 1; i <= 3; i++ {
		if got := mustPublish(t, rdb, channel, fmt.Sprintf("offline-%d", i)); got != 0 {
			t.Fatalf("PUBLISH khi chưa có subscriber trả về %d, mong đợi 0", got)
		}
	}

	sub := newSubscriber(t, rdb, channel)
	// Xác nhận subscribe đã về, và server cũng đồng ý: có một subscriber.
	waitForSubscribers(t, rdb, channel, 1)

	// Giờ có đúng một receiver, nên message này được giao.
	if got := mustPublish(t, rdb, channel, "online-1"); got != 1 {
		t.Fatalf("PUBLISH khi có một subscriber trả về %d, mong đợi 1", got)
	}
	got := waitForMessages(t, sub, 1)

	// Ba message cũ không bao giờ tới: Pub/Sub không có lịch sử, subscriber vào muộn chỉ
	// thấy những gì được publish sau khi nó subscribe. Thứ tự trên một connection nghĩa là một
	// message cũ lạc tới sẽ đến trước "online-1".
	if want := []string{"online-1"}; !slices.Equal(got, want) {
		t.Fatalf("messages = %v, mong đợi %v", got, want)
	}
}

func TestAllSubscribersReceiveEachMessage(t *testing.T) {
	rdb := newClient(t)
	channel := testkit.UniqueName("lab03-fanout")
	const total = 20
	subs := []*Subscription{
		newSubscriber(t, rdb, channel),
		newSubscriber(t, rdb, channel),
		newSubscriber(t, rdb, channel),
	}
	waitForSubscribers(t, rdb, channel, 3)

	want := make([]string, total)
	for i := range want {
		want[i] = fmt.Sprintf("msg-%d", i)
		// Fan-out: mỗi PUBLISH tới cả 3 subscriber.
		if got := mustPublish(t, rdb, channel, want[i]); got != 3 {
			t.Fatalf("PUBLISH %q trả về %d receiver, mong đợi 3", want[i], got)
		}
	}

	for i, sub := range subs {
		got := waitForMessages(t, sub, total)
		// Mỗi subscriber nhận đủ mọi message, theo thứ tự publish, đúng một lần.
		if !slices.Equal(got, want) {
			t.Fatalf("subscriber %d messages = %v, mong đợi %v", i, got, want)
		}
	}
}
