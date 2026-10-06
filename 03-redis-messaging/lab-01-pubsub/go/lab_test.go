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
		t.Fatalf("parse REDIS_URL: %v", err)
	}
	rdb := redis.NewClient(opts)
	t.Cleanup(func() { _ = rdb.Close() })
	return rdb
}

// newSubscriber subscribes on its own connection (go-redis gives every PubSub a dedicated
// one) and closes it when the test ends.
func newSubscriber(t *testing.T, rdb *redis.Client, channel string) *Subscription {
	t.Helper()
	sub, err := Subscribe(context.Background(), rdb, channel)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	t.Cleanup(func() { _ = sub.Close() })
	return sub
}

func mustPublish(t *testing.T, rdb *redis.Client, channel, message string) int64 {
	t.Helper()
	receivers, err := Publish(context.Background(), rdb, channel, message)
	if err != nil {
		t.Fatalf("Publish %q: %v", message, err)
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

	// Nobody is subscribed: PUBLISH replies with the number of receivers, which is 0,
	// and Redis does not keep the message anywhere.
	for i := 1; i <= 3; i++ {
		if got := mustPublish(t, rdb, channel, fmt.Sprintf("offline-%d", i)); got != 0 {
			t.Fatalf("PUBLISH with no subscriber replied %d, want 0", got)
		}
	}

	sub := newSubscriber(t, rdb, channel)
	// The subscribe confirmation is already in, and the server agrees: one subscriber.
	waitForSubscribers(t, rdb, channel, 1)

	// Now there is exactly one receiver, so this one is delivered.
	if got := mustPublish(t, rdb, channel, "online-1"); got != 1 {
		t.Fatalf("PUBLISH with one subscriber replied %d, want 1", got)
	}
	got := waitForMessages(t, sub, 1)

	// The three old messages never arrive: Pub/Sub has no history, the late subscriber only
	// sees what is published after it subscribed. Order on one connection means a stray old
	// message would have arrived before "online-1".
	if want := []string{"online-1"}; !slices.Equal(got, want) {
		t.Fatalf("messages = %v, want %v", got, want)
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
		// Fan-out: every PUBLISH reaches all 3 subscribers.
		if got := mustPublish(t, rdb, channel, want[i]); got != 3 {
			t.Fatalf("PUBLISH %q replied %d receivers, want 3", want[i], got)
		}
	}

	for i, sub := range subs {
		got := waitForMessages(t, sub, total)
		// Each subscriber got every message, in publish order, exactly once.
		if !slices.Equal(got, want) {
			t.Fatalf("subscriber %d messages = %v, want %v", i, got, want)
		}
	}
}
