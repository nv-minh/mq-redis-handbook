package lab

import (
	"errors"
	"slices"
	"sync"
	"testing"

	"github.com/nv-minh/mq-redis-handbook/internal/testkit"
)

// scripted trả về rand() phát lại một kịch bản cố định, để việc mất message "ngẫu nhiên" do test quyết định.
func scripted(t *testing.T, values ...float64) func() float64 {
	i := 0
	return func() float64 {
		if i >= len(values) {
			// Được gọi từ goroutine của queue, nơi không được dùng Fatalf.
			t.Errorf("kịch bản rand đã hết sau %d giá trị", len(values))
			return 1
		}
		v := values[i]
		i++
		return v
	}
}

func count(ids []string, want string) int {
	n := 0
	for _, id := range ids {
		if id == want {
			n++
		}
	}
	return n
}

func TestAtMostOnceLosesMessagesWhenConsumerCrashes(t *testing.T) {
	m1, m2, m3 := testkit.UniqueName("m1"), testkit.UniqueName("m2"), testkit.UniqueName("m3")
	q := NewQueue(Options{Mode: AtMostOnce, LossRate: 0})
	var attempts, processed []string
	q.Consume(func(id string) error {
		attempts = append(attempts, id)
		// Consumer crash khi đang xử lý m2: handler trả về error trước khi xử lý xong.
		if id == m2 {
			return errors.New("consumer bị crash")
		}
		processed = append(processed, id)
		return nil
	})

	for _, id := range []string{m1, m2, m3} {
		q.Publish(id)
	}
	q.Drain()

	// m2 được giao đúng một lần và không bao giờ được giao lại, nên công việc của nó mất hẳn.
	if !slices.Equal(attempts, []string{m1, m2, m3}) {
		t.Fatalf("attempts = %v", attempts)
	}
	if !slices.Equal(processed, []string{m1, m3}) {
		t.Fatalf("processed = %v", processed)
	}
	if dead := q.DeadLetters(); len(dead) != 0 {
		t.Fatalf("dead = %v, mong đợi rỗng", dead)
	}
}

func TestAtMostOnceDropsEveryDeliveryWhenLossRateIsOne(t *testing.T) {
	q := NewQueue(Options{Mode: AtMostOnce, LossRate: 1})
	var seen []string
	q.Consume(func(id string) error { seen = append(seen, id); return nil })
	for _, id := range []string{"a", "b", "c"} {
		q.Publish(id)
	}
	q.Drain()
	if len(seen) != 0 {
		t.Fatalf("seen = %v, mong đợi rỗng", seen)
	}
}

func TestAtLeastOnceRedeliversWhenConsumerCrashes(t *testing.T) {
	id := testkit.UniqueName("m")
	q := NewQueue(Options{Mode: AtLeastOnce, LossRate: 0})
	attempts := 0
	var processed []string
	q.Consume(func(got string) error {
		attempts++
		if attempts == 1 {
			return errors.New("consumer bị crash") // không có ack, nên broker giao lại
		}
		processed = append(processed, got)
		return nil
	})
	q.Publish(id)
	q.Drain()
	if attempts != 2 || !slices.Equal(processed, []string{id}) {
		t.Fatalf("attempts = %d, processed = %v", attempts, processed)
	}
}

func TestAtLeastOnceDuplicatesWhenAckIsLost(t *testing.T) {
	// LossRate = 1: mọi ack đều mất, nên mỗi message được giao lại tới khi đạt MaxDeliveries.
	q := NewQueue(Options{Mode: AtLeastOnce, LossRate: 1, MaxDeliveries: 3})
	a, b := testkit.UniqueName("a"), testkit.UniqueName("b")
	var seen []string
	q.Consume(func(id string) error { seen = append(seen, id); return nil })
	q.Publish(a)
	q.Publish(b)
	q.Drain()

	if got := count(seen, a); got != 3 {
		t.Fatalf("số lần giao của a = %d, mong đợi 3", got)
	}
	if got := count(seen, b); got != 3 {
		t.Fatalf("số lần giao của b = %d, mong đợi 3", got)
	}
	// Lần nào handler cũng chạy xong; chỉ có ack bị mất.
	// Sau MaxDeliveries, broker bỏ cuộc và cho message vào dead.
	dead := q.DeadLetters()
	slices.Sort(dead)
	want := []string{a, b}
	slices.Sort(want)
	if !slices.Equal(dead, want) {
		t.Fatalf("dead = %v, mong đợi %v", dead, want)
	}
}

func TestAtLeastOnceStopsRedeliveringOnceAnAckGetsThrough(t *testing.T) {
	// Ack đầu mất (0 < 0.5), ack thứ hai tới nơi (0.9 >= 0.5): đúng một duplicate.
	q := NewQueue(Options{Mode: AtLeastOnce, LossRate: 0.5, Rand: scripted(t, 0, 0.9)})
	id := testkit.UniqueName("m")
	var seen []string
	q.Consume(func(got string) error { seen = append(seen, got); return nil })
	q.Publish(id)
	q.Drain()
	if !slices.Equal(seen, []string{id, id}) {
		t.Fatalf("seen = %v", seen)
	}
	if dead := q.DeadLetters(); len(dead) != 0 {
		t.Fatalf("dead = %v, mong đợi rỗng", dead)
	}
}

func TestIdempotentConsumerAppliesEachIDOnce(t *testing.T) {
	q := NewQueue(Options{Mode: AtLeastOnce, LossRate: 1, MaxDeliveries: 4})
	ids := []string{testkit.UniqueName("a"), testkit.UniqueName("b"), testkit.UniqueName("c")}
	var applied []string
	deliveries := 0
	handler := NewIdempotentHandler(func(id string) error {
		applied = append(applied, id)
		return nil
	})
	q.Consume(func(id string) error {
		deliveries++
		return handler(id)
	})
	for _, id := range ids {
		q.Publish(id)
	}
	q.Drain()

	if deliveries != len(ids)*4 { // duplicate thực sự đã tới
		t.Fatalf("số lần giao = %d, mong đợi %d", deliveries, len(ids)*4)
	}
	slices.Sort(applied) // vậy mà mỗi id chỉ được áp dụng một lần
	want := slices.Clone(ids)
	slices.Sort(want)
	if !slices.Equal(applied, want) {
		t.Fatalf("applied = %v, mong đợi %v", applied, want)
	}
}

func TestIdempotentConsumerRetriesAnIDWhoseApplyFailed(t *testing.T) {
	calls := 0
	var applied []string
	handler := NewIdempotentHandler(func(id string) error {
		calls++
		if calls == 1 {
			return errors.New("side effect failed")
		}
		applied = append(applied, id)
		return nil
	})
	if err := handler("x"); err == nil {
		t.Fatal("lần gọi đầu phải lỗi")
	}
	_ = handler("x") // không phải duplicate: lần thử đầu chưa bao giờ hoàn tất
	_ = handler("x") // giờ thì nó là duplicate
	if !slices.Equal(applied, []string{"x"}) || calls != 2 {
		t.Fatalf("applied = %v, calls = %d", applied, calls)
	}
}

// Publish từ nhiều goroutine trong lúc drain phải không có race (chạy với -race).
func TestQueueIsSafeForConcurrentPublish(t *testing.T) {
	q := NewQueue(Options{Mode: AtLeastOnce, LossRate: 0})
	var mu sync.Mutex
	got := 0
	q.Consume(func(string) error { mu.Lock(); got++; mu.Unlock(); return nil })
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); q.Publish(testkit.UniqueName("m")) }()
	}
	wg.Wait()
	q.Drain()
	mu.Lock()
	defer mu.Unlock()
	if got != 50 {
		t.Fatalf("got = %d, mong đợi 50", got)
	}
}
