package lab

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nv-minh/mq-redis-handbook/internal/testkit"
)

// gatedConsumer dừng lại ở mỗi message cho tới khi test cho đi tiếp.
type gatedConsumer struct {
	mu       sync.Mutex
	taken    []int
	took     chan struct{} // một tín hiệu cho mỗi message được lấy
	releases chan struct{}
}

func newGatedConsumer() *gatedConsumer {
	return &gatedConsumer{took: make(chan struct{}, 64), releases: make(chan struct{})}
}

func (g *gatedConsumer) handle(msg int) {
	g.mu.Lock()
	g.taken = append(g.taken, msg)
	g.mu.Unlock()
	g.took <- struct{}{}
	<-g.releases
}

func (g *gatedConsumer) takenSnapshot() []int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return slices.Clone(g.taken)
}

func TestPublishBlocksWhenQueueFull(t *testing.T) {
	for _, capacity := range []int{1, 3} {
		q := NewBoundedQueue[int](capacity)
		t.Cleanup(q.Close)
		for i := 0; i < capacity; i++ {
			if err := q.Publish(context.Background(), i); err != nil { // không lần publish nào bị chặn
				t.Fatalf("capacity %d: publish %d lỗi: %v", capacity, i, err)
			}
		}
		if q.Size() != capacity {
			t.Fatalf("capacity %d: size = %d, queue chưa đầy", capacity, q.Size())
		}

		// Queue đầy giữ publisher lại cho tới khi context bỏ cuộc: nó không return sớm.
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		err := q.Publish(ctx, capacity)
		cancel()
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("capacity %d: publish bị chặn trả về %v, mong đợi DeadlineExceeded", capacity, err)
		}
		if q.Size() != capacity {
			t.Fatalf("capacity %d: size sau publish bị bỏ dở = %d", capacity, q.Size())
		}

		// Consume một message tạo chỗ trống, và một publish đang chờ hoàn tất.
		var published atomic.Bool
		var publishErr atomic.Value
		go func() {
			if err := q.Publish(context.Background(), capacity); err != nil {
				publishErr.Store(err)
			}
			published.Store(true)
		}()
		consumer := newGatedConsumer()
		q.Consume(consumer.handle)
		testkit.Eventually(t, 2*time.Second, func() (bool, bool) { return published.Load(), published.Load() })
		if err, _ := publishErr.Load().(error); err != nil {
			t.Fatalf("capacity %d: publish được giải phóng bị lỗi: %v", capacity, err)
		}
		if got := consumer.takenSnapshot(); !slices.Equal(got, []int{0}) { // FIFO: cũ nhất trước
			t.Fatalf("capacity %d: taken = %v, mong đợi [0]", capacity, got)
		}
		if q.Size() != capacity { // chỗ trống vừa giải phóng được publish đang bị chặn lấp lại
			t.Fatalf("capacity %d: size = %d, mong đợi đầy", capacity, q.Size())
		}
		q.Close() // giải phóng goroutine queue của handler đang dừng; cũng cho handler đi tiếp
		close(consumer.releases)
	}
}

func TestPublishReturnsErrClosedAfterClose(t *testing.T) {
	q := NewBoundedQueue[int](1)
	q.Close()
	if err := q.Publish(context.Background(), 1); !errors.Is(err, ErrClosed) {
		t.Fatalf("err = %v, mong đợi ErrClosed", err)
	}
}

func TestSizeNeverExceedsCapacityWithSlowConsumer(t *testing.T) {
	const capacity, total = 3, 20
	q := NewBoundedQueue[int](capacity)
	t.Cleanup(q.Close)

	var maxSize atomic.Int64
	observe := func() {
		n := int64(q.Size())
		for {
			cur := maxSize.Load()
			if n <= cur || maxSize.CompareAndSwap(cur, n) {
				return
			}
		}
	}

	// Producer nhanh: 20 lần publish liên tiếp, mỗi lần đều chờ chỗ trống.
	var produced atomic.Int64
	go func() {
		for i := 0; i < total; i++ {
			if err := q.Publish(context.Background(), i); err != nil {
				t.Errorf("publish %d lỗi: %v", i, err)
				return
			}
			observe()
			produced.Add(1)
		}
	}()

	// Consumer chậm: nó giữ mỗi message cho tới khi test cho đi tiếp.
	consumer := newGatedConsumer()
	q.Consume(func(msg int) {
		observe()
		consumer.handle(msg)
	})

	for done := 0; done < total; done++ {
		select {
		case <-consumer.took:
		case <-time.After(2 * time.Second):
			t.Fatalf("consumer không lấy message %d kịp lúc", done)
		}
		observe()
		consumer.releases <- struct{}{}
	}
	testkit.Eventually(t, 2*time.Second, func() (bool, bool) {
		ok := produced.Load() == total
		return ok, ok
	})

	want := make([]int, total)
	for i := range want {
		want[i] = i
	}
	if got := consumer.takenSnapshot(); !slices.Equal(got, want) {
		t.Fatalf("taken = %v, mong đợi %v", got, want)
	}
	if got := maxSize.Load(); got > capacity {
		t.Fatalf("max size = %d, vượt capacity %d", got, capacity)
	} else if got != capacity { // giới hạn thực sự đã chạm tới, nên phép kiểm tra có ý nghĩa
		t.Fatalf("max size = %d, mong đợi producer lấp đầy queue tới %d", got, capacity)
	}
}
