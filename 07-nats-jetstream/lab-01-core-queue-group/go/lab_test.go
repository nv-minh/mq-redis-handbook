package lab

import (
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nv-minh/mq-redis-handbook/internal/testkit"
)

// dial mở một connection và đăng ký để t.Cleanup luôn đóng nó, kể cả khi test fail.
func dial(t *testing.T) *nats.Conn {
	t.Helper()
	nc, err := Connect()
	if err != nil {
		t.Fatalf("không kết nối được NATS (%s): %v. Đã chạy make up chưa?", URL(), err)
	}
	t.Cleanup(nc.Close)
	return nc
}

// settle dừng subscription bằng drain: server không gửi thêm, message còn trong buffer của client vẫn được xử lý nốt.
func settle(t *testing.T, receivers ...*Receiver) {
	t.Helper()
	for _, r := range receivers {
		if err := r.Subscription.Drain(); err != nil {
			t.Fatalf("drain subscription: %v", err)
		}
	}
	testkit.Eventually(t, 10*time.Second, func() (struct{}, bool) {
		for _, r := range receivers {
			if r.Subscription.IsValid() {
				return struct{}{}, false
			}
		}
		return struct{}{}, true
	})
}

func TestQueueGroupDeliversEachMessageToOneMember(t *testing.T) {
	const total = 60
	subject := testkit.UniqueName("lab01.orders")
	queue := testkit.UniqueName("packers")
	bodies := make([]string, total)
	for i := range bodies {
		bodies[i] = fmt.Sprintf("order-%d", i)
	}

	// Mỗi member là một connection riêng, giống ba worker chạy ở ba process khác nhau.
	// SubscribeCollect đã flush, nên server chắc chắn biết cả ba member trước khi publish.
	members := make([]*Receiver, 3)
	for i := range members {
		r, err := SubscribeCollect(dial(t), subject, queue)
		if err != nil {
			t.Fatalf("SubscribeCollect: %v", err)
		}
		members[i] = r
	}

	if err := PublishAll(dial(t), subject, bodies); err != nil {
		t.Fatalf("PublishAll: %v", err)
	}

	testkit.Eventually(t, 10*time.Second, func() (struct{}, bool) {
		n := 0
		for _, m := range members {
			n += len(m.Received())
		}
		return struct{}{}, n >= total
	})
	settle(t, members...)

	// Mỗi message tới đúng một member: tổng bằng N, không trùng, hợp của ba member là toàn bộ.
	// Server chọn member ngẫu nhiên nên KHÔNG assert việc chia đều.
	var all []string
	for _, m := range members {
		all = append(all, m.Received()...)
	}
	if len(all) != total {
		t.Fatalf("tổng message nhận được là %d, mong đợi %d", len(all), total)
	}
	seen := map[string]int{}
	for _, body := range all {
		seen[body]++
	}
	for body, count := range seen {
		if count != 1 {
			t.Errorf("message %q được giao %d lần, mong đợi đúng 1", body, count)
		}
	}
	sort.Strings(all)
	want := append([]string(nil), bodies...)
	sort.Strings(want)
	for i := range want {
		if all[i] != want[i] {
			t.Fatalf("hợp của ba member khác tập message đã publish: vị trí %d có %q, mong đợi %q", i, all[i], want[i])
		}
	}
}

func TestCoreNatsDropsMessagesWhenNoSubscriber(t *testing.T) {
	subject := testkit.UniqueName("lab01.events")
	publisher := dial(t)

	// Chưa có subscriber nào: publish vẫn thành công nhưng server bỏ message, không lưu gì cả.
	// PublishAll flush, nên server đã xử lý xong ba message này trước khi subscriber xuất hiện.
	if err := PublishAll(publisher, subject, []string{"old-1", "old-2", "old-3"}); err != nil {
		t.Fatalf("PublishAll: %v", err)
	}

	late, err := SubscribeCollect(dial(t), subject, "")
	if err != nil {
		t.Fatalf("SubscribeCollect: %v", err)
	}
	if err := PublishAll(publisher, subject, []string{"new-1"}); err != nil {
		t.Fatalf("PublishAll: %v", err)
	}

	// Cùng một connection nhận theo thứ tự: nếu server có giữ message cũ thì chúng đã đến trước new-1.
	testkit.Eventually(t, 10*time.Second, func() (struct{}, bool) {
		return struct{}{}, len(late.Received()) >= 1
	})
	settle(t, late)
	got := late.Received()
	if len(got) != 1 || got[0] != "new-1" {
		t.Fatalf("subscriber đến sau nhận %v, mong đợi đúng [new-1]", got)
	}
}
