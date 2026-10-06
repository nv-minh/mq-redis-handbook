package lab

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/nv-minh/mq-redis-handbook/internal/testkit"
)

const partitions = 3

func testCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// newTopic tạo topic 3 partition và đăng ký xóa trong t.Cleanup, kể cả khi test fail.
// Cleanup chạy ngược thứ tự đăng ký, nên topic bị xóa sau khi group đã bị xóa.
func newTopic(t *testing.T) string {
	t.Helper()
	topic := testkit.UniqueName("lab03-events")
	// Đăng ký xóa trước khi tạo: DeleteTopic bỏ qua topic không tồn tại.
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := DeleteTopic(ctx, topic); err != nil {
			t.Errorf("xóa topic %s: %v", topic, err)
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := CreateTopic(ctx, topic, partitions); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	return topic
}

// newGroup sinh group id mới và đăng ký xóa group (cùng offset của nó) trong t.Cleanup.
// Mỗi test và mỗi lần replay dùng group id mới, để offset đã commit của lần trước không ảnh hưởng.
func newGroup(t *testing.T) string {
	t.Helper()
	group := testkit.UniqueName("lab03-group")
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := DeleteGroup(ctx, group); err != nil {
			t.Errorf("xóa group %s: %v", group, err)
		}
	})
	return group
}

func newProducer(t *testing.T) *Producer {
	t.Helper()
	p := NewProducer()
	t.Cleanup(func() {
		if err := p.Close(); err != nil {
			t.Errorf("đóng producer: %v", err)
		}
	})
	return p
}

func TestNewGroupWithEarliestReplaysAllMessages(t *testing.T) {
	ctx := testCtx(t)
	topic := newTopic(t)
	producer := newProducer(t)
	var expected []string
	// Sáu key khác nhau để message nằm trên nhiều partition.
	for i := 0; i < 12; i++ {
		value := fmt.Sprintf("event-%d", i)
		if err := producer.Produce(ctx, topic, fmt.Sprintf("key-%d", i%6), value); err != nil {
			t.Fatalf("Produce: %v", err)
		}
		expected = append(expected, value)
	}
	slices.Sort(expected)

	// Group mới chưa có offset đã commit nên StartOffset=FirstOffset cho đọc lại toàn bộ log.
	first, err := ReplayFromBeginning(ctx, topic, newGroup(t))
	if err != nil {
		t.Fatalf("ReplayFromBeginning: %v", err)
	}
	slices.Sort(first)
	if !slices.Equal(first, expected) {
		t.Errorf("group thứ nhất đọc được %v, mong đợi %v", first, expected)
	}

	// Kafka không xóa message sau khi đọc: group mới thứ hai cũng nhận đủ.
	second, err := ReplayFromBeginning(ctx, topic, newGroup(t))
	if err != nil {
		t.Fatalf("ReplayFromBeginning (lần hai): %v", err)
	}
	slices.Sort(second)
	if !slices.Equal(second, expected) {
		t.Errorf("group thứ hai đọc được %v, mong đợi %v", second, expected)
	}
}

func TestReplayOfAnEmptyTopicReturnsAnEmptyList(t *testing.T) {
	ctx := testCtx(t)
	topic := newTopic(t)
	// Topic rỗng: điều kiện dừng (tổng offset cuối trừ offset đầu bằng 0) thỏa ngay, không chờ message nào.
	got, err := ReplayFromBeginning(ctx, topic, newGroup(t))
	if err != nil {
		t.Fatalf("ReplayFromBeginning: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("topic rỗng nhưng đọc được %v", got)
	}
}

func TestCommitAfterProcessingLosesNothingOnCrash(t *testing.T) {
	ctx := testCtx(t)
	topic := newTopic(t)
	groupID := newGroup(t)
	producer := newProducer(t)
	if err := producer.Produce(ctx, topic, "order-1", "M1"); err != nil {
		t.Fatalf("Produce: %v", err)
	}

	// Xử lý M1 rồi "crash" TRƯỚC khi commit: offset của group không nhúc nhích.
	var processed []string
	if _, err := ConsumeOneThenCrash(ctx, topic, groupID, ProcessThenCommit, func(v string) { processed = append(processed, v) }); err != nil {
		t.Fatalf("ConsumeOneThenCrash: %v", err)
	}
	if !slices.Equal(processed, []string{"M1"}) {
		t.Fatalf("đã xử lý %v, mong đợi [M1]", processed)
	}

	// M2 cùng key nên cùng partition với M1, và Kafka giữ thứ tự trong một partition.
	if err := producer.Produce(ctx, topic, "order-1", "M2"); err != nil {
		t.Fatalf("Produce: %v", err)
	}

	// Consumer mới trong cùng group đọc lại từ offset đã commit, tức là từ M1: at-least-once, M1 được xử lý lần hai.
	got, err := ReceiveUntil(ctx, topic, groupID, "M2")
	if err != nil {
		t.Fatalf("ReceiveUntil: %v", err)
	}
	if !slices.Equal(got, []string{"M1", "M2"}) {
		t.Errorf("consumer mới nhận %v, mong đợi [M1 M2]", got)
	}
}

func TestCommitBeforeProcessingLosesMessageOnCrash(t *testing.T) {
	ctx := testCtx(t)
	topic := newTopic(t)
	groupID := newGroup(t)
	producer := newProducer(t)
	if err := producer.Produce(ctx, topic, "order-1", "M1"); err != nil {
		t.Fatalf("Produce: %v", err)
	}

	// Commit M1 rồi "crash" TRƯỚC khi xử lý: offset đã đi qua M1 nhưng M1 chưa từng được xử lý.
	var processed []string
	if _, err := ConsumeOneThenCrash(ctx, topic, groupID, CommitThenProcess, func(v string) { processed = append(processed, v) }); err != nil {
		t.Fatalf("ConsumeOneThenCrash: %v", err)
	}
	if len(processed) != 0 {
		t.Fatalf("đã xử lý %v, mong đợi không có gì", processed)
	}

	if err := producer.Produce(ctx, topic, "order-1", "M2"); err != nil {
		t.Fatalf("Produce: %v", err)
	}

	// Consumer mới bắt đầu sau offset đã commit nên không bao giờ thấy M1: at-most-once, M1 mất.
	// M2 cùng partition và đến sau M1, nên khi đã nhận M2 thì chắc chắn M1 sẽ không còn tới nữa.
	got, err := ReceiveUntil(ctx, topic, groupID, "M2")
	if err != nil {
		t.Fatalf("ReceiveUntil: %v", err)
	}
	if !slices.Equal(got, []string{"M2"}) {
		t.Errorf("consumer mới nhận %v, mong đợi [M2]", got)
	}
}
