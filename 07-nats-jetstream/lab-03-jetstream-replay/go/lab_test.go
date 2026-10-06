package lab

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/nv-minh/mq-redis-handbook/internal/testkit"
)

// newStream đăng ký tên trước khi tạo để t.Cleanup luôn dọn, kể cả khi bước tạo bị lỗi giữa chừng.
func newStream(t *testing.T) (context.Context, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	name := testkit.UniqueName("lab03")
	t.Cleanup(func() {
		cleanCtx, cleanCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanCancel()
		if err := Teardown(cleanCtx, name); err != nil {
			t.Errorf("teardown %s: %v", name, err)
		}
	})
	if err := CreateReplayStream(ctx, name); err != nil {
		t.Fatalf("CreateReplayStream: %v", err)
	}
	return ctx, name
}

func bodies(count int, prefix string) []string {
	out := make([]string, count)
	for i := range out {
		out[i] = fmt.Sprintf("%s-%d", prefix, i)
	}
	return out
}

func publish(t *testing.T, ctx context.Context, name string, bodies []string) []uint64 {
	t.Helper()
	seqs, err := PublishBatch(ctx, name, bodies)
	if err != nil {
		t.Fatalf("PublishBatch: %v", err)
	}
	return seqs
}

func open(t *testing.T, ctx context.Context, name string, policy Policy) *Reader {
	t.Helper()
	reader, err := OpenReplay(ctx, name, policy)
	if err != nil {
		t.Fatalf("OpenReplay: %v", err)
	}
	return reader
}

func read(t *testing.T, reader *Reader) []Replayed {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	msgs, err := reader.Read(ctx)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	return msgs
}

func bodiesOf(msgs []Replayed) []string {
	out := make([]string, len(msgs))
	for i, m := range msgs {
		out[i] = m.Body
	}
	return out
}

func storedTime(t *testing.T, ctx context.Context, name string, seq uint64) time.Time {
	t.Helper()
	ts, err := StoredMessageTime(ctx, name, seq)
	if err != nil {
		t.Fatalf("StoredMessageTime: %v", err)
	}
	return ts
}

func TestDeliverAllReplaysHistory(t *testing.T) {
	const total = 8
	ctx, name := newStream(t)
	published := bodies(total, "event")
	publish(t, ctx, name, published)

	// Consumer MỚI với DeliverAll đọc lại từ message đầu tiên, đúng thứ tự stream.
	first := read(t, open(t, ctx, name, DeliverAll()))
	if got := bodiesOf(first); !slices.Equal(got, published) {
		t.Fatalf("consumer thứ nhất nhận %v, mong đợi %v", got, published)
	}
	for i, m := range first {
		if m.Seq != uint64(i+1) {
			t.Fatalf("message %d có stream sequence %d, mong đợi %d", i, m.Seq, i+1)
		}
	}

	// Đọc không xóa message (retention limits): consumer mới thứ hai cũng nhận đủ N.
	second := read(t, open(t, ctx, name, DeliverAll()))
	if got := bodiesOf(second); !slices.Equal(got, published) {
		t.Fatalf("consumer thứ hai nhận %v, mong đợi %v", got, published)
	}
}

func TestDeliverByStartTimeSkipsOlderMessages(t *testing.T) {
	ctx, name := newStream(t)
	older := bodies(5, "old")
	newer := bodies(5, "new")
	olderSeqs := publish(t, ctx, name, older)
	newerSeqs := publish(t, ctx, name, newer)

	// Mốc thời gian lấy từ timestamp server gán cho message, không dùng đồng hồ của client:
	// đồng hồ của VM Docker có thể lệch đồng hồ của máy chạy test.
	lastOlder := storedTime(t, ctx, name, olderSeqs[len(olderSeqs)-1])
	firstNewer := storedTime(t, ctx, name, newerSeqs[0])
	// Điều kiện tiên quyết của test: server gán timestamp tăng thật sự giữa hai batch (độ phân giải nanosecond).
	if !firstNewer.After(lastOlder) {
		t.Fatalf("timestamp server không tăng giữa hai batch: %s rồi %s", lastOlder, firstNewer)
	}
	startTime := StartTimeBetween(lastOlder, firstNewer)

	// DeliverByStartTime chọn message đầu tiên có timestamp >= startTime, nên chỉ còn batch mới.
	reader := open(t, ctx, name, DeliverByStartTime(startTime))
	if reader.PendingAtStart != uint64(len(newer)) {
		t.Fatalf("pending lúc tạo consumer là %d, mong đợi %d", reader.PendingAtStart, len(newer))
	}
	if got := bodiesOf(read(t, reader)); !slices.Equal(got, newer) {
		t.Fatalf("consumer nhận %v, mong đợi %v", got, newer)
	}
}

func TestByStartTimeAfterLastMessageStartsAtNextMessage(t *testing.T) {
	ctx, name := newStream(t)
	seqs := publish(t, ctx, name, bodies(3, "old"))

	// Mốc nằm sau message cuối cùng một giờ (tính từ timestamp của server): không message nào có timestamp >= mốc.
	startTime := storedTime(t, ctx, name, seqs[len(seqs)-1]).Add(time.Hour)
	reader := open(t, ctx, name, DeliverByStartTime(startTime))
	// Đo trên server 2.15.0: consumer vẫn tạo được, không có gì để giao và vị trí bắt đầu ở sau message cuối.
	if reader.PendingAtStart != 0 {
		t.Fatalf("pending lúc tạo consumer là %d, mong đợi 0", reader.PendingAtStart)
	}
	if got := read(t, reader); len(got) != 0 {
		t.Fatalf("nhận %v, mong đợi không có gì", bodiesOf(got))
	}

	// Message publish sau đó vẫn được giao dù timestamp của nó nhỏ hơn mốc: consumer bắt đầu ở message kế tiếp (giống `new`).
	afterSeqs := publish(t, ctx, name, []string{"after"})
	if ts := storedTime(t, ctx, name, afterSeqs[0]); !ts.Before(startTime) {
		t.Fatalf("timestamp %s không nhỏ hơn mốc %s, test không còn chứng minh điều định chứng minh", ts, startTime)
	}
	if got := bodiesOf(read(t, reader)); !slices.Equal(got, []string{"after"}) {
		t.Fatalf("nhận %v, mong đợi [after]", got)
	}
}
