package lab

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/nv-minh/mq-redis-handbook/internal/testkit"
)

// newName đăng ký tên trước khi tạo để t.Cleanup luôn dọn, kể cả khi bước tạo bị lỗi giữa chừng.
func newName(t *testing.T) string {
	t.Helper()
	name := testkit.UniqueName("lab02")
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if err := Teardown(ctx, name); err != nil {
			t.Errorf("teardown %s: %v", name, err)
		}
	})
	return name
}

func ctxFor(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func bodies(count int) []string {
	out := make([]string, count)
	for i := range out {
		out[i] = fmt.Sprintf("job-%d", i)
	}
	return out
}

func texts(msgs []jetstream.Msg) []string {
	out := make([]string, len(msgs))
	for i, m := range msgs {
		out[i] = string(m.Data())
	}
	return out
}

func deliveryCount(t *testing.T, m jetstream.Msg) int {
	t.Helper()
	meta, err := m.Metadata()
	if err != nil {
		t.Fatalf("Metadata: %v", err)
	}
	return int(meta.NumDelivered)
}

func mustFetch(t *testing.T, cons jetstream.Consumer, max int, expires time.Duration) []jetstream.Msg {
	t.Helper()
	msgs, err := FetchMessages(cons, max, expires)
	if err != nil {
		t.Fatalf("FetchMessages: %v", err)
	}
	return msgs
}

func mustInfo(t *testing.T, ctx context.Context, name string) *jetstream.ConsumerInfo {
	t.Helper()
	info, err := ConsumerInfo(ctx, name)
	if err != nil {
		t.Fatalf("ConsumerInfo: %v", err)
	}
	return info
}

func TestDurableConsumerResumesAfterReconnect(t *testing.T) {
	const total, consumed = 10, 4
	ctx := ctxFor(t)
	name := newName(t)
	cons, err := CreateStreamAndConsumer(ctx, name, Options{AckWait: 5 * time.Second, MaxDeliver: 5})
	if err != nil {
		t.Fatalf("CreateStreamAndConsumer: %v", err)
	}
	if err := PublishMessages(ctx, name, bodies(total)); err != nil {
		t.Fatalf("PublishMessages: %v", err)
	}

	// Nhận 4 message đầu và ack. DoubleAck chờ server xác nhận đã ghi ack, nên không còn ack nào đang bay khi ngắt kết nối.
	first := mustFetch(t, cons, consumed, 5*time.Second)
	if got := texts(first); !slices.Equal(got, bodies(total)[:consumed]) {
		t.Fatalf("lần nhận đầu: %v, mong đợi %v", got, bodies(total)[:consumed])
	}
	for _, m := range first {
		if err := m.DoubleAck(ctx); err != nil {
			t.Fatalf("DoubleAck: %v", err)
		}
	}

	// Ngắt kết nối thật: vị trí đọc nằm ở server (durable consumer), không nằm ở client.
	Disconnect(name)

	// Connection mới bind vào cùng durable: chỉ nhận phần còn lại, không nhận lại message đã ack.
	resumed, err := BindConsumer(ctx, name)
	if err != nil {
		t.Fatalf("BindConsumer: %v", err)
	}
	rest := mustFetch(t, resumed, total-consumed, 5*time.Second)
	if got := texts(rest); !slices.Equal(got, bodies(total)[consumed:]) {
		t.Fatalf("sau reconnect nhận %v, mong đợi %v", got, bodies(total)[consumed:])
	}
	for _, m := range rest {
		if n := deliveryCount(t, m); n != 1 {
			t.Errorf("message %s có delivery count %d, mong đợi 1", m.Data(), n)
		}
		if err := m.DoubleAck(ctx); err != nil {
			t.Fatalf("DoubleAck: %v", err)
		}
	}

	info := mustInfo(t, ctx, name)
	if info.NumAckPending != 0 || info.NumPending != 0 || info.AckFloor.Stream != total {
		t.Fatalf("info sau khi ack hết: ack_pending=%d pending=%d ack_floor=%d, mong đợi 0, 0, %d",
			info.NumAckPending, info.NumPending, info.AckFloor.Stream, total)
	}
}

func TestUnackedMessageIsRedeliveredAfterAckWait(t *testing.T) {
	const ackWait = time.Second
	ctx := ctxFor(t)
	name := newName(t)
	cons, err := CreateStreamAndConsumer(ctx, name, Options{AckWait: ackWait, MaxDeliver: 5})
	if err != nil {
		t.Fatalf("CreateStreamAndConsumer: %v", err)
	}
	if err := PublishMessages(ctx, name, []string{"job-1"}); err != nil {
		t.Fatalf("PublishMessages: %v", err)
	}

	first := mustFetch(t, cons, 1, 5*time.Second)
	if len(first) != 1 || string(first[0].Data()) != "job-1" || deliveryCount(t, first[0]) != 1 {
		t.Fatalf("lần giao đầu: %v", texts(first))
	}
	// Không ack: server coi worker đã chết khi hết AckWait. Đồng hồ monotonic của client chỉ dùng để chặn dưới.
	receivedAt := time.Now()

	// Poll bằng fetch có hạn (1 s): hết AckWait thì server giao lại cho pull request đang chờ.
	second := testkit.Eventually(t, 20*time.Second, func() (jetstream.Msg, bool) {
		msgs := mustFetch(t, cons, 1, time.Second)
		if len(msgs) == 0 {
			return nil, false
		}
		return msgs[0], true
	})
	elapsed := time.Since(receivedAt)

	if string(second.Data()) != "job-1" || deliveryCount(t, second) != 2 {
		t.Fatalf("lần giao lại: %s với delivery count %d, mong đợi job-1 và 2", second.Data(), deliveryCount(t, second))
	}
	// Chỉ khẳng định cận dưới. Timer của server chạy từ lúc giao nên client đo hụt vài ms, vì vậy chừa 20% dung sai.
	if elapsed < ackWait*8/10 {
		t.Fatalf("giao lại sau %s, nhanh hơn AckWait %s", elapsed, ackWait)
	}
	if err := second.DoubleAck(ctx); err != nil {
		t.Fatalf("DoubleAck: %v", err)
	}
	if info := mustInfo(t, ctx, name); info.NumAckPending != 0 {
		t.Fatalf("num_ack_pending = %d sau khi ack, mong đợi 0", info.NumAckPending)
	}
}

func TestMessageStopsAfterMaxDeliver(t *testing.T) {
	const maxDeliver = 3
	ctx := ctxFor(t)
	name := newName(t)
	cons, err := CreateStreamAndConsumer(ctx, name, Options{AckWait: 500 * time.Millisecond, MaxDeliver: maxDeliver})
	if err != nil {
		t.Fatalf("CreateStreamAndConsumer: %v", err)
	}
	// Subscribe advisory trước khi publish: WatchMaxDeliveries đã flush nên server chắc chắn thấy subscription.
	watch, err := WatchMaxDeliveries(name)
	if err != nil {
		t.Fatalf("WatchMaxDeliveries: %v", err)
	}
	if err := PublishMessages(ctx, name, []string{"poison"}); err != nil {
		t.Fatalf("PublishMessages: %v", err)
	}

	// Handler không bao giờ ack. Vòng lặp chạy tới khi server báo MAX_DELIVERIES, nên không cần đoán thời gian.
	var deliveries []int
	testkit.Eventually(t, 30*time.Second, func() (struct{}, bool) {
		for _, m := range mustFetch(t, cons, 1, time.Second) {
			deliveries = append(deliveries, deliveryCount(t, m))
		}
		return struct{}{}, len(watch.Advisories()) > 0
	})

	// Phía client: đúng maxDeliver lần giao với delivery count 1, 2, 3.
	if !slices.Equal(deliveries, []int{1, 2, 3}) {
		t.Fatalf("delivery count các lần giao: %v, mong đợi [1 2 3]", deliveries)
	}
	// Phía server: advisory đúng một lần, nói rõ message nào và đã giao bao nhiêu lần.
	advisories := watch.Advisories()
	if len(advisories) != 1 {
		t.Fatalf("có %d advisory, mong đợi 1", len(advisories))
	}
	if a := advisories[0]; a.Stream != name || a.Consumer != name || a.StreamSeq != 1 || a.Deliveries != maxDeliver {
		t.Fatalf("advisory = %+v", a)
	}

	// Sau advisory: không còn pending ack, không còn gì để giao, và một fetch có hạn không nhận được gì.
	if msgs := mustFetch(t, cons, 1, time.Second); len(msgs) != 0 {
		t.Fatalf("còn nhận được %v sau khi vượt MaxDeliver", texts(msgs))
	}
	info := mustInfo(t, ctx, name)
	if info.NumAckPending != 0 || info.NumPending != 0 || info.Delivered.Consumer != maxDeliver || info.Delivered.Stream != 1 {
		t.Fatalf("info: ack_pending=%d pending=%d delivered=%+v", info.NumAckPending, info.NumPending, info.Delivered)
	}
	// Round trip trên connection của watch: nếu còn advisory thứ hai thì nó đã tới trước PONG.
	if err := watch.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if got := len(watch.Advisories()); got != 1 {
		t.Fatalf("có %d advisory sau flush, mong đợi 1", got)
	}
}

func TestMaxDeliverLargerThanAttemptsDoesNotStopEarly(t *testing.T) {
	ctx := ctxFor(t)
	name := newName(t)
	cons, err := CreateStreamAndConsumer(ctx, name, Options{AckWait: 5 * time.Second, MaxDeliver: 5})
	if err != nil {
		t.Fatalf("CreateStreamAndConsumer: %v", err)
	}
	watch, err := WatchMaxDeliveries(name)
	if err != nil {
		t.Fatalf("WatchMaxDeliveries: %v", err)
	}
	if err := PublishMessages(ctx, name, []string{"flaky"}); err != nil {
		t.Fatalf("PublishMessages: %v", err)
	}

	// Hai lần đầu nak (giao lại ngay, không chờ AckWait), lần thứ ba ack: 3 lần giao chưa chạm maxDeliver = 5.
	var deliveries []int
	testkit.Eventually(t, 20*time.Second, func() (struct{}, bool) {
		for _, m := range mustFetch(t, cons, 1, time.Second) {
			n := deliveryCount(t, m)
			deliveries = append(deliveries, n)
			if n < 3 {
				if err := m.Nak(); err != nil {
					t.Fatalf("Nak: %v", err)
				}
				continue
			}
			if err := m.DoubleAck(ctx); err != nil {
				t.Fatalf("DoubleAck: %v", err)
			}
			return struct{}{}, true
		}
		return struct{}{}, false
	})

	if !slices.Equal(deliveries, []int{1, 2, 3}) {
		t.Fatalf("delivery count các lần giao: %v, mong đợi [1 2 3]", deliveries)
	}
	if err := watch.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if got := watch.Advisories(); len(got) != 0 {
		t.Fatalf("có advisory MAX_DELIVERIES %+v dù mới giao 3 trên tối đa 5", got)
	}
	if info := mustInfo(t, ctx, name); info.NumAckPending != 0 || info.AckFloor.Stream != 1 {
		t.Fatalf("info: ack_pending=%d ack_floor=%d, mong đợi 0 và 1", info.NumAckPending, info.AckFloor.Stream)
	}
}

func TestFetchOnEmptyStreamReturnsNothingWithinBound(t *testing.T) {
	ctx := ctxFor(t)
	name := newName(t)
	cons, err := CreateStreamAndConsumer(ctx, name, Options{AckWait: 5 * time.Second, MaxDeliver: 5})
	if err != nil {
		t.Fatalf("CreateStreamAndConsumer: %v", err)
	}

	// Stream rỗng: fetch có hạn trả về rỗng khi hết hạn (server trả 408), không treo và không trả lỗi.
	started := time.Now()
	if msgs := mustFetch(t, cons, 5, time.Second); len(msgs) != 0 {
		t.Fatalf("stream rỗng nhưng nhận được %v", texts(msgs))
	}
	if elapsed := time.Since(started); elapsed > 10*time.Second {
		t.Fatalf("fetch trên stream rỗng mất %s, mong đợi bị chặn bởi expires 1 s", elapsed)
	}
}

func TestInvalidAckWaitAndMaxDeliverAreRejected(t *testing.T) {
	// Server nhận ack_wait = 0 và im lặng đổi thành 30 s, max_deliver = 0 hoặc -2 thành -1 (đã đo).
	// Lab chặn từ phía client để giá trị sai không biến thành giá trị mặc định khác hẳn ý định.
	ctx := ctxFor(t)
	name := newName(t)
	cases := []struct {
		opts Options
		want string
	}{
		{Options{AckWait: 0, MaxDeliver: 3}, "AckWait"},
		{Options{AckWait: -5 * time.Second, MaxDeliver: 3}, "AckWait"},
		{Options{AckWait: time.Second, MaxDeliver: 0}, "MaxDeliver"},
		{Options{AckWait: time.Second, MaxDeliver: -2}, "MaxDeliver"},
	}
	for _, c := range cases {
		_, err := CreateStreamAndConsumer(ctx, name, c.opts)
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("opts %+v: err = %v, mong đợi lỗi nhắc tới %s", c.opts, err, c.want)
		}
	}
}
