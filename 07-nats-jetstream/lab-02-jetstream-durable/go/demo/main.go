// Command demo cho thấy durable consumer resume sau khi mất kết nối, redelivery khi hết AckWait,
// và message bị bỏ khi vượt MaxDeliver.
package main

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	lab "github.com/nv-minh/mq-redis-handbook/07-nats-jetstream/lab-02-jetstream-durable/go"
	"github.com/nv-minh/mq-redis-handbook/internal/testkit"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func texts(msgs []jetstream.Msg) string {
	out := make([]string, len(msgs))
	for i, m := range msgs {
		out[i] = string(m.Data())
	}
	return strings.Join(out, ", ")
}

func deliveries(m jetstream.Msg) (uint64, error) {
	meta, err := m.Metadata()
	if err != nil {
		return 0, err
	}
	return meta.NumDelivered, nil
}

// fetchOne poll bằng fetch có hạn tới khi nhận được một message hoặc hết timeout.
func fetchOne(cons jetstream.Consumer, timeout time.Duration) (jetstream.Msg, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		msgs, err := lab.FetchMessages(cons, 1, time.Second)
		if err != nil {
			return nil, err
		}
		if len(msgs) > 0 {
			return msgs[0], nil
		}
	}
	return nil, fmt.Errorf("không nhận được message sau %s", timeout)
}

func run() (err error) {
	ctx := context.Background()
	var names []string
	newName := func(prefix string) string {
		name := testkit.UniqueName(prefix)
		names = append(names, name)
		return name
	}
	defer func() {
		for _, name := range names {
			if tdErr := lab.Teardown(ctx, name); tdErr != nil && err == nil {
				err = tdErr
			}
		}
	}()

	fmt.Println("--- durable consumer: mất kết nối giữa chừng ---")
	name := newName("demo-resume")
	cons, err := lab.CreateStreamAndConsumer(ctx, name, lab.Options{AckWait: 5 * time.Second, MaxDeliver: 5})
	if err != nil {
		return err
	}
	if err := lab.PublishMessages(ctx, name, []string{"job-0", "job-1", "job-2", "job-3", "job-4", "job-5"}); err != nil {
		return err
	}
	first, err := lab.FetchMessages(cons, 3, 5*time.Second)
	if err != nil {
		return err
	}
	for _, m := range first {
		if err := m.DoubleAck(ctx); err != nil {
			return err
		}
	}
	fmt.Printf("Worker 1 nhận và ack: %s\n", texts(first))
	lab.Disconnect(name)
	fmt.Println("Worker 1 mất kết nối (connection bị đóng đột ngột).")
	resumed, err := lab.BindConsumer(ctx, name)
	if err != nil {
		return err
	}
	rest, err := lab.FetchMessages(resumed, 3, 5*time.Second)
	if err != nil {
		return err
	}
	for _, m := range rest {
		if err := m.DoubleAck(ctx); err != nil {
			return err
		}
	}
	fmt.Printf("Worker 2 bind lại cùng durable, chỉ nhận phần còn lại: %s\n", texts(rest))
	fmt.Println("Kết luận: vị trí đọc nằm trên server, nên message đã ack không bị giao lại và message chưa đọc không bị mất.")

	fmt.Println("--- AckWait: worker nhận message nhưng không ack ---")
	const ackWait = time.Second
	name = newName("demo-ackwait")
	cons, err = lab.CreateStreamAndConsumer(ctx, name, lab.Options{AckWait: ackWait, MaxDeliver: 5})
	if err != nil {
		return err
	}
	if err := lab.PublishMessages(ctx, name, []string{"job-1"}); err != nil {
		return err
	}
	msg, err := fetchOne(cons, 5*time.Second)
	if err != nil {
		return err
	}
	receivedAt := time.Now()
	n, err := deliveries(msg)
	if err != nil {
		return err
	}
	fmt.Printf("Lần giao 1: %s (delivery count %d), worker không ack\n", msg.Data(), n)
	msg, err = fetchOne(cons, 20*time.Second)
	if err != nil {
		return err
	}
	if n, err = deliveries(msg); err != nil {
		return err
	}
	fmt.Printf("Lần giao 2: %s (delivery count %d) sau %d ms, AckWait là %d ms\n",
		msg.Data(), n, time.Since(receivedAt).Milliseconds(), ackWait.Milliseconds())
	if err := msg.DoubleAck(ctx); err != nil {
		return err
	}
	fmt.Println("Kết luận: hết AckWait mà chưa ack thì server coi worker đã chết và giao lại, nên xử lý phải idempotent.")

	fmt.Println("--- MaxDeliver = 3: message độc không bao giờ được ack ---")
	name = newName("demo-maxdeliver")
	cons, err = lab.CreateStreamAndConsumer(ctx, name, lab.Options{AckWait: 500 * time.Millisecond, MaxDeliver: 3})
	if err != nil {
		return err
	}
	watch, err := lab.WatchMaxDeliveries(name)
	if err != nil {
		return err
	}
	if err := lab.PublishMessages(ctx, name, []string{"poison"}); err != nil {
		return err
	}
	deadline := time.Now().Add(30 * time.Second)
	for len(watch.Advisories()) == 0 {
		if time.Now().After(deadline) {
			return fmt.Errorf("không nhận được advisory MAX_DELIVERIES sau 30s")
		}
		msgs, err := lab.FetchMessages(cons, 1, time.Second)
		if err != nil {
			return err
		}
		for _, m := range msgs {
			count, err := deliveries(m)
			if err != nil {
				return err
			}
			fmt.Printf("Lần giao %d: %s, worker không ack\n", count, m.Data())
		}
	}
	advisory := watch.Advisories()[0]
	fmt.Printf("Advisory MAX_DELIVERIES: stream_seq=%d, deliveries=%d\n", advisory.StreamSeq, advisory.Deliveries)
	extra, err := lab.FetchMessages(cons, 1, time.Second)
	if err != nil {
		return err
	}
	fmt.Printf("Fetch thêm một lần: nhận %d message\n", len(extra))
	info, err := lab.ConsumerInfo(ctx, name)
	if err != nil {
		return err
	}
	fmt.Printf("Thông tin consumer: num_pending=%d, num_ack_pending=%d, num_redelivered=%d, delivered.stream_seq=%d, ack_floor.stream_seq=%d\n",
		info.NumPending, info.NumAckPending, info.NumRedelivered, info.Delivered.Stream, info.AckFloor.Stream)
	fmt.Println("Kết luận: sau MaxDeliver server ngừng giao và không có dead-letter queue, advisory là dấu vết duy nhất (message vẫn nằm trong stream).")
	return nil
}
