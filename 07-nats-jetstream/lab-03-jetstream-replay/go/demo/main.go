// Command demo cho thấy consumer mới đọc lại lịch sử bằng DeliverAll,
// rồi chỉ đọc batch mới bằng DeliverByStartTime.
package main

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	lab "github.com/nv-minh/mq-redis-handbook/07-nats-jetstream/lab-03-jetstream-replay/go"
	"github.com/nv-minh/mq-redis-handbook/internal/testkit"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func show(msgs []lab.Replayed) string {
	out := make([]string, len(msgs))
	for i, m := range msgs {
		out[i] = m.Body
	}
	return strings.Join(out, ", ")
}

func run() (err error) {
	ctx := context.Background()
	name := testkit.UniqueName("demo-replay")
	defer func() {
		if tdErr := lab.Teardown(ctx, name); tdErr != nil && err == nil {
			err = tdErr
		}
	}()
	if err := lab.CreateReplayStream(ctx, name); err != nil {
		return err
	}
	older := []string{"old-0", "old-1", "old-2", "old-3"}
	newer := []string{"new-0", "new-1", "new-2", "new-3"}
	olderSeqs, err := lab.PublishBatch(ctx, name, older)
	if err != nil {
		return err
	}
	newerSeqs, err := lab.PublishBatch(ctx, name, newer)
	if err != nil {
		return err
	}
	fmt.Printf("Đã publish batch cũ (%s) rồi batch mới (%s).\n", strings.Join(older, ", "), strings.Join(newer, ", "))

	fmt.Println("--- DeliverAll: mỗi consumer mới đọc lại toàn bộ lịch sử ---")
	for _, label := range []string{"thứ nhất", "thứ hai"} {
		reader, err := lab.OpenReplay(ctx, name, lab.DeliverAll())
		if err != nil {
			return err
		}
		got, err := reader.Read(ctx)
		if err != nil {
			return err
		}
		fmt.Printf("Consumer %s (pending lúc tạo %d): %s\n", label, reader.PendingAtStart, show(got))
	}
	fmt.Println("Kết luận: đọc không xóa message (retention limits), nên consumer mới nào cũng thấy đủ lịch sử.")

	fmt.Println("--- DeliverByStartTime: bỏ qua message cũ hơn mốc ---")
	lastOlder, err := lab.StoredMessageTime(ctx, name, olderSeqs[len(olderSeqs)-1])
	if err != nil {
		return err
	}
	firstNewer, err := lab.StoredMessageTime(ctx, name, newerSeqs[0])
	if err != nil {
		return err
	}
	startTime := lab.StartTimeBetween(lastOlder, firstNewer)
	fmt.Printf("Timestamp server của message cũ cuối : %s\n", lastOlder.Format(time.RFC3339Nano))
	fmt.Printf("Timestamp server của message mới đầu : %s\n", firstNewer.Format(time.RFC3339Nano))
	fmt.Printf("Mốc start time (nằm giữa hai timestamp, lấy từ server): %s\n", startTime.Format(time.RFC3339Nano))
	byTime, err := lab.OpenReplay(ctx, name, lab.DeliverByStartTime(startTime))
	if err != nil {
		return err
	}
	got, err := byTime.Read(ctx)
	if err != nil {
		return err
	}
	fmt.Printf("Consumer by_start_time (pending lúc tạo %d): %s\n", byTime.PendingAtStart, show(got))
	fmt.Println("Kết luận: server chọn message đầu tiên có timestamp >= mốc. Mốc lấy từ timestamp server nên không phụ thuộc đồng hồ của client.")

	fmt.Println("--- DeliverByStartTime với mốc sau message cuối cùng ---")
	future := lastOlder.Add(time.Hour)
	fmt.Printf("Mốc = timestamp message cuối + 1 giờ = %s\n", future.Format(time.RFC3339Nano))
	empty, err := lab.OpenReplay(ctx, name, lab.DeliverByStartTime(future))
	if err != nil {
		return err
	}
	got, err = empty.Read(ctx)
	if err != nil {
		return err
	}
	fmt.Printf("Pending lúc tạo: %d, đọc được: [%s]\n", empty.PendingAtStart, show(got))
	lateSeqs, err := lab.PublishBatch(ctx, name, []string{"late-0"})
	if err != nil {
		return err
	}
	lateTime, err := lab.StoredMessageTime(ctx, name, lateSeqs[0])
	if err != nil {
		return err
	}
	fmt.Printf("Publish late-0 (timestamp server %s, nhỏ hơn mốc)\n", lateTime.Format(time.RFC3339Nano))
	if got, err = empty.Read(ctx); err != nil {
		return err
	}
	fmt.Printf("Consumer đó đọc được: [%s]\n", show(got))
	fmt.Println("Kết luận (đo trên server 2.15.0): mốc sau message cuối thì consumer bắt đầu ở message kế tiếp, giống deliver policy new.")
	return nil
}
