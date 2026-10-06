// Command demo cho thấy một producer nhanh bị consumer chậm điều tiết tốc độ qua một queue có giới hạn.
package main

import (
	"context"
	"fmt"
	"time"

	lab "github.com/nv-minh/mq-redis-handbook/01-fundamentals/lab-02-backpressure/go"
)

func main() {
	const (
		capacity      = 3
		total         = 10
		consumerDelay = 40 * time.Millisecond
	)
	q := lab.NewBoundedQueue[int](capacity)
	defer q.Close()
	start := time.Now()
	at := func() string { return fmt.Sprintf("t=%4dms", time.Since(start).Milliseconds()) }

	consumed := make(chan struct{})
	q.Consume(func(msg int) {
		fmt.Printf("%s  consumer  nhận %d  (size=%d)\n", at(), msg, q.Size())
		time.Sleep(consumerDelay) // consumer chậm
		if msg == total {
			close(consumed)
		}
	})

	// Producer cố ý không tự chờ: bản thân Publish làm nó chậm lại khi queue đầy.
	for i := 1; i <= total; i++ {
		if err := q.Publish(context.Background(), i); err != nil {
			fmt.Println("publish:", err)
			return
		}
		fmt.Printf("%s  producer  đã nhận %d (size=%d/%d)\n", at(), i, q.Size(), capacity)
	}
	fmt.Printf("%s  producer  xong: nó bị consumer điều tiết tốc độ, queue không bao giờ vượt quá %d\n", at(), capacity)
	<-consumed
}
