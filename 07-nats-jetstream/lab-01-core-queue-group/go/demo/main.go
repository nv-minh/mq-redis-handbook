// Command demo cho ba worker cùng một queue group chia nhau message,
// rồi cho thấy core NATS bỏ message khi chưa có subscriber.
package main

import (
	"fmt"
	"log"
	"time"

	"github.com/nats-io/nats.go"
	lab "github.com/nv-minh/mq-redis-handbook/07-nats-jetstream/lab-01-core-queue-group/go"
	"github.com/nv-minh/mq-redis-handbook/internal/testkit"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

// waitFor poll điều kiện tới khi đúng hoặc hết timeout (demo không dùng testing.T nên không dùng testkit.Eventually).
func waitFor(timeout time.Duration, cond func() bool) error {
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			return fmt.Errorf("điều kiện chưa đạt sau %s", timeout)
		}
		time.Sleep(20 * time.Millisecond)
	}
	return nil
}

// drain dừng subscription và chờ tới khi message còn trong buffer của client được xử lý hết.
func drain(receivers ...*lab.Receiver) error {
	for _, r := range receivers {
		if err := r.Subscription.Drain(); err != nil {
			return err
		}
	}
	return waitFor(10*time.Second, func() bool {
		for _, r := range receivers {
			if r.Subscription.IsValid() {
				return false
			}
		}
		return true
	})
}

func run() error {
	var conns []*nats.Conn
	defer func() {
		for _, nc := range conns {
			nc.Close()
		}
	}()
	open := func() (*nats.Conn, error) {
		nc, err := lab.Connect()
		if err != nil {
			return nil, fmt.Errorf("không kết nối được NATS (%s): %w. Đã chạy make up chưa?", lab.URL(), err)
		}
		conns = append(conns, nc)
		return nc, nil
	}

	const total = 30
	subject := testkit.UniqueName("demo.orders")
	queue := testkit.UniqueName("packers")

	fmt.Printf("--- queue group: 3 worker cùng group, publish %d message ---\n", total)
	workers := make([]*lab.Receiver, 3)
	for i := range workers {
		nc, err := open()
		if err != nil {
			return err
		}
		if workers[i], err = lab.SubscribeCollect(nc, subject, queue); err != nil {
			return err
		}
	}
	bodies := make([]string, total)
	for i := range bodies {
		bodies[i] = fmt.Sprintf("order-%d", i)
	}
	publisher, err := open()
	if err != nil {
		return err
	}
	if err := lab.PublishAll(publisher, subject, bodies); err != nil {
		return err
	}
	if err := waitFor(10*time.Second, func() bool {
		n := 0
		for _, w := range workers {
			n += len(w.Received())
		}
		return n >= total
	}); err != nil {
		return err
	}
	if err := drain(workers...); err != nil {
		return err
	}
	distinct := map[string]bool{}
	sum := 0
	for i, w := range workers {
		got := w.Received()
		fmt.Printf("worker %d nhận %d message\n", i+1, len(got))
		sum += len(got)
		for _, body := range got {
			distinct[body] = true
		}
	}
	fmt.Printf("Tổng %d message, %d message khác nhau: mỗi message tới đúng một worker.\n", sum, len(distinct))
	fmt.Println("Kết luận: server chọn member ngẫu nhiên nên số message mỗi worker lệch nhau, đừng kỳ vọng chia đều.")

	fmt.Println("--- core NATS không lưu message ---")
	eventSubject := testkit.UniqueName("demo.events")
	if err := lab.PublishAll(publisher, eventSubject, []string{"old-1", "old-2", "old-3"}); err != nil {
		return err
	}
	fmt.Println("Đã publish old-1, old-2, old-3 khi chưa có subscriber nào (publish vẫn thành công).")
	lateConn, err := open()
	if err != nil {
		return err
	}
	late, err := lab.SubscribeCollect(lateConn, eventSubject, "")
	if err != nil {
		return err
	}
	if err := lab.PublishAll(publisher, eventSubject, []string{"new-1"}); err != nil {
		return err
	}
	if err := waitFor(10*time.Second, func() bool { return len(late.Received()) >= 1 }); err != nil {
		return err
	}
	if err := drain(late); err != nil {
		return err
	}
	fmt.Printf("Subscriber đến sau chỉ nhận: %q\n", late.Received())
	fmt.Println("Kết luận: core NATS là at-most-once và không có persistence, muốn giữ message phải dùng JetStream (lab 02).")
	return nil
}
