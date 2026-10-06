// Command demo cho thấy hai member chia ba partition, một member rời group thì member còn lại nhận hết,
// rồi bốn member trên ba partition (một member thừa ngồi không).
package main

import (
	"context"
	"fmt"
	"log"
	"slices"
	"strings"
	"sync"
	"time"

	lab "github.com/nv-minh/mq-redis-handbook/06-kafka/lab-02-consumer-group/go"
	"github.com/nv-minh/mq-redis-handbook/internal/testkit"
)

type member struct {
	name   string
	handle *lab.Member

	mu         sync.Mutex
	assignment []int
	assigned   bool
	received   []lab.Message
}

func (m *member) snapshot() ([]int, bool, []lab.Message) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return slices.Clone(m.assignment), m.assigned, slices.Clone(m.received)
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() (err error) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	topic := testkit.UniqueName("demo-group")
	if err := lab.CreateTopic(ctx, topic, 3); err != nil {
		return fmt.Errorf("không tạo được topic (%v): %w. Đã chạy make up chưa?", lab.Brokers(), err)
	}
	var groups []string
	var started []*member
	producer := lab.NewProducer()
	defer func() {
		for _, m := range started {
			if stopErr := m.handle.Stop(); stopErr != nil && err == nil {
				err = stopErr
			}
		}
		if closeErr := producer.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
		cleanup := context.Background()
		for _, g := range groups {
			if delErr := lab.DeleteGroup(cleanup, g); delErr != nil && err == nil {
				err = delErr
			}
		}
		if delErr := lab.DeleteTopic(cleanup, topic); delErr != nil && err == nil {
			err = delErr
		}
	}()
	fmt.Printf("topic %s có 3 partition\n", topic)

	join := func(groupID, name string) (*member, error) {
		m := &member{name: name}
		h, err := lab.StartGroupMember(topic, groupID,
			func(parts []int) {
				sorted := slices.Sorted(slices.Values(parts))
				m.mu.Lock()
				m.assignment, m.assigned = sorted, true
				m.mu.Unlock()
				fmt.Printf("  %s được gán partition %v\n", name, sorted)
			},
			func(msg lab.Message) {
				m.mu.Lock()
				m.received = append(m.received, msg)
				m.mu.Unlock()
			})
		if err != nil {
			return nil, err
		}
		m.handle = h
		started = append(started, m)
		return m, nil
	}
	// until poll tới khi cond đúng hoặc hết timeout.
	until := func(timeout time.Duration, cond func() bool) error {
		deadline := time.Now().Add(timeout)
		for !cond() {
			if time.Now().After(deadline) {
				return fmt.Errorf("điều kiện chưa đạt sau %s", timeout)
			}
			time.Sleep(50 * time.Millisecond)
		}
		return nil
	}
	stable := func(members ...*member) error {
		return until(45*time.Second, func() bool {
			var union []int
			for _, m := range members {
				a, assigned, _ := m.snapshot()
				if !assigned {
					return false
				}
				union = append(union, a...)
			}
			slices.Sort(union)
			return slices.Equal(union, []int{0, 1, 2})
		})
	}
	describe := func(members ...*member) string {
		var parts []string
		for _, m := range members {
			a, _, _ := m.snapshot()
			parts = append(parts, fmt.Sprintf("%s: %v", m.name, a))
		}
		return strings.Join(parts, " | ")
	}

	fmt.Println("--- hai member cùng một group ---")
	groupA := testkit.UniqueName("demo-group-a")
	groups = append(groups, groupA)
	m1, err := join(groupA, "member-1")
	if err != nil {
		return err
	}
	m2, err := join(groupA, "member-2")
	if err != nil {
		return err
	}
	if err := stable(m1, m2); err != nil {
		return err
	}
	fmt.Printf("assignment ổn định: %s\n", describe(m1, m2))
	for _, p := range []int{0, 1, 2} {
		if err := producer.ProduceToPartition(ctx, topic, p, fmt.Sprintf("msg-partition-%d", p)); err != nil {
			return err
		}
	}
	total := func(members ...*member) int {
		n := 0
		for _, m := range members {
			_, _, r := m.snapshot()
			n += len(r)
		}
		return n
	}
	if err := until(30*time.Second, func() bool { return total(m1, m2) >= 3 }); err != nil {
		return err
	}
	for _, m := range []*member{m1, m2} {
		_, _, received := m.snapshot()
		var values []string
		for _, r := range received {
			values = append(values, r.Value)
		}
		fmt.Printf("%s nhận: %s\n", m.name, strings.Join(values, ", "))
	}
	fmt.Println("Kết luận: mỗi partition chỉ có một chủ, message của partition nào tới đúng member sở hữu partition đó.")

	fmt.Println("--- member-2 rời group (LeaveGroup) ---")
	left := time.Now()
	if err := m2.handle.Stop(); err != nil {
		return err
	}
	err = until(45*time.Second, func() bool {
		a, _, _ := m1.snapshot()
		return slices.Equal(a, []int{0, 1, 2})
	})
	if err != nil {
		return err
	}
	fmt.Printf("member-1 giữ cả ba partition sau %d ms (một lần đo, gồm cả thời gian Stop)\n", time.Since(left).Milliseconds())
	fmt.Println("Kết luận: rebalance chuyển partition của member đã rời sang member còn lại. Nếu member chết đột ngột, coordinator phải chờ SessionTimeout (30 giây của kafka-go) mới phát hiện.")

	fmt.Println("--- bốn member trên ba partition (group mới) ---")
	groupB := testkit.UniqueName("demo-group-b")
	groups = append(groups, groupB)
	var four []*member
	for _, n := range []string{"a", "b", "c", "d"} {
		m, err := join(groupB, "member-"+n)
		if err != nil {
			return err
		}
		four = append(four, m)
	}
	if err := stable(four...); err != nil {
		return err
	}
	fmt.Printf("assignment ổn định: %s\n", describe(four...))
	fmt.Println("Kết luận: số member nhiều hơn số partition thì member thừa ngồi không, nên số partition là giới hạn của song song hóa.")
	return nil
}
