package lab

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/nv-minh/mq-redis-handbook/internal/testkit"
)

const partitions = 3

var all = []int{0, 1, 2}

// member là một member của group kèm những gì nó đã thấy: assignment mới nhất và các message đã nhận.
type member struct {
	handle *Member

	mu         sync.Mutex
	assignment []int // nil nếu member chưa được gán lần nào
	assigned   bool
	received   []Message
}

func (m *member) snapshot() (assignment []int, assigned bool, received []Message) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return slices.Clone(m.assignment), m.assigned, slices.Clone(m.received)
}

func testCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// newTopic tạo topic 3 partition và đăng ký xóa trong t.Cleanup, kể cả khi test fail.
// Cleanup chạy ngược thứ tự đăng ký, nên topic bị xóa sau khi group đã bị xóa và member đã dừng.
func newTopic(t *testing.T) string {
	t.Helper()
	topic := testkit.UniqueName("lab02-orders")
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

// newGroup sinh group id duy nhất và đăng ký xóa group (cùng offset của nó) trong t.Cleanup.
func newGroup(t *testing.T) string {
	t.Helper()
	group := testkit.UniqueName("lab02-group")
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := DeleteGroup(ctx, group); err != nil {
			t.Errorf("xóa group %s: %v", group, err)
		}
	})
	return group
}

// join chạy một member và đăng ký dừng nó trong t.Cleanup.
func join(t *testing.T, topic, groupID string) *member {
	t.Helper()
	m := &member{}
	handle, err := StartGroupMember(topic, groupID,
		func(parts []int) {
			m.mu.Lock()
			defer m.mu.Unlock()
			m.assignment = slices.Sorted(slices.Values(parts))
			m.assigned = true
		},
		func(msg Message) {
			m.mu.Lock()
			defer m.mu.Unlock()
			m.received = append(m.received, msg)
		})
	if err != nil {
		t.Fatalf("StartGroupMember: %v", err)
	}
	m.handle = handle
	t.Cleanup(func() {
		if err := handle.Stop(); err != nil {
			t.Errorf("dừng member: %v", err)
		}
	})
	return m
}

// stableAssignments chỉ trả về khi assignment của mọi member đã ổn định: mọi member đã được gán ít nhất một lần,
// các assignment rời nhau và hợp lại đúng ba partition. Trạng thái trung gian của rebalance không bao giờ thỏa điều kiện này.
func stableAssignments(t *testing.T, group ...*member) [][]int {
	t.Helper()
	return testkit.Eventually(t, 45*time.Second, func() ([][]int, bool) {
		var assignments [][]int
		var union []int
		for _, m := range group {
			a, assigned, _ := m.snapshot()
			if !assigned {
				return nil, false
			}
			assignments = append(assignments, a)
			union = append(union, a...)
		}
		slices.Sort(union)
		return assignments, slices.Equal(union, all)
	})
}

func totalReceived(group ...*member) int {
	n := 0
	for _, m := range group {
		_, _, received := m.snapshot()
		n += len(received)
	}
	return n
}

func TestPartitionsAreSplitAcrossGroupMembers(t *testing.T) {
	ctx := testCtx(t)
	topic := newTopic(t)
	groupID := newGroup(t)
	producer := NewProducer()
	t.Cleanup(func() {
		if err := producer.Close(); err != nil {
			t.Errorf("đóng producer: %v", err)
		}
	})
	m1 := join(t, topic, groupID)
	m2 := join(t, topic, groupID)

	assignments := stableAssignments(t, m1, m2)
	// Assignment rời nhau và phủ đủ ba partition (stableAssignments đã kiểm tra phần phủ, ở đây kiểm tra không chồng).
	for _, p := range assignments[0] {
		if slices.Contains(assignments[1], p) {
			t.Errorf("partition %d thuộc cả hai member: %v", p, assignments)
		}
	}
	if len(assignments[0]) == 0 || len(assignments[1]) == 0 {
		t.Errorf("mỗi member phải có ít nhất một partition, thực tế %v", assignments)
	}

	// Mỗi partition nhận một message, và message chỉ tới member đang sở hữu partition đó.
	for _, p := range all {
		if err := producer.ProduceToPartition(ctx, topic, p, fmt.Sprintf("p%d", p)); err != nil {
			t.Fatalf("ProduceToPartition(%d): %v", p, err)
		}
	}
	testkit.Eventually(t, 30*time.Second, func() (int, bool) {
		n := totalReceived(m1, m2)
		return n, n >= partitions
	})
	var values []string
	for _, m := range []*member{m1, m2} {
		assignment, _, received := m.snapshot()
		for _, r := range received {
			values = append(values, r.Value)
			if !slices.Contains(assignment, r.Partition) {
				t.Errorf("message %q của partition %d tới member chỉ sở hữu %v", r.Value, r.Partition, assignment)
			}
		}
	}
	slices.Sort(values)
	if !slices.Equal(values, []string{"p0", "p1", "p2"}) {
		t.Errorf("các message nhận được: %v", values)
	}
}

func TestRebalanceReassignsPartitionsWhenMemberLeaves(t *testing.T) {
	ctx := testCtx(t)
	topic := newTopic(t)
	groupID := newGroup(t)
	producer := NewProducer()
	t.Cleanup(func() {
		if err := producer.Close(); err != nil {
			t.Errorf("đóng producer: %v", err)
		}
	})
	m1 := join(t, topic, groupID)
	m2 := join(t, topic, groupID)
	stableAssignments(t, m1, m2)

	// Rời group một cách có chủ đích (LeaveGroup) để rebalance chạy ngay, không chờ session timeout (30 giây của kafka-go).
	// Member chết đột ngột thì coordinator chỉ phát hiện sau tối đa SessionTimeout.
	if err := m2.handle.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	// Chỉ assert trạng thái ổn định cuối cùng: member còn lại giữ cả ba partition.
	testkit.Eventually(t, 45*time.Second, func() (int, bool) {
		a, _, _ := m1.snapshot()
		return len(a), slices.Equal(a, all)
	})

	// Member còn lại thật sự nhận message của mọi partition, kể cả partition vừa nhận từ member đã rời.
	for _, p := range all {
		if err := producer.ProduceToPartition(ctx, topic, p, fmt.Sprintf("after-%d", p)); err != nil {
			t.Fatalf("ProduceToPartition(%d): %v", p, err)
		}
	}
	testkit.Eventually(t, 30*time.Second, func() (int, bool) {
		_, _, received := m1.snapshot()
		seen := map[string]bool{}
		for _, r := range received {
			seen[r.Value] = true
		}
		return len(seen), seen["after-0"] && seen["after-1"] && seen["after-2"]
	})
}

func TestMembersBeyondPartitionCountStayIdle(t *testing.T) {
	ctx := testCtx(t)
	topic := newTopic(t)
	groupID := newGroup(t)
	producer := NewProducer()
	t.Cleanup(func() {
		if err := producer.Close(); err != nil {
			t.Errorf("đóng producer: %v", err)
		}
	})
	var four []*member
	for range 4 {
		four = append(four, join(t, topic, groupID))
	}

	assignments := stableAssignments(t, four...)
	// Ba partition chia cho bốn member: ba member mỗi người một partition, đúng một member không có gì.
	var sizes []int
	for _, a := range assignments {
		sizes = append(sizes, len(a))
	}
	slices.Sort(sizes)
	if !slices.Equal(sizes, []int{0, 1, 1, 1}) {
		t.Fatalf("kích thước assignment %v, mong đợi [0 1 1 1]", sizes)
	}
	var idle *member
	for i, a := range assignments {
		if len(a) == 0 {
			idle = four[i]
		}
	}

	for _, p := range all {
		if err := producer.ProduceToPartition(ctx, topic, p, fmt.Sprintf("p%d", p)); err != nil {
			t.Fatalf("ProduceToPartition(%d): %v", p, err)
		}
	}
	testkit.Eventually(t, 30*time.Second, func() (int, bool) {
		n := totalReceived(four...)
		return n, n >= partitions
	})
	// Cả ba message đã tới ba member có partition, nên member thừa chắc chắn không nhận gì.
	if _, _, received := idle.snapshot(); len(received) != 0 {
		t.Errorf("member thừa nhận được %v", received)
	}
	for _, m := range four {
		assignment, _, received := m.snapshot()
		for _, r := range received {
			if !slices.Contains(assignment, r.Partition) {
				t.Errorf("message %q của partition %d tới member chỉ sở hữu %v", r.Value, r.Partition, assignment)
			}
		}
	}
}
