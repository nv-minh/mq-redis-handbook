package lab

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/nv-minh/mq-redis-handbook/internal/testkit"
	"github.com/segmentio/kafka-go"
)

const partitions = 3

// newTopic tạo topic 3 partition và đăng ký xóa trong t.Cleanup, kể cả khi test fail.
func newTopic(t *testing.T) string {
	t.Helper()
	topic := testkit.UniqueName("lab01-orders")
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

// newProducer tạo producer với balancer cho trước (nil là balancer mặc định của kafka-go) và đóng nó khi test kết thúc.
func newProducer(t *testing.T, balancer kafka.Balancer) *Producer {
	t.Helper()
	p := NewProducer(balancer)
	t.Cleanup(func() {
		if err := p.Close(); err != nil {
			t.Errorf("đóng producer: %v", err)
		}
	})
	return p
}

func testCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func TestSameKeyAlwaysGoesToSamePartition(t *testing.T) {
	ctx := testCtx(t)
	topic := newTopic(t)
	producer := newProducer(t, &kafka.Murmur2Balancer{})
	keys := []string{"alice", "bob", "carol", "dave", "erin"}
	seen := map[string]map[int]bool{}

	// Partition lấy từ ack của broker khi produce, không cần consume để biết message nằm ở đâu.
	for round := 0; round < 5; round++ {
		for _, key := range keys {
			res, err := producer.ProduceKeyed(ctx, topic, key, fmt.Sprintf("%s-%d", key, round))
			if err != nil {
				t.Fatalf("ProduceKeyed(%s): %v", key, err)
			}
			if res.Partition < 0 || res.Partition >= partitions {
				t.Fatalf("partition %d ngoài khoảng [0,%d)", res.Partition, partitions)
			}
			if seen[key] == nil {
				seen[key] = map[int]bool{}
			}
			seen[key][res.Partition] = true
		}
	}

	used := map[int]bool{}
	for _, key := range keys {
		if len(seen[key]) != 1 {
			t.Errorf("key %s phải nằm đúng một partition, thực tế %v", key, seen[key])
		}
		for p := range seen[key] {
			used[p] = true
		}
	}
	// Hash murmur2 là hàm xác định, nên năm key này luôn rơi vào nhiều hơn một partition (đã đo, không phải ngẫu nhiên).
	if len(used) < 2 {
		t.Errorf("năm key chỉ dùng %d partition, mong đợi ít nhất 2", len(used))
	}
}

func TestOrderIsPreservedWithinAPartition(t *testing.T) {
	ctx := testCtx(t)
	topic := newTopic(t)
	producer := newProducer(t, &kafka.Murmur2Balancer{})
	keys := []string{"a", "b", "c"}
	produced := map[string][]Result{}
	values := map[string][]string{}

	// Trộn ba key để partition nào cũng có nhiều key; thứ tự chỉ được bảo đảm trong từng partition.
	for n := 0; n < 8; n++ {
		for _, key := range keys {
			value := fmt.Sprintf("%s-%d", key, n)
			res, err := producer.ProduceKeyed(ctx, topic, key, value)
			if err != nil {
				t.Fatalf("ProduceKeyed(%s): %v", key, err)
			}
			produced[key] = append(produced[key], res)
			values[key] = append(values[key], value)
		}
	}

	for _, key := range keys {
		list := produced[key]
		for i, res := range list {
			if res.Partition != list[0].Partition {
				t.Errorf("key %s: message %d nằm partition %d, message đầu nằm partition %d", key, i, res.Partition, list[0].Partition)
			}
			if i > 0 && res.Offset <= list[i-1].Offset {
				t.Errorf("key %s: offset %d không tăng so với %d", key, res.Offset, list[i-1].Offset)
			}
		}
	}

	// Đọc lại topic từ đầu: với mỗi key, thứ tự value đọc ra phải đúng thứ tự đã produce.
	records, err := ReadTopic(ctx, topic)
	if err != nil {
		t.Fatalf("ReadTopic: %v", err)
	}
	for _, key := range keys {
		var gotValues []string
		var gotOffsets, wantOffsets []int64
		for _, r := range records {
			if r.Key != key {
				continue
			}
			gotValues = append(gotValues, r.Value)
			gotOffsets = append(gotOffsets, r.Offset)
			if r.Partition != produced[key][0].Partition {
				t.Errorf("key %s đọc ra ở partition %d, mong đợi %d", key, r.Partition, produced[key][0].Partition)
			}
		}
		for _, res := range produced[key] {
			wantOffsets = append(wantOffsets, res.Offset)
		}
		if !slices.Equal(gotValues, values[key]) {
			t.Errorf("key %s: đọc ra %v, mong đợi %v", key, gotValues, values[key])
		}
		if !slices.Equal(gotOffsets, wantOffsets) {
			t.Errorf("key %s: offset đọc ra %v, mong đợi %v", key, gotOffsets, wantOffsets)
		}
	}
}

func TestNullKeySpreadsAcrossPartitions(t *testing.T) {
	ctx := testCtx(t)
	topic := newTopic(t)
	// Round-robin tường minh: key nil được luân phiên qua các partition.
	producer := newProducer(t, &kafka.RoundRobin{})
	seen := map[int]bool{}
	sent := 0

	// Gửi từng message một cho tới khi thấy ít nhất hai partition khác nhau, không assert phân phối đều.
	testkit.Eventually(t, 45*time.Second, func() (int, bool) {
		res, err := producer.ProduceUnkeyed(ctx, topic, fmt.Sprintf("null-key-%d", sent))
		sent++
		if err != nil {
			t.Errorf("ProduceUnkeyed: %v", err)
			return 0, true
		}
		seen[res.Partition] = true
		return len(seen), len(seen) >= 2
	})
	if len(seen) < 2 {
		t.Errorf("key nil chỉ rơi vào %d partition", len(seen))
	}
}

// Bẫy của kafka-go: Writer không đặt Balancer sẽ round-robin, nên cùng một key KHÔNG nằm trên một partition.
func TestDefaultBalancerDoesNotKeepAKeyOnOnePartition(t *testing.T) {
	ctx := testCtx(t)
	topic := newTopic(t)
	producer := newProducer(t, nil)
	seen := map[int]bool{}

	for n := 0; n < 2*partitions; n++ {
		res, err := producer.ProduceKeyed(ctx, topic, "same-key", fmt.Sprintf("v-%d", n))
		if err != nil {
			t.Fatalf("ProduceKeyed: %v", err)
		}
		seen[res.Partition] = true
	}
	// Round-robin trên 6 message liên tiếp đi qua đủ cả 3 partition, dù key giống hệt nhau.
	if len(seen) != partitions {
		t.Errorf("balancer mặc định cho cùng một key đi qua %d partition, mong đợi %d (round-robin)", len(seen), partitions)
	}
}
