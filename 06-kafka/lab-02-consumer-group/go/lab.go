// Package lab là lab 02 của chương Kafka: consumer group. Lab chứng minh partition được chia cho các member
// của group, rebalance chạy khi một member rời đi, và member vượt quá số partition ngồi không (client Go: segmentio/kafka-go).
package lab

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

// Brokers trả về danh sách broker. Lab đọc KAFKA_BROKERS (phân tách bằng dấu phẩy), mặc định là Kafka của `make up`.
func Brokers() []string {
	if v := os.Getenv("KAFKA_BROKERS"); v != "" {
		return strings.Split(v, ",")
	}
	return []string{"127.0.0.1:9092"}
}

func client() *kafka.Client {
	return &kafka.Client{Addr: kafka.TCP(Brokers()...), Timeout: 10 * time.Second}
}

// CreateTopic tạo topic với số partition xác định và replication factor 1 (broker chỉ có một node),
// rồi chờ tới khi metadata báo đủ partition và mọi partition đã có leader.
func CreateTopic(ctx context.Context, topic string, partitions int) error {
	c := client()
	resp, err := c.CreateTopics(ctx, &kafka.CreateTopicsRequest{
		Topics: []kafka.TopicConfig{{Topic: topic, NumPartitions: partitions, ReplicationFactor: 1}},
	})
	if err != nil {
		return fmt.Errorf("tạo topic %s: %w", topic, err)
	}
	if err := resp.Errors[topic]; err != nil {
		return fmt.Errorf("tạo topic %s: %w", topic, err)
	}
	return poll(ctx, 20*time.Second, func() (bool, error) {
		md, err := c.Metadata(ctx, &kafka.MetadataRequest{Topics: []string{topic}})
		if err != nil {
			return false, nil
		}
		if len(md.Topics) != 1 || md.Topics[0].Error != nil || len(md.Topics[0].Partitions) != partitions {
			return false, nil
		}
		for _, p := range md.Topics[0].Partitions {
			if p.Error != nil || p.Leader.ID < 0 {
				return false, nil
			}
		}
		return true, nil
	})
}

// DeleteTopic xóa topic. Topic không tồn tại không phải lỗi, để dọn dẹp luôn chạy được dù topic chưa kịp tạo.
func DeleteTopic(ctx context.Context, topic string) error {
	resp, err := client().DeleteTopics(ctx, &kafka.DeleteTopicsRequest{Topics: []string{topic}})
	if err != nil {
		return fmt.Errorf("xóa topic %s: %w", topic, err)
	}
	if err := resp.Errors[topic]; err != nil && !errors.Is(err, kafka.UnknownTopicOrPartition) {
		return fmt.Errorf("xóa topic %s: %w", topic, err)
	}
	return nil
}

// DeleteGroup xóa consumer group cùng offset đã commit của nó.
// Group phải rỗng nên hàm thử lại tới khi member cuối đã rời group. Group chưa từng tồn tại không phải lỗi.
func DeleteGroup(ctx context.Context, groupID string) error {
	c := client()
	return poll(ctx, 20*time.Second, func() (bool, error) {
		resp, err := c.DeleteGroups(ctx, &kafka.DeleteGroupsRequest{Addr: kafka.TCP(Brokers()...), GroupIDs: []string{groupID}})
		if err != nil {
			return false, nil
		}
		switch gerr := resp.Errors[groupID]; {
		case gerr == nil, errors.Is(gerr, kafka.GroupIdNotFound):
			return true, nil
		case errors.Is(gerr, kafka.NonEmptyGroup), errors.Is(gerr, kafka.GroupCoordinatorNotAvailable), errors.Is(gerr, kafka.NotCoordinatorForGroup):
			return false, nil
		default:
			return false, fmt.Errorf("xóa group %s: %w", groupID, gerr)
		}
	})
}

// poll gọi fn mỗi 100ms tới khi fn báo xong hoặc hết timeout. Lab dùng hàm này thay cho sleep cố định.
func poll(ctx context.Context, timeout time.Duration, fn func() (bool, error)) error {
	deadline := time.Now().Add(timeout)
	for {
		ok, err := fn()
		if err != nil {
			return err
		}
		if ok {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("điều kiện chưa đạt sau %s", timeout)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// Producer ghi message thẳng vào partition chỉ định. Writer dùng balancer đọc partition từ key của message.
type Producer struct {
	writer *kafka.Writer
}

// NewProducer tạo producer. RequireAll chờ ack của toàn bộ ISR (mặc định của kafka-go là RequireNone),
// BatchTimeout 10ms thay cho mặc định 1 giây để mỗi lần WriteMessages không bị trễ.
func NewProducer() *Producer {
	return &Producer{writer: &kafka.Writer{
		Addr:         kafka.TCP(Brokers()...),
		RequiredAcks: kafka.RequireAll,
		BatchTimeout: 10 * time.Millisecond,
		// Key của message là số partition cần ghi. Balancer trả đúng số đó, nên producer chọn partition thay cho hash.
		Balancer: kafka.BalancerFunc(func(msg kafka.Message, _ ...int) int {
			n, _ := strconv.Atoi(string(msg.Key))
			return n
		}),
	}}
}

// Close đóng writer và chờ các batch đang bay hoàn tất.
func (p *Producer) Close() error {
	return p.writer.Close()
}

// ProduceToPartition produce một message vào partition chỉ định và chờ ack của broker.
func (p *Producer) ProduceToPartition(ctx context.Context, topic string, partition int, value string) error {
	err := p.writer.WriteMessages(ctx, kafka.Message{Topic: topic, Key: []byte(strconv.Itoa(partition)), Value: []byte(value)})
	if err != nil {
		return fmt.Errorf("produce vào %s[%d]: %w", topic, partition, err)
	}
	return nil
}

// Message là một message mà member của group nhận được.
type Message struct {
	Partition int
	Value     string
}

// Member là một member đang chạy của consumer group.
type Member struct {
	group    *kafka.ConsumerGroup
	cancel   context.CancelFunc
	done     chan struct{}
	stopOnce sync.Once
}

// StartGroupMember chạy một member của consumer group và trả về ngay.
//
// Tương ứng với startGroupMember của lab TypeScript, chỉ khác hình thức: callback thay cho Promise.
//   - onAssign(partitions) được gọi mỗi lần member được gán partition, với TOÀN BỘ assignment của generation mới
//     (mảng rỗng khi member thừa). kafka-go chỉ có rebalance eager (classic), nên mỗi generation mang assignment đầy đủ.
//     Test chỉ nên assert trên assignment cuối cùng.
//   - onMessage nhận từng message. Hai callback chạy ở goroutine của member nên phải an toàn khi gọi đồng thời.
//   - Group đọc từ đầu (FirstOffset) khi chưa có offset đã commit, và commit offset sau mỗi message.
//
// Lab dùng kafka.ConsumerGroup thay cho kafka.Reader vì chỉ ConsumerGroup cho biết assignment của member.
// Reader với GroupID giấu assignment bên trong. Stop dừng member bằng LeaveGroup, nên group rebalance ngay.
func StartGroupMember(topic, groupID string, onAssign func(partitions []int), onMessage func(Message)) (*Member, error) {
	group, err := kafka.NewConsumerGroup(kafka.ConsumerGroupConfig{
		ID:          groupID,
		Brokers:     Brokers(),
		Topics:      []string{topic},
		StartOffset: kafka.FirstOffset,
	})
	if err != nil {
		return nil, fmt.Errorf("tạo consumer group %s: %w", groupID, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	m := &Member{group: group, cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(m.done)
		for {
			gen, err := group.Next(ctx)
			if err != nil {
				if ctx.Err() != nil || errors.Is(err, kafka.ErrGroupClosed) {
					return
				}
				// Lỗi còn lại là lỗi tạm thời của một lần join hoặc sync, ví dụ RebalanceInProgress khi nhiều member
				// vào group cùng lúc. Next báo lỗi đó qua kênh lỗi và thư viện tự thử lại, nên chỉ cần gọi Next tiếp.
				// Thoát ở đây sẽ làm member mất mọi generation sau đó (đã gặp khi bốn member join cùng lúc).
				continue
			}
			assignments := gen.Assignments[topic]
			ids := make([]int, 0, len(assignments))
			for _, a := range assignments {
				ids = append(ids, a.ID)
			}
			onAssign(ids)
			for _, a := range assignments {
				gen.Start(func(genCtx context.Context) {
					consumePartition(genCtx, gen, topic, a, onMessage)
				})
			}
		}
	}()
	return m, nil
}

// consumePartition đọc một partition tới khi generation kết thúc (rebalance hoặc dừng member), và commit sau mỗi message.
// Hàm return sớm khi gặp lỗi, và generation kết thúc theo, nên member tham gia lại group.
func consumePartition(ctx context.Context, gen *kafka.Generation, topic string, a kafka.PartitionAssignment, onMessage func(Message)) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   Brokers(),
		Topic:     topic,
		Partition: a.ID,
		MinBytes:  1,
		MaxBytes:  1 << 20,
		MaxWait:   250 * time.Millisecond,
	})
	defer func() { _ = r.Close() }()
	offset := a.Offset
	if offset < 0 {
		offset = kafka.FirstOffset
	}
	if err := r.SetOffset(offset); err != nil {
		return
	}
	for {
		msg, err := r.ReadMessage(ctx)
		if err != nil {
			return
		}
		onMessage(Message{Partition: msg.Partition, Value: string(msg.Value)})
		if err := gen.CommitOffsets(map[string]map[int]int64{topic: {a.ID: msg.Offset + 1}}); err != nil {
			return
		}
	}
}

// Stop dừng member: rời group (LeaveGroup), đợi mọi goroutine đọc kết thúc rồi trả về. Gọi nhiều lần cũng được.
func (m *Member) Stop() error {
	var err error
	m.stopOnce.Do(func() {
		m.cancel()
		err = m.group.Close()
		<-m.done
	})
	return err
}
