// Package lab là lab 03 của chương Kafka: offset và replay. Lab chứng minh group mới đọc lại được toàn bộ log,
// và thứ tự "xử lý" với "commit offset" quyết định at-least-once hay at-most-once khi consumer crash (client Go: segmentio/kafka-go).
package lab

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
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

// Producer ghi message có key. Cùng key thì cùng partition (Murmur2Balancer), nên giữ thứ tự theo key.
type Producer struct {
	writer *kafka.Writer
}

// NewProducer tạo producer. RequireAll chờ ack của toàn bộ ISR (mặc định của kafka-go là RequireNone),
// BatchTimeout 10ms thay cho mặc định 1 giây để mỗi lần WriteMessages không bị trễ.
func NewProducer() *Producer {
	return &Producer{writer: &kafka.Writer{
		Addr:         kafka.TCP(Brokers()...),
		Balancer:     &kafka.Murmur2Balancer{},
		RequiredAcks: kafka.RequireAll,
		BatchTimeout: 10 * time.Millisecond,
	}}
}

// Close đóng writer và chờ các batch đang bay hoàn tất.
func (p *Producer) Close() error {
	return p.writer.Close()
}

// Produce produce một message có key và chờ ack của broker.
func (p *Producer) Produce(ctx context.Context, topic, key, value string) error {
	err := p.writer.WriteMessages(ctx, kafka.Message{Topic: topic, Key: []byte(key), Value: []byte(value)})
	if err != nil {
		return fmt.Errorf("produce vào %s: %w", topic, err)
	}
	return nil
}

// newReader tạo reader thuộc group `groupID`, đọc từ đầu khi group chưa có offset đã commit (FirstOffset).
// Reader chỉ được dùng với FetchMessage: FetchMessage không commit, còn offset chỉ được lưu khi code gọi CommitMessages.
// (ReadMessage sẽ tự commit nên lab không dùng.) CommitInterval 0 nghĩa là CommitMessages commit đồng bộ.
func newReader(topic, groupID string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:     Brokers(),
		GroupID:     groupID,
		Topic:       topic,
		StartOffset: kafka.FirstOffset,
		MinBytes:    1,
		MaxBytes:    1 << 20,
		MaxWait:     250 * time.Millisecond,
	})
}

// pendingCount trả về tổng (offset cuối - offset đầu) của mọi partition: số message đang có trong topic.
// Ngay sau khi tạo topic, ListOffsets có thể báo NotLeaderForPartition hoặc LeaderNotAvailable dù metadata đã có leader
// (replica chưa sẵn sàng nhận request). Đó là lỗi tạm thời nên hàm thử lại tới khi hết lỗi hoặc hết 20 giây.
func pendingCount(ctx context.Context, topic string) (int64, error) {
	c := client()
	var total int64
	err := poll(ctx, 20*time.Second, func() (bool, error) {
		md, err := c.Metadata(ctx, &kafka.MetadataRequest{Topics: []string{topic}})
		if err != nil {
			return false, err
		}
		if len(md.Topics) != 1 {
			return false, fmt.Errorf("không có metadata của topic %s", topic)
		}
		req := map[string][]kafka.OffsetRequest{}
		for _, p := range md.Topics[0].Partitions {
			req[topic] = append(req[topic], kafka.FirstOffsetOf(p.ID), kafka.LastOffsetOf(p.ID))
		}
		offsets, err := c.ListOffsets(ctx, &kafka.ListOffsetsRequest{Topics: req})
		if err != nil {
			return false, err
		}
		total = 0
		for _, po := range offsets.Topics[topic] {
			if errors.Is(po.Error, kafka.NotLeaderForPartition) || errors.Is(po.Error, kafka.LeaderNotAvailable) {
				return false, nil
			}
			if po.Error != nil {
				return false, po.Error
			}
			total += po.LastOffset - po.FirstOffset
		}
		return true, nil
	})
	return total, err
}

// ReplayFromBeginning đọc lại toàn bộ log của topic bằng group `groupID` rồi trả về các value đã đọc
// (thứ tự giữa các partition không xác định).
//
// Group mới chưa có offset đã commit nên StartOffset=FirstOffset đưa reader về đầu mỗi partition.
// (Mặc định của Java và librdkafka là `latest`, kafka-go thì khác: mặc định là FirstOffset, lab đặt tường minh.)
// Hàm không commit gì, nên chạy lại với cùng group id vẫn đọc lại từ đầu; test vẫn dùng group id mới cho mỗi lần.
//
// Điều kiện dừng không dùng sleep: tổng (offset cuối - offset đầu) của các partition lúc bắt đầu là số message phải đọc.
// Topic rỗng cho 0 nên hàm trả về slice rỗng ngay mà không cần join group.
func ReplayFromBeginning(ctx context.Context, topic, groupID string) ([]string, error) {
	expected, err := pendingCount(ctx, topic)
	if err != nil {
		return nil, err
	}
	values := []string{}
	if expected == 0 {
		return values, nil
	}
	r := newReader(topic, groupID)
	defer func() { _ = r.Close() }()
	for int64(len(values)) < expected {
		msg, err := r.FetchMessage(ctx)
		if err != nil {
			return nil, fmt.Errorf("đọc topic %s: %w", topic, err)
		}
		values = append(values, string(msg.Value))
	}
	return values, nil
}

// ReceiveUntil dùng một consumer mới trong group `groupID` (đã có offset đã commit): đọc tiếp từ offset đó và trả về
// mọi value đã nhận cho tới khi gặp `last` (tính cả nó). Không commit gì.
// Dùng để quan sát consumer thay thế sau một lần crash bắt đầu từ đâu.
func ReceiveUntil(ctx context.Context, topic, groupID, last string) ([]string, error) {
	r := newReader(topic, groupID)
	defer func() { _ = r.Close() }()
	var values []string
	for {
		msg, err := r.FetchMessage(ctx)
		if err != nil {
			return nil, fmt.Errorf("đọc topic %s: %w", topic, err)
		}
		values = append(values, string(msg.Value))
		if string(msg.Value) == last {
			return values, nil
		}
	}
}

// CommitOrder là thứ tự hai bước "xử lý" và "commit offset" của consumer.
type CommitOrder int

const (
	// ProcessThenCommit (at-least-once): gọi process, rồi crash TRƯỚC khi commit.
	ProcessThenCommit CommitOrder = iota
	// CommitThenProcess (at-most-once): commit offset của message, rồi crash TRƯỚC khi gọi process.
	CommitThenProcess
)

// ConsumeOneThenCrash mô phỏng một consumer nhận một message rồi crash giữa hai bước xử lý và commit.
//
//   - ProcessThenCommit: gọi process(value), rồi crash trước khi commit.
//   - CommitThenProcess: CommitMessages (offset + 1, vì Kafka lưu "offset kế tiếp cần đọc"), rồi crash trước khi gọi process.
//
// Message đầu tiên nhận được là message bị "crash". process chạy ở goroutine của người gọi.
// Crash được mô phỏng bằng Reader.Close() ngay sau điểm crash, không commit thêm gì. Khác với kill -9 thật ở chỗ
// Close gửi LeaveGroup nên consumer thay thế vào group ngay, còn kill thật phải chờ SessionTimeout (30 giây).
// Offset mà group lưu giống hệt nhau trong cả hai trường hợp, và đó là thứ test cần kiểm chứng.
func ConsumeOneThenCrash(ctx context.Context, topic, groupID string, order CommitOrder, process func(value string)) (Message, error) {
	r := newReader(topic, groupID)
	defer func() { _ = r.Close() }()
	msg, err := r.FetchMessage(ctx)
	if err != nil {
		return Message{}, fmt.Errorf("đọc topic %s: %w", topic, err)
	}
	switch order {
	case ProcessThenCommit:
		process(string(msg.Value))
	case CommitThenProcess:
		if err := r.CommitMessages(ctx, msg); err != nil {
			return Message{}, fmt.Errorf("commit offset: %w", err)
		}
	}
	return Message{Partition: msg.Partition, Offset: msg.Offset, Value: string(msg.Value)}, nil
}

// Message là message bị "crash": vị trí và value của nó.
type Message struct {
	Partition int
	Offset    int64
	Value     string
}
