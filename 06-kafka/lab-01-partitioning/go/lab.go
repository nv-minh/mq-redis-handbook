// Package lab là lab 01 của chương Kafka: partitioning. Lab chứng minh cùng key luôn vào cùng partition,
// thứ tự được giữ trong một partition, và key nil không bị dồn vào một partition (client Go: segmentio/kafka-go).
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
// Broker không tự tạo topic đúng số partition (topic tự tạo chỉ có 1 partition), nên lab luôn tạo tường minh.
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

// Result là vị trí mà broker báo trong ack của một message.
type Result struct {
	Partition int
	Offset    int64
}

// Producer bọc kafka.Writer và trả partition, offset của từng message lấy từ ack của broker.
type Producer struct {
	writer *kafka.Writer
}

// NewProducer tạo producer với balancer cho trước.
//
// Truyền nil để dùng balancer mặc định của kafka-go: đó là round-robin, nên cùng một key KHÔNG nằm trên một partition.
// Muốn "cùng key cùng partition" phải đặt balancer tường minh. Lab dùng `&kafka.Murmur2Balancer{}` vì nó khớp
// partitioner mặc định của Java và của wrapper kafka-javascript (murmur2), còn `&kafka.Hash{}` dùng FNV-1a nên cho partition khác.
//
// Writer đặt RequireAll (chờ ack của toàn bộ ISR, mặc định của kafka-go là RequireNone) và BatchTimeout 10ms
// (mặc định 1 giây làm mỗi lần WriteMessages đơn lẻ chậm tới 1 giây). Không dùng Async vì Async nuốt lỗi.
func NewProducer(balancer kafka.Balancer) *Producer {
	w := &kafka.Writer{
		Addr:         kafka.TCP(Brokers()...),
		Balancer:     balancer,
		RequiredAcks: kafka.RequireAll,
		BatchTimeout: 10 * time.Millisecond,
		// Completion nhận lại các message đã được điền Partition và Offset. WriterData mang kênh
		// để trả kết quả về đúng lời gọi produce. Callback chỉ gửi vào kênh có đệm nên không bao giờ bị chặn.
		Completion: func(messages []kafka.Message, err error) {
			if err != nil {
				return
			}
			for _, m := range messages {
				if ch, ok := m.WriterData.(chan Result); ok {
					ch <- Result{Partition: m.Partition, Offset: m.Offset}
				}
			}
		},
	}
	return &Producer{writer: w}
}

// Close đóng writer và chờ các batch đang bay hoàn tất.
func (p *Producer) Close() error {
	return p.writer.Close()
}

// ProduceKeyed produce một message có key và chờ ack của broker, rồi trả partition và offset trong ack.
func (p *Producer) ProduceKeyed(ctx context.Context, topic, key, value string) (Result, error) {
	return p.produce(ctx, topic, []byte(key), value)
}

// ProduceUnkeyed produce một message có key nil. Cách chọn partition phụ thuộc vào balancer của producer.
func (p *Producer) ProduceUnkeyed(ctx context.Context, topic, value string) (Result, error) {
	return p.produce(ctx, topic, nil, value)
}

func (p *Producer) produce(ctx context.Context, topic string, key []byte, value string) (Result, error) {
	// Kênh có đệm 1: callback Completion của writer không bao giờ bị chặn dù WriteMessages đã trả về lỗi.
	done := make(chan Result, 1)
	// Writer không có Topic mặc định nên topic nằm trong từng message.
	msg := kafka.Message{Topic: topic, Key: key, Value: []byte(value), WriterData: done}
	if err := p.writer.WriteMessages(ctx, msg); err != nil {
		return Result{}, fmt.Errorf("produce vào %s: %w", topic, err)
	}
	select {
	case res := <-done:
		return res, nil
	case <-ctx.Done():
		return Result{}, ctx.Err()
	}
}

// Record là một message đọc ra từ topic.
type Record struct {
	Partition int
	Offset    int64
	Key       string
	Value     string
}

// ReadTopic đọc mọi message đang có trong topic từ đầu, từng partition một bằng reader không có group.
// Điều kiện dừng không dùng sleep: mỗi partition được đọc tới offset cuối cùng mà ListOffsets báo lúc bắt đầu.
// Ngay sau khi tạo topic, ListOffsets có thể báo NotLeaderForPartition hoặc LeaderNotAvailable dù metadata đã có leader,
// nên bước lấy offset được thử lại tới khi hết lỗi hoặc hết 20 giây.
func ReadTopic(ctx context.Context, topic string) ([]Record, error) {
	c := client()
	var ranges []kafka.PartitionOffsets
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
		for _, po := range offsets.Topics[topic] {
			if errors.Is(po.Error, kafka.NotLeaderForPartition) || errors.Is(po.Error, kafka.LeaderNotAvailable) {
				return false, nil
			}
			if po.Error != nil {
				return false, po.Error
			}
		}
		ranges = offsets.Topics[topic]
		return true, nil
	})
	if err != nil {
		return nil, err
	}
	var records []Record
	for _, po := range ranges {
		count := po.LastOffset - po.FirstOffset
		if count == 0 {
			continue
		}
		part, err := readPartition(ctx, topic, po.Partition, po.FirstOffset, count)
		if err != nil {
			return nil, err
		}
		records = append(records, part...)
	}
	return records, nil
}

func readPartition(ctx context.Context, topic string, partition int, first, count int64) ([]Record, error) {
	r := kafka.NewReader(kafka.ReaderConfig{Brokers: Brokers(), Topic: topic, Partition: partition, MaxWait: 500 * time.Millisecond})
	defer func() { _ = r.Close() }()
	if err := r.SetOffset(first); err != nil {
		return nil, err
	}
	var records []Record
	for int64(len(records)) < count {
		m, err := r.ReadMessage(ctx)
		if err != nil {
			return nil, fmt.Errorf("đọc partition %d: %w", partition, err)
		}
		records = append(records, Record{Partition: m.Partition, Offset: m.Offset, Key: string(m.Key), Value: string(m.Value)})
	}
	return records, nil
}
