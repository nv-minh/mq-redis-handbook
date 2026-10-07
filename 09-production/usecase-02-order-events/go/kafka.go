package lab

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/segmentio/kafka-go"
)

// Brokers trả về danh sách broker. Use case đọc KAFKA_BROKERS (phân tách bằng dấu phẩy), mặc định là Kafka của `make up`.
func Brokers() []string {
	if v := os.Getenv("KAFKA_BROKERS"); v != "" {
		return strings.Split(v, ",")
	}
	return []string{"127.0.0.1:9092"}
}

func client() *kafka.Client {
	return &kafka.Client{Addr: kafka.TCP(Brokers()...), Timeout: 10 * time.Second}
}

// poll gọi fn mỗi 100ms tới khi fn báo xong hoặc hết timeout. Code dùng hàm này thay cho sleep cố định.
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

// PartitionOffsets là offset đầu và offset cuối của một partition.
// Last là log end offset: offset của message KẾ TIẾP sẽ được ghi.
type PartitionOffsets struct {
	Partition int
	First     int64
	Last      int64
}

// topicOffsets trả offset đầu và cuối của mọi partition. Ngay sau khi tạo topic, ListOffsets có thể báo
// NotLeaderForPartition hoặc LeaderNotAvailable dù metadata đã có leader (replica chưa sẵn sàng nhận request).
// Đó là lỗi tạm thời nên hàm thử lại tới khi hết lỗi hoặc hết 10 giây.
func topicOffsets(ctx context.Context, topic string) ([]PartitionOffsets, error) {
	c := client()
	var out []PartitionOffsets
	err := poll(ctx, 10*time.Second, func() (bool, error) {
		md, err := c.Metadata(ctx, &kafka.MetadataRequest{Topics: []string{topic}})
		if err != nil {
			return false, err
		}
		if len(md.Topics) != 1 || md.Topics[0].Error != nil {
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
		out = out[:0]
		for _, po := range offsets.Topics[topic] {
			if errors.Is(po.Error, kafka.NotLeaderForPartition) || errors.Is(po.Error, kafka.LeaderNotAvailable) {
				return false, nil
			}
			if po.Error != nil {
				return false, po.Error
			}
			out = append(out, PartitionOffsets{Partition: po.Partition, First: po.FirstOffset, Last: po.LastOffset})
		}
		return true, nil
	})
	return out, err
}

// ConsumerLag trả lag của một group trên một topic: tổng theo partition của (log end offset trừ offset đã commit của group).
//
//   - Log end offset lấy từ ListOffsets (Client.ListOffsets, LastOffsetOf).
//   - Offset đã commit lấy từ Client.OffsetFetch: Kafka lưu "offset kế tiếp cần đọc", nên hiệu số là số message chưa xử lý xong.
//     (Reader.Lag() của kafka-go luôn trả -1 khi dùng GroupID nên không dùng được.)
//   - Partition chưa có offset đã commit (OffsetFetch trả CommittedOffset -1) tính từ offset đầu, vì consumer mới của group
//     đọc từ FirstOffset. Offset đã commit nhỏ hơn offset đầu (retention đã xóa đoạn đầu) cũng tính từ offset đầu.
//
// Message đang được consumer xử lý mà chưa commit vẫn tính là lag: lag là "việc chưa xong", không phải "việc chưa nhận".
func ConsumerLag(ctx context.Context, groupID, topic string) (int64, error) {
	ends, err := topicOffsets(ctx, topic)
	if err != nil {
		return 0, fmt.Errorf("đọc offset của topic %s: %w", topic, err)
	}
	ids := make([]int, 0, len(ends))
	for _, end := range ends {
		ids = append(ids, end.Partition)
	}
	resp, err := client().OffsetFetch(ctx, &kafka.OffsetFetchRequest{GroupID: groupID, Topics: map[string][]int{topic: ids}})
	if err != nil {
		return 0, fmt.Errorf("OffsetFetch của group %s: %w", groupID, err)
	}
	if resp.Error != nil {
		return 0, fmt.Errorf("OffsetFetch của group %s: %w", groupID, resp.Error)
	}
	committed := map[int]int64{}
	for _, p := range resp.Topics[topic] {
		if p.Error != nil {
			return 0, fmt.Errorf("OffsetFetch của group %s, partition %d: %w", groupID, p.Partition, p.Error)
		}
		committed[p.Partition] = p.CommittedOffset
	}
	var lag int64
	for _, end := range ends {
		offset, ok := committed[end.Partition]
		if !ok || offset < end.First {
			offset = end.First
		}
		lag += max(0, end.Last-offset)
	}
	return lag, nil
}

// Producer ghi message có key. Cùng key thì cùng partition (Murmur2Balancer), nên giữ thứ tự theo key.
type Producer struct {
	writer *kafka.Writer
}

// NewProducer tạo producer. RequireAll chờ ack của toàn bộ ISR (acks=all, mặc định của kafka-go là RequireNone),
// BatchTimeout 10ms thay cho mặc định 1 giây để mỗi lần WriteMessages không bị trễ.
// AllowAutoTopicCreation để mặc định false: topic đã xóa không bị tạo lại.
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

// Send gửi một message có key vào topic và chờ ack của broker.
func (p *Producer) Send(ctx context.Context, topic, key, value string) error {
	err := p.writer.WriteMessages(ctx, kafka.Message{Topic: topic, Key: []byte(key), Value: []byte(value)})
	if err != nil {
		return fmt.Errorf("produce vào %s: %w", topic, err)
	}
	return nil
}

// TopicRecord là một message đọc từ topic. Event là nil với poison event.
type TopicRecord struct {
	Partition int
	Offset    int64
	Key       string
	Value     []byte
	Event     *DomainEvent
}

// ReadTopic đọc lại toàn bộ log của topic, từng partition một, mà không dùng consumer group (nên không commit gì và
// không để lại group). Điều kiện dừng không dùng sleep: log end offset của mỗi partition lúc bắt đầu là mốc phải đọc tới.
func ReadTopic(ctx context.Context, topic string) ([]TopicRecord, error) {
	ends, err := topicOffsets(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf("đọc offset của topic %s: %w", topic, err)
	}
	slices.SortFunc(ends, func(a, b PartitionOffsets) int { return a.Partition - b.Partition })
	var records []TopicRecord
	for _, end := range ends {
		if end.Last <= end.First {
			continue
		}
		if err := readPartition(ctx, topic, end, &records); err != nil {
			return nil, err
		}
	}
	return records, nil
}

func readPartition(ctx context.Context, topic string, end PartitionOffsets, records *[]TopicRecord) error {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   Brokers(),
		Topic:     topic,
		Partition: end.Partition,
		MinBytes:  1,
		MaxBytes:  1 << 20,
		MaxWait:   250 * time.Millisecond,
	})
	defer func() { _ = r.Close() }()
	if err := r.SetOffset(end.First); err != nil {
		return err
	}
	readCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	for {
		msg, err := r.ReadMessage(readCtx)
		if err != nil {
			return fmt.Errorf("đọc %s[%d]: %w", topic, end.Partition, err)
		}
		rec := TopicRecord{Partition: msg.Partition, Offset: msg.Offset, Key: string(msg.Key), Value: msg.Value}
		if event, ok := ParseEvent(msg.Value); ok {
			rec.Event = &event
		}
		*records = append(*records, rec)
		if msg.Offset+1 >= end.Last {
			return nil
		}
	}
}

// Handler xử lý một event hợp lệ. Trả lỗi nghĩa là chưa xử lý xong: consumer không commit offset và thử lại message đó.
type Handler func(ctx context.Context, event DomainEvent) (HandleResult, error)

// ConsumerSpec cách dựng hàm xử lý và theo dõi kết quả của một consumer.
type ConsumerSpec struct {
	Topic   string
	GroupID string
	Handle  Handler
	// OnResult báo kết quả của từng message (kể cả ResultPoison). Có thể nil.
	OnResult func(HandleResult)
	// CrashBeforeCommit chỉ để mô phỏng lỗi: trả true thì consumer "crash" ngay sau khi effect đã commit và TRƯỚC khi
	// commit offset, tức là dừng xử lý và không commit gì (message sẽ được giao lại cho consumer kế tiếp của group). Có thể nil.
	CrashBeforeCommit func(DomainEvent) bool
}

// Consumer là một member đang chạy của consumer group.
type Consumer struct {
	reader   *kafka.Reader
	cancel   context.CancelFunc
	done     chan struct{}
	crashed  atomic.Bool
	stopOnce sync.Once
	stopErr  error
}

// retryBackoff là thời gian chờ ngắn trước khi thử lại một lỗi tạm thời, để không thành vòng lặp nóng.
const retryBackoff = 200 * time.Millisecond

// StartConsumer chạy một consumer của group spec.GroupID và trả về ngay.
//
// Dùng kafka.Reader với GroupID cùng FetchMessage và CommitMessages (như lab 06-kafka/lab-03): đơn giản hơn ConsumerGroup
// khi chỉ cần một vòng lặp, và Reader tự xử lý rebalance. FetchMessage không commit, còn CommitMessages (CommitInterval 0)
// commit đồng bộ. Auto commit tắt theo nghĩa đó: offset chỉ được lưu SAU KHI Handle thành công (transaction của effect đã
// commit), nên crash giữa hai bước cho một bản trùng chứ không bao giờ làm mất event.
//
// Poison event (không parse được) được log, đếm qua OnResult(ResultPoison) rồi commit offset để bỏ qua:
// một message hỏng không chặn partition mãi mãi. Nội dung hỏng chỉ nằm trong log, use case này không có DLQ topic.
// Nếu Handle trả lỗi, consumer chờ một chút rồi thử lại đúng message đó.
func StartConsumer(spec ConsumerSpec) *Consumer {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     Brokers(),
		GroupID:     spec.GroupID,
		Topic:       spec.Topic,
		StartOffset: kafka.FirstOffset,
		MinBytes:    1,
		MaxBytes:    1 << 20,
		MaxWait:     250 * time.Millisecond,
	})
	ctx, cancel := context.WithCancel(context.Background())
	c := &Consumer{reader: reader, cancel: cancel, done: make(chan struct{})}
	go c.run(ctx, spec)
	return c
}

// wait chờ d hoặc tới khi ctx bị hủy. Trả false nếu ctx đã bị hủy.
func wait(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}

func (c *Consumer) run(ctx context.Context, spec ConsumerSpec) {
	defer close(c.done)
	report := func(result HandleResult) {
		if spec.OnResult != nil {
			spec.OnResult(result)
		}
	}
	commit := func(msg kafka.Message) bool {
		for {
			err := c.reader.CommitMessages(ctx, msg)
			if err == nil {
				return true
			}
			if ctx.Err() != nil {
				return false
			}
			log.Printf("[order-events] lỗi commit offset, thử lại: %v", err)
			if !wait(ctx, retryBackoff) {
				return false
			}
		}
	}
	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, io.EOF) {
				return
			}
			log.Printf("[order-events] lỗi đọc message, thử lại: %v", err)
			if !wait(ctx, retryBackoff) {
				return
			}
			continue
		}
		event, ok := ParseEvent(msg.Value)
		if !ok {
			log.Printf("[order-events] bỏ qua poison event ở partition %d, offset %d: %.80s", msg.Partition, msg.Offset, msg.Value)
			report(ResultPoison)
			if !commit(msg) {
				return
			}
			continue
		}
		var result HandleResult
		for {
			result, err = spec.Handle(ctx, event)
			if err == nil {
				break
			}
			if ctx.Err() != nil {
				return
			}
			log.Printf("[order-events] lỗi khi xử lý event %s, sẽ thử lại: %v", event.EventID, err)
			if !wait(ctx, retryBackoff) {
				return
			}
		}
		report(result)
		if spec.CrashBeforeCommit != nil && spec.CrashBeforeCommit(event) {
			c.crashed.Store(true)
			return
		}
		if !commit(msg) {
			return
		}
	}
}

// Crashed cho biết CrashBeforeCommit đã kích hoạt.
func (c *Consumer) Crashed() bool { return c.crashed.Load() }

// Stop dừng consumer: hủy vòng lặp, đợi nó kết thúc rồi đóng reader (rời group bằng LeaveGroup). Gọi nhiều lần cũng được.
func (c *Consumer) Stop() error {
	c.stopOnce.Do(func() {
		c.cancel()
		<-c.done
		c.stopErr = c.reader.Close()
	})
	return c.stopErr
}
