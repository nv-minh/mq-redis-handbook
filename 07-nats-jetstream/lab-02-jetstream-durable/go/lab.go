// Package lab cài đặt durable pull consumer của JetStream: ack explicit, AckWait, MaxDeliver,
// và resume sau khi connection đóng. Dùng package jetstream của nats.go v1.54.0.
package lab

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// URL là địa chỉ server. Lab đọc NATS_URL, mặc định là NATS của make up.
func URL() string {
	if url := os.Getenv("NATS_URL"); url != "" {
		return url
	}
	return "nats://127.0.0.1:4222"
}

// Options là cấu hình consumer của lab.
type Options struct {
	// AckWait: sau chừng này không ack thì server giao lại message. Phải dương.
	AckWait time.Duration
	// MaxDeliver: số lần giao tối đa. Số nguyên từ 1 trở lên, hoặc -1 là không giới hạn.
	MaxDeliver int
}

// SubjectOf là subject của stream: lab đặt tên stream, subject và durable consumer đều xuất phát từ cùng một name.
func SubjectOf(name string) string { return name + ".events" }

// DurableOf là tên durable consumer. Tên consumer chỉ cần duy nhất trong một stream nên dùng luôn name.
func DurableOf(name string) string { return name }

// conns giữ các connection mà lab đã mở cho từng name, để Disconnect và Teardown đóng được chúng.
// Chữ ký CreateStreamAndConsumer chỉ trả về Consumer, nên connection của Consumer đó nằm ở đây.
var conns = struct {
	sync.Mutex
	byName map[string][]*nats.Conn
}{byName: map[string][]*nats.Conn{}}

func openFor(name string) (*nats.Conn, error) {
	nc, err := nats.Connect(URL())
	if err != nil {
		return nil, fmt.Errorf("không kết nối được NATS (%s): %w", URL(), err)
	}
	conns.Lock()
	conns.byName[name] = append(conns.byName[name], nc)
	conns.Unlock()
	return nc, nil
}

func takeConns(name string) []*nats.Conn {
	conns.Lock()
	defer conns.Unlock()
	taken := conns.byName[name]
	delete(conns.byName, name)
	return taken
}

// validate từ chối giá trị mà server sẽ im lặng đổi: ack_wait = 0 thành 30 s, max_deliver = 0 hoặc -2 thành -1 (đã đo trên 2.15.0).
// Từ chối ở client để giá trị sai không biến thành một giá trị mặc định khác hẳn ý định của người gọi.
func validate(opts Options) error {
	if opts.AckWait <= 0 {
		return fmt.Errorf("AckWait phải dương, nhận được %s. Server sẽ im lặng đổi 0 thành 30s", opts.AckWait)
	}
	if opts.MaxDeliver < 1 && opts.MaxDeliver != -1 {
		return fmt.Errorf("MaxDeliver phải từ 1 trở lên hoặc -1 (không giới hạn), nhận được %d", opts.MaxDeliver)
	}
	return nil
}

// CreateStreamAndConsumer tạo stream memory (subject duy nhất) và một durable pull consumer với ack explicit,
// rồi trả về consumer đó.
//
// Consumer là object phía server, nên vị trí đọc sống sót khi connection đóng: dùng BindConsumer để nối lại.
// Stream dùng retention limits nên ack không xóa message khỏi stream, chỉ đẩy vị trí của consumer.
// Gọi Teardown(name) để xóa stream và đóng connection, kể cả khi test fail.
func CreateStreamAndConsumer(ctx context.Context, name string, opts Options) (jetstream.Consumer, error) {
	if err := validate(opts); err != nil {
		return nil, err
	}
	nc, err := openFor(name)
	if err != nil {
		return nil, err
	}
	js, err := jetstream.New(nc)
	if err != nil {
		return nil, err
	}
	if _, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name:      name,
		Subjects:  []string{SubjectOf(name)},
		Storage:   jetstream.MemoryStorage,
		Retention: jetstream.LimitsPolicy,
	}); err != nil {
		return nil, fmt.Errorf("tạo stream %s: %w", name, err)
	}
	cons, err := js.CreateOrUpdateConsumer(ctx, name, jetstream.ConsumerConfig{
		Durable:       DurableOf(name),
		AckPolicy:     jetstream.AckExplicitPolicy,
		DeliverPolicy: jetstream.DeliverAllPolicy,
		AckWait:       opts.AckWait,
		MaxDeliver:    opts.MaxDeliver,
	})
	if err != nil {
		return nil, fmt.Errorf("tạo consumer %s: %w", name, err)
	}
	return cons, nil
}

// BindConsumer mở một connection mới và bind vào durable consumer đã có: mô phỏng worker khởi động lại sau khi crash.
func BindConsumer(ctx context.Context, name string) (jetstream.Consumer, error) {
	nc, err := openFor(name)
	if err != nil {
		return nil, err
	}
	js, err := jetstream.New(nc)
	if err != nil {
		return nil, err
	}
	return js.Consumer(ctx, name, DurableOf(name))
}

// Disconnect đóng đột ngột mọi connection lab đã mở cho name (không drain): mô phỏng worker mất kết nối.
func Disconnect(name string) {
	for _, nc := range takeConns(name) {
		nc.Close()
	}
}

// PublishMessages publish các body vào stream và chờ PubAck của từng message (bằng chứng message đã được lưu).
func PublishMessages(ctx context.Context, name string, bodies []string) error {
	nc, err := nats.Connect(URL())
	if err != nil {
		return err
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		return err
	}
	for _, body := range bodies {
		if _, err := js.Publish(ctx, SubjectOf(name), []byte(body)); err != nil {
			return err
		}
	}
	return nil
}

// FetchMessages pull tối đa max message, chờ tối đa expires (server giữ pull request nhiều nhất chừng đó).
// Trả về ngay khi đủ max message. Stream rỗng trả về slice rỗng khi hết hạn, không trả lỗi.
func FetchMessages(cons jetstream.Consumer, max int, expires time.Duration) ([]jetstream.Msg, error) {
	batch, err := cons.Fetch(max, jetstream.FetchMaxWait(expires))
	if err != nil {
		return nil, err
	}
	var msgs []jetstream.Msg
	for msg := range batch.Messages() {
		msgs = append(msgs, msg)
	}
	return msgs, batch.Error()
}

// ConsumerInfo đọc thông tin consumer từ server qua một connection riêng, nên dùng được cả khi connection của test đã đóng.
func ConsumerInfo(ctx context.Context, name string) (*jetstream.ConsumerInfo, error) {
	nc, err := nats.Connect(URL())
	if err != nil {
		return nil, err
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		return nil, err
	}
	cons, err := js.Consumer(ctx, name, DurableOf(name))
	if err != nil {
		return nil, err
	}
	return cons.Info(ctx)
}

// MaxDeliveriesAdvisory là nội dung advisory MAX_DELIVERIES (type io.nats.jetstream.advisory.v1.max_deliver).
type MaxDeliveriesAdvisory struct {
	Stream     string `json:"stream"`
	Consumer   string `json:"consumer"`
	StreamSeq  uint64 `json:"stream_seq"`
	Deliveries int    `json:"deliveries"`
}

// MaxDeliveriesWatch gom các advisory MAX_DELIVERIES của một consumer.
type MaxDeliveriesWatch struct {
	nc *nats.Conn

	mu         sync.Mutex
	advisories []MaxDeliveriesAdvisory
}

// Advisories trả về bản sao các advisory đã nhận, theo thứ tự tới.
func (w *MaxDeliveriesWatch) Advisories() []MaxDeliveriesAdvisory {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]MaxDeliveriesAdvisory(nil), w.advisories...)
}

// Flush là một round trip tới server: advisory nào server đã gửi trước đó chắc chắn đã nằm trong Advisories().
func (w *MaxDeliveriesWatch) Flush() error { return w.nc.Flush() }

// WatchMaxDeliveries lắng nghe advisory $JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.<stream>.<consumer>.
//
// Khi message vượt MaxDeliver, server không giao lại và không có dead-letter queue: advisory này là dấu vết duy nhất.
// Hàm flush trước khi return, nên publish ngay sau đó chắc chắn được theo dõi.
func WatchMaxDeliveries(name string) (*MaxDeliveriesWatch, error) {
	nc, err := openFor(name)
	if err != nil {
		return nil, err
	}
	w := &MaxDeliveriesWatch{nc: nc}
	subject := fmt.Sprintf("$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.%s.%s", name, DurableOf(name))
	if _, err := nc.Subscribe(subject, func(msg *nats.Msg) {
		var a MaxDeliveriesAdvisory
		if err := json.Unmarshal(msg.Data, &a); err != nil {
			return
		}
		w.mu.Lock()
		w.advisories = append(w.advisories, a)
		w.mu.Unlock()
	}); err != nil {
		return nil, err
	}
	if err := nc.Flush(); err != nil {
		return nil, err
	}
	return w, nil
}

// Teardown đóng mọi connection của name rồi xóa stream bằng một connection mới (xóa stream kéo theo xóa consumer).
// Chịu được stream chưa bao giờ được tạo.
func Teardown(ctx context.Context, name string) error {
	for _, nc := range takeConns(name) {
		nc.Close()
	}
	nc, err := nats.Connect(URL())
	if err != nil {
		return err
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		return err
	}
	if err := js.DeleteStream(ctx, name); err != nil && !errors.Is(err, jetstream.ErrStreamNotFound) {
		return err
	}
	return nil
}
