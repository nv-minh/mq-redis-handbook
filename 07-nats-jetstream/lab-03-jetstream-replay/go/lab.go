// Package lab cài đặt replay trên JetStream: consumer mới với deliver policy All hoặc ByStartTime
// đọc lại lịch sử của stream. Dùng package jetstream của nats.go v1.54.0.
package lab

import (
	"context"
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

// SubjectOf là subject của stream: tên stream và subject đều xuất phát từ cùng một name.
func SubjectOf(name string) string { return name + ".events" }

// conns giữ các connection mà lab đã mở cho từng name (mỗi Reader giữ một connection), để Teardown đóng được chúng.
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

// CreateReplayStream tạo stream memory với retention limits: ack và đọc không xóa message,
// nên lịch sử còn nguyên để replay. Storage, retention và deliver policy cố định từ lúc tạo, không sửa được sau này.
func CreateReplayStream(ctx context.Context, name string) error {
	nc, err := openFor(name)
	if err != nil {
		return err
	}
	js, err := jetstream.New(nc)
	if err != nil {
		return err
	}
	_, err = js.CreateStream(ctx, jetstream.StreamConfig{
		Name:      name,
		Subjects:  []string{SubjectOf(name)},
		Storage:   jetstream.MemoryStorage,
		Retention: jetstream.LimitsPolicy,
	})
	return err
}

// PublishBatch publish các body vào stream, chờ PubAck của từng message và trả về stream sequence do server gán.
func PublishBatch(ctx context.Context, name string, bodies []string) ([]uint64, error) {
	nc, err := nats.Connect(URL())
	if err != nil {
		return nil, err
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		return nil, err
	}
	seqs := make([]uint64, 0, len(bodies))
	for _, body := range bodies {
		ack, err := js.Publish(ctx, SubjectOf(name), []byte(body))
		if err != nil {
			return nil, err
		}
		seqs = append(seqs, ack.Sequence)
	}
	return seqs, nil
}

// StoredMessageTime trả về timestamp server gán cho message đã lưu (độ phân giải nanosecond).
func StoredMessageTime(ctx context.Context, name string, seq uint64) (time.Time, error) {
	nc, err := nats.Connect(URL())
	if err != nil {
		return time.Time{}, err
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		return time.Time{}, err
	}
	stream, err := js.Stream(ctx, name)
	if err != nil {
		return time.Time{}, err
	}
	raw, err := stream.GetMsg(ctx, seq)
	if err != nil {
		return time.Time{}, err
	}
	return raw.Time, nil
}

// StartTimeBetween trả về mốc thời gian nằm giữa hai timestamp của server: older < kết quả <= newer.
// Cả hai đầu vào đều do server gán nên kết quả không phụ thuộc đồng hồ của client.
func StartTimeBetween(older, newer time.Time) time.Time {
	gap := newer.Sub(older)
	if gap <= 0 {
		panic(fmt.Sprintf("StartTimeBetween cần older < newer, nhận được %s và %s", older, newer))
	}
	return older.Add((gap + 1) / 2)
}

// Policy là deliver policy của consumer replay: DeliverAll hoặc DeliverByStartTime.
type Policy struct {
	startTime *time.Time
}

// DeliverAll đọc từ message đầu tiên của stream.
func DeliverAll() Policy { return Policy{} }

// DeliverByStartTime đọc từ message đầu tiên có timestamp >= t.
func DeliverByStartTime(t time.Time) Policy { return Policy{startTime: &t} }

// Replayed là một message đã đọc lại.
type Replayed struct {
	// Seq là stream sequence do server gán.
	Seq  uint64
	Body string
}

// Reader đọc lại message qua một consumer mới.
type Reader struct {
	// PendingAtStart là số message consumer còn phải giao ngay sau khi tạo (num_pending):
	// với ByStartTime đó là số message có timestamp >= mốc.
	PendingAtStart uint64

	cons jetstream.Consumer
}

const probeExpires = time.Second

// OpenReplay tạo một consumer MỚI (ephemeral pull consumer, ack none) với deliver policy đã chọn.
//
// Deliver policy chỉ áp dụng lúc consumer được tạo và không sửa được: muốn đọc lại từ chỗ khác thì tạo consumer mới.
// Ack none vì replay chỉ đọc: không có pending list, không AckWait, không redelivery.
// Consumer bị xóa cùng stream khi Teardown(name).
func OpenReplay(ctx context.Context, name string, policy Policy) (*Reader, error) {
	nc, err := openFor(name)
	if err != nil {
		return nil, err
	}
	js, err := jetstream.New(nc)
	if err != nil {
		return nil, err
	}
	cfg := jetstream.ConsumerConfig{
		AckPolicy: jetstream.AckNonePolicy,
		// Ephemeral consumer bị server xóa khi không có pull request nào trong khoảng này.
		InactiveThreshold: time.Minute,
	}
	if policy.startTime == nil {
		cfg.DeliverPolicy = jetstream.DeliverAllPolicy
	} else {
		cfg.DeliverPolicy = jetstream.DeliverByStartTimePolicy
		cfg.OptStartTime = policy.startTime
	}
	cons, err := js.CreateConsumer(ctx, name, cfg)
	if err != nil {
		return nil, fmt.Errorf("tạo consumer replay cho %s: %w", name, err)
	}
	info, err := cons.Info(ctx)
	if err != nil {
		return nil, err
	}
	return &Reader{PendingAtStart: info.NumPending, cons: cons}, nil
}

// Read đọc hết những gì consumer có thể giao lúc này, rồi dừng khi một fetch có hạn trả về rỗng.
func (r *Reader) Read(ctx context.Context) ([]Replayed, error) {
	var out []Replayed
	collect := func(max int, expires time.Duration) (int, error) {
		batch, err := r.cons.Fetch(max, jetstream.FetchMaxWait(expires))
		if err != nil {
			return 0, err
		}
		received := 0
		for msg := range batch.Messages() {
			meta, err := msg.Metadata()
			if err != nil {
				return received, err
			}
			out = append(out, Replayed{Seq: meta.Sequence.Stream, Body: string(msg.Data())})
			received++
		}
		return received, batch.Error()
	}
	// Số message còn phải giao biết trước từ server, nên lần fetch đầu trả về ngay khi đủ, không chờ hết hạn.
	info, err := r.cons.Info(ctx)
	if err != nil {
		return nil, err
	}
	if info.NumPending > 0 {
		if _, err := collect(int(info.NumPending), 5*time.Second); err != nil {
			return nil, err
		}
	}
	// Fetch dò: server không còn gì thì trả về rỗng sau probeExpires. Nếu còn thừa thì message thừa lộ ra ở kết quả.
	for {
		n, err := collect(100, probeExpires)
		if err != nil {
			return nil, err
		}
		if n == 0 {
			return out, nil
		}
	}
}

// Teardown xóa stream (kéo theo xóa mọi consumer) bằng một connection mới rồi đóng mọi connection của name.
// Chịu được stream chưa bao giờ được tạo.
func Teardown(ctx context.Context, name string) error {
	open := takeConns(name)
	defer func() {
		for _, nc := range open {
			nc.Close()
		}
	}()
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
