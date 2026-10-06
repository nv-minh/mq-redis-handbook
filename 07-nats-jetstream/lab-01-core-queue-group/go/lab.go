// Package lab minh họa core NATS: queue group chia message cho đúng một member,
// và at-most-once (message không có subscriber thì bị server bỏ).
package lab

import (
	"os"
	"sync"

	"github.com/nats-io/nats.go"
)

// URL là địa chỉ server. Lab đọc NATS_URL, mặc định là NATS của make up.
func URL() string {
	if url := os.Getenv("NATS_URL"); url != "" {
		return url
	}
	return "nats://127.0.0.1:4222"
}

// Connect mở một connection core NATS tới URL().
func Connect() (*nats.Conn, error) {
	return nats.Connect(URL())
}

// Receiver là một subscriber đang gom body của message nhận được theo đúng thứ tự nhận.
type Receiver struct {
	Subscription *nats.Subscription

	mu       sync.Mutex
	received []string
}

// Received trả về bản sao danh sách body đã nhận, an toàn khi gọi từ goroutine khác.
func (r *Receiver) Received() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.received...)
}

// SubscribeCollect subscribe subject (kèm tên queue group nếu queue khác rỗng) và gom body của mọi message.
//
// Hàm flush trước khi return: PONG của server chứng tỏ server đã xử lý xong lệnh SUB,
// nên publish ngay sau đó chắc chắn thấy subscriber này. Đây là cách làm "subscriber đã sẵn sàng"
// mà không cần sleep.
func SubscribeCollect(nc *nats.Conn, subject, queue string) (*Receiver, error) {
	r := &Receiver{}
	handler := func(msg *nats.Msg) {
		r.mu.Lock()
		defer r.mu.Unlock()
		r.received = append(r.received, string(msg.Data))
	}
	var err error
	if queue == "" {
		r.Subscription, err = nc.Subscribe(subject, handler)
	} else {
		r.Subscription, err = nc.QueueSubscribe(subject, queue, handler)
	}
	if err != nil {
		return nil, err
	}
	if err := nc.Flush(); err != nil {
		return nil, err
	}
	return r, nil
}

// PublishAll publish từng body lên subject rồi flush.
// Publish của core NATS chỉ ghi vào buffer của client và không có ack; Flush chờ server xác nhận
// đã đọc hết, nhưng vẫn không cho biết có ai nhận message hay không.
func PublishAll(nc *nats.Conn, subject string, bodies []string) error {
	for _, body := range bodies {
		if err := nc.Publish(subject, []byte(body)); err != nil {
			return err
		}
	}
	return nc.Flush()
}
