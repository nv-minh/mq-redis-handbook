// Package lab cài đặt topic routing trên RabbitMQ: topic exchange với binding wildcard,
// publisher confirm và xử lý basic.return cho message không route được.
package lab

import (
	"context"
	"errors"
	"os"

	amqp "github.com/rabbitmq/amqp091-go"
)

// URL là địa chỉ broker. Lab đọc AMQP_URL, mặc định là RabbitMQ của make up.
func URL() string {
	if url := os.Getenv("AMQP_URL"); url != "" {
		return url
	}
	return "amqp://guest:guest@127.0.0.1:5672"
}

// TopicRouting là tên exchange và queue do DeclareTopicRouting dựng.
type TopicRouting struct {
	Exchange string
	Queue    string
}

// DeclareTopicRouting dựng một topic exchange <name> (durable) và một quorum queue <name>.queue,
// rồi bind queue vào exchange bằng pattern. Topic pattern là các từ phân tách bằng dấu chấm:
// * thay đúng một từ, # thay không hoặc nhiều từ.
//
// Queue khai báo x-queue-type: quorum tường minh (compose đặt quorum làm mặc định, broker stock mặc định classic).
// Quorum queue không thể exclusive hay auto-delete nên người gọi phải xóa queue và exchange khi dọn dẹp.
func DeclareTopicRouting(ch *amqp.Channel, name, pattern string) (TopicRouting, error) {
	r := TopicRouting{Exchange: name, Queue: name + ".queue"}
	if err := ch.ExchangeDeclare(r.Exchange, amqp.ExchangeTopic, true, false, false, false, nil); err != nil {
		return r, err
	}
	if _, err := ch.QueueDeclare(r.Queue, true, false, false, false, amqp.Table{"x-queue-type": "quorum"}); err != nil {
		return r, err
	}
	return r, ch.QueueBind(r.Queue, pattern, r.Exchange, false, nil)
}

// Publisher là publisher ở confirm mode và biết xử lý basic.return.
//
// Broker confirm cả message không route được, nên confirm không chứng minh message đã vào queue.
// Muốn biết message có bị bỏ hay không phải publish với mandatory=true và nghe basic.return.
// AMQP bảo đảm basic.return đến TRƯỚC basic.ack của chính message đó, và amqp091-go xử lý frame
// tuần tự trong một goroutine, nên khi confirm về thì return (nếu có) đã nằm trong kênh returns.
// Vì vậy Publish không cần sleep hay timeout.
type Publisher struct {
	ch      *amqp.Channel
	returns chan amqp.Return
}

// NewPublisher bật confirm mode trên ch và đăng ký nhận basic.return.
// Kênh returns có đệm để goroutine đọc frame của amqp091-go không bao giờ bị chặn.
func NewPublisher(ch *amqp.Channel) (*Publisher, error) {
	if err := ch.Confirm(false); err != nil {
		return nil, err
	}
	return &Publisher{ch: ch, returns: ch.NotifyReturn(make(chan amqp.Return, 16))}, nil
}

// Publish publish rồi chờ confirm. Trả về message bị trả về, hoặc nil nếu broker không trả gì.
func (p *Publisher) Publish(ctx context.Context, exchange, routingKey, body string, mandatory bool) (*amqp.Return, error) {
	confirmation, err := p.ch.PublishWithDeferredConfirmWithContext(ctx, exchange, routingKey, mandatory, false, amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		Body:         []byte(body),
	})
	if err != nil {
		return nil, err
	}
	acked, err := confirmation.WaitContext(ctx)
	if err != nil {
		return nil, err
	}
	if !acked {
		return nil, errors.New("broker nack message khi publish")
	}
	select {
	case returned := <-p.returns:
		return &returned, nil
	default:
		return nil, nil
	}
}

// ReceiveRoutingKeys lấy hết message đang có trong queue bằng basic.get (auto-ack)
// và trả về routing key của chúng theo thứ tự.
func ReceiveRoutingKeys(ch *amqp.Channel, queue string) ([]string, error) {
	keys := []string{}
	for {
		msg, ok, err := ch.Get(queue, true)
		if err != nil {
			return nil, err
		}
		if !ok {
			return keys, nil
		}
		keys = append(keys, msg.RoutingKey)
	}
}
