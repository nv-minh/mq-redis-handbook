// Package lab cài đặt transactional outbox trên Postgres và RabbitMQ: placeOrder ghi order và event trong một
// transaction, relay (polling publisher) đọc outbox với FOR UPDATE SKIP LOCKED, publish có confirm rồi đánh dấu
// published_at. Delivery là at-least-once, nên consumer phải dedupe theo event id.
package lab

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	amqp "github.com/rabbitmq/amqp091-go"
)

// ErrInvalidArgument được bọc vào lỗi khi caller truyền tham số sai (ví dụ batchSize không dương).
var ErrInvalidArgument = errors.New("tham số không hợp lệ")

// DatabaseURL là địa chỉ Postgres. Lab đọc DATABASE_URL, mặc định là Postgres của make up.
func DatabaseURL() string {
	if url := os.Getenv("DATABASE_URL"); url != "" {
		return url
	}
	return "postgres://handbook:handbook@127.0.0.1:5432/handbook"
}

// AMQPURL là địa chỉ broker. Lab đọc AMQP_URL, mặc định là RabbitMQ của make up.
func AMQPURL() string {
	if url := os.Getenv("AMQP_URL"); url != "" {
		return url
	}
	return "amqp://guest:guest@127.0.0.1:5672"
}

// OutboxEvent là một event trong bảng outbox. ID là khóa tăng dần của bảng và cũng là event id ổn định
// mà consumer dùng để dedupe (đi vào thuộc tính messageId của AMQP dưới dạng chuỗi).
type OutboxEvent struct {
	ID          int64
	AggregateID string
	Type        string
	Payload     json.RawMessage
}

// Order là dữ liệu business của placeOrder.
type Order struct {
	ID     string
	Amount float64
}

// PublishFn do caller cung cấp. Trả lỗi nghĩa là event chưa được broker nhận (relay sẽ rollback batch).
type PublishFn func(ctx context.Context, event OutboxEvent) error

// quoteIdent đặt tên schema trong dấu nháy kép: tên do UniqueName sinh ra có dấu "-" nên phải quote.
func quoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// CreateSchema tạo schema riêng cho một test hoặc một lần chạy demo.
func CreateSchema(ctx context.Context, schema string) error {
	conn, err := pgx.Connect(ctx, DatabaseURL())
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(context.WithoutCancel(ctx)) }()
	_, err = conn.Exec(ctx, "create schema "+quoteIdent(schema))
	return err
}

// DropSchema xóa schema cùng mọi bảng bên trong. Gọi sau khi mọi pool của schema đã đóng.
func DropSchema(ctx context.Context, schema string) error {
	conn, err := pgx.Connect(ctx, DatabaseURL())
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(context.WithoutCancel(ctx)) }()
	// lock_timeout: nếu một transaction treo còn giữ bảng thì báo lỗi thay vì chờ mãi.
	if _, err := conn.Exec(ctx, "set lock_timeout = '10s'"); err != nil {
		return err
	}
	_, err = conn.Exec(ctx, "drop schema if exists "+quoteIdent(schema)+" cascade")
	return err
}

// OpenPool mở pool có search_path trỏ vào schema, nên mọi câu SQL của lab (không ghi tên schema)
// chỉ chạm bảng của schema đó.
func OpenPool(ctx context.Context, schema string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(DatabaseURL())
	if err != nil {
		return nil, err
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = quoteIdent(schema)
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return pool, nil
}

// CreateTables tạo hai bảng của lab. outbox.published_at null nghĩa là event chưa publish.
// Partial index chỉ chứa các dòng chưa publish: relay luôn quét "dòng chưa publish theo id", nên index nhỏ
// và không phình theo lịch sử event đã gửi.
func CreateTables(ctx context.Context, pool *pgxpool.Pool) error {
	for _, ddl := range []string{
		`create table orders (
			id text primary key,
			amount numeric not null,
			created_at timestamptz not null default now()
		)`,
		`create table outbox (
			id bigserial primary key,
			aggregate_id text not null,
			type text not null,
			payload jsonb not null,
			created_at timestamptz not null default now(),
			published_at timestamptz
		)`,
		"create index outbox_unpublished_idx on outbox (id) where published_at is null",
	} {
		if _, err := pool.Exec(ctx, ddl); err != nil {
			return err
		}
	}
	return nil
}

// inTransaction chạy work trong một transaction. Lỗi thì ROLLBACK (lỗi của ROLLBACK không che lỗi gốc)
// và trả lại lỗi gốc. Rollback dùng context không bị hủy để vẫn chạy khi ctx của caller đã hết hạn.
func inTransaction(ctx context.Context, pool *pgxpool.Pool, work func(tx pgx.Tx) error) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	if err := work(tx); err != nil {
		_ = tx.Rollback(context.WithoutCancel(ctx))
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		_ = tx.Rollback(context.WithoutCancel(ctx))
		return err
	}
	return nil
}

// PlaceOrder ghi order và event OrderPlaced vào outbox trong MỘT transaction: cả hai cùng commit hoặc cùng biến mất.
// Đây là phần "transactional" của outbox. Việc gửi lên broker nằm ở relay, không nằm ở đây.
func PlaceOrder(ctx context.Context, pool *pgxpool.Pool, order Order) error {
	payload, err := json.Marshal(map[string]any{"orderId": order.ID, "amount": order.Amount})
	if err != nil {
		return err
	}
	return inTransaction(ctx, pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, "insert into orders (id, amount) values ($1, $2)", order.ID, order.Amount); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, "insert into outbox (aggregate_id, type, payload) values ($1, $2, $3::jsonb)",
			order.ID, "OrderPlaced", string(payload))
		return err
	})
}

// RunRelay chạy một lượt của polling relay. Trong MỘT transaction:
//
//  1. Chọn tối đa batchSize dòng chưa publish theo thứ tự id với FOR UPDATE SKIP LOCKED
//     (relay khác đang giữ dòng nào thì bỏ qua dòng đó, hai relay không bao giờ lấy trùng một dòng).
//  2. Gọi publish(event) lần lượt cho từng dòng và chờ nó xong (với RabbitMQ là chờ publisher confirm).
//  3. Đặt published_at cho cả batch rồi COMMIT.
//
// Trả về số event đã publish (0 khi outbox rỗng hoặc mọi dòng đang bị relay khác khóa).
//
// At-least-once: nếu process chết sau publish và trước COMMIT, Postgres rollback transaction, nhả lock và dòng
// quay lại trạng thái chưa publish, nên lượt sau publish lại event đó. Consumer phải dedupe theo event.ID.
// Nếu publish trả lỗi cho một dòng, cả batch ROLLBACK và lỗi được trả ra: những dòng đã publish trước đó trong
// batch vẫn chưa được đánh dấu và sẽ được publish lại. Đổi lại, transaction (và lock) mở suốt thời gian publish.
func RunRelay(ctx context.Context, pool *pgxpool.Pool, publish PublishFn, batchSize int) (int, error) {
	if batchSize < 1 {
		return 0, fmt.Errorf("%w: batchSize phải là số nguyên dương, nhận %d", ErrInvalidArgument, batchSize)
	}
	published := 0
	err := inTransaction(ctx, pool, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `
			select id, aggregate_id, type, payload
			  from outbox
			 where published_at is null
			 order by id
			 limit $1
			 for update skip locked`, batchSize)
		if err != nil {
			return err
		}
		// Đọc hết dòng trước khi chạy câu lệnh khác trên cùng transaction (connection đang bận tới khi rows đóng).
		events, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (OutboxEvent, error) {
			var e OutboxEvent
			err := row.Scan(&e.ID, &e.AggregateID, &e.Type, &e.Payload)
			return e, err
		})
		if err != nil {
			return err
		}
		ids := make([]int64, 0, len(events))
		for _, e := range events {
			if err := publish(ctx, e); err != nil {
				return err
			}
			ids = append(ids, e.ID)
		}
		if len(ids) > 0 {
			if _, err := tx.Exec(ctx, "update outbox set published_at = now() where id = any($1)", ids); err != nil {
				return err
			}
		}
		published = len(ids)
		return nil
	})
	if err != nil {
		return 0, err
	}
	return published, nil
}

// DeclareQueue khai báo queue quorum (tường minh x-queue-type, không phụ thuộc cấu hình mặc định của broker).
func DeclareQueue(ch *amqp.Channel, queue string) error {
	_, err := ch.QueueDeclare(queue, true, false, false, false, amqp.Table{"x-queue-type": "quorum"})
	return err
}

// NewPublisher bật confirm mode trên ch và trả về hàm publish cho relay: publish vào queue qua default exchange
// rồi chờ broker confirm. Event id đi vào thuộc tính MessageId (consumer dùng nó để dedupe), type vào Type,
// aggregate id vào header. Confirm bị nack, ctx hết hạn hoặc channel đóng thì trả lỗi và relay rollback batch.
// Lưu ý: publish qua default exchange tới queue chưa tồn tại vẫn được confirm (message bị bỏ), nên queue phải khai báo trước.
// Một channel chỉ nên dùng cho một relay vì hàm publish chờ confirm tuần tự.
func NewPublisher(ch *amqp.Channel, queue string) (PublishFn, error) {
	if err := ch.Confirm(false); err != nil {
		return nil, fmt.Errorf("confirm.select: %w", err)
	}
	return func(ctx context.Context, e OutboxEvent) error {
		confirmation, err := ch.PublishWithDeferredConfirmWithContext(ctx, "", queue, false, false, amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			MessageId:    strconv.FormatInt(e.ID, 10),
			Type:         e.Type,
			Headers:      amqp.Table{"aggregateId": e.AggregateID},
			Body:         e.Payload,
		})
		if err != nil {
			return err
		}
		acked, err := confirmation.WaitContext(ctx)
		if err != nil {
			return err
		}
		if !acked {
			return errors.New("broker nack message khi publish")
		}
		return nil
	}, nil
}

// ReceivedEvent là event consumer nhận được.
type ReceivedEvent struct {
	// EventID là event id của outbox (thuộc tính MessageId).
	EventID string
	Type    string
	Body    []byte
}

// StartConsumer chạy consumer ack thủ công trong một goroutine: gọi onEvent rồi mới ack. Mỗi lần broker giao là
// một lần gọi, kể cả bản trùng: việc dedupe theo EventID là việc của onEvent. Goroutine thoát khi channel đóng.
func StartConsumer(ch *amqp.Channel, queue, consumerTag string, onEvent func(ReceivedEvent)) error {
	if err := ch.Qos(10, 0, false); err != nil {
		return fmt.Errorf("basic.qos: %w", err)
	}
	deliveries, err := ch.Consume(queue, consumerTag, false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("basic.consume: %w", err)
	}
	go func() {
		for d := range deliveries {
			onEvent(ReceivedEvent{EventID: d.MessageId, Type: d.Type, Body: d.Body})
			_ = d.Ack(false)
		}
	}()
	return nil
}
