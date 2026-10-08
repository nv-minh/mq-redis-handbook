// Package lab là use case 2 của chương production: luồng order và payment hướng sự kiện.
// POST /orders ghi order và event vào outbox (Postgres) trong một transaction, relay publish lên Kafka,
// hai consumer idempotent `inventory` và `payment` xử lý event, saga hoàn kho khi thanh toán lỗi,
// và gauge lag của consumer group nằm trên /metrics.
package lab

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// DatabaseURL là địa chỉ Postgres. Use case đọc DATABASE_URL, mặc định là Postgres của make up.
func DatabaseURL() string {
	if url := os.Getenv("DATABASE_URL"); url != "" {
		return url
	}
	return "postgres://handbook:handbook@127.0.0.1:5432/handbook"
}

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

// OpenPool mở pool có search_path trỏ vào schema, nên mọi câu SQL (không ghi tên schema)
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

// CreateTables tạo các bảng của use case. Mỗi bảng thuộc một "service" logic, nhưng chúng nằm chung một database để demo gọn:
//
//   - orders và order_history: trạng thái order và dấu vết các lần đổi trạng thái.
//   - outbox: event chờ publish. event_id do Postgres sinh một lần khi ghi dòng, nên mọi lần relay publish lại đều mang cùng id.
//     published_at null nghĩa là chưa publish, partial index chỉ chứa các dòng đó nên không phình theo lịch sử.
//   - stock và reservations: kho. quantity >= 0 là hàng rào cuối: không bao giờ bán âm.
//   - payments: order_id là khóa chính và event_id unique, nên một order không thể có hai khoản thanh toán.
//   - processed_events: bảng dedup (inbox). Khóa gồm tên consumer và event id, vì hai consumer độc lập cùng đọc một event.
func CreateTables(ctx context.Context, pool *pgxpool.Pool) error {
	for _, ddl := range []string{
		`create table orders (
			id text primary key,
			amount numeric not null,
			sku text not null,
			quantity integer not null,
			status text not null default 'placed',
			created_at timestamptz not null default now()
		)`,
		`create table order_history (
			id bigserial primary key,
			order_id text not null,
			status text not null,
			created_at timestamptz not null default now()
		)`,
		`create table outbox (
			id bigserial primary key,
			event_id text not null unique default gen_random_uuid()::text,
			aggregate_id text not null,
			type text not null,
			payload jsonb not null,
			created_at timestamptz not null default now(),
			published_at timestamptz
		)`,
		"create index outbox_unpublished_idx on outbox (id) where published_at is null",
		"create table stock (sku text primary key, quantity integer not null check (quantity >= 0))",
		`create table reservations (
			order_id text primary key,
			sku text not null,
			quantity integer not null,
			status text not null
		)`,
		`create table payments (
			order_id text primary key,
			event_id text not null unique,
			amount numeric not null,
			status text not null
		)`,
		`create table processed_events (
			consumer text not null,
			event_id text not null,
			primary key (consumer, event_id)
		)`,
	} {
		if _, err := pool.Exec(ctx, ddl); err != nil {
			return err
		}
	}
	return nil
}

// SeedStock đặt số lượng tồn kho của một sku (ghi đè nếu đã có).
func SeedStock(ctx context.Context, pool *pgxpool.Pool, sku string, quantity int) error {
	_, err := pool.Exec(ctx, `insert into stock (sku, quantity) values ($1, $2)
		on conflict (sku) do update set quantity = excluded.quantity`, sku, quantity)
	return err
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

// emit ghi một event vào outbox trong transaction đang mở: nó chỉ tồn tại nếu transaction commit.
func emit(ctx context.Context, tx pgx.Tx, eventType, orderID string, data any) error {
	payload, err := json.Marshal(data)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, "insert into outbox (aggregate_id, type, payload) values ($1, $2, $3::jsonb)",
		orderID, eventType, string(payload))
	return err
}

// setStatus đổi trạng thái order và ghi dấu vết, chỉ khi trạng thái hiện tại nằm trong from.
// Guard này là lớp bảo vệ thứ hai sau bảng dedup: một event lặp không thể kéo order lùi về trạng thái cũ.
func setStatus(ctx context.Context, tx pgx.Tx, orderID, to string, from ...string) error {
	tag, err := tx.Exec(ctx, "update orders set status = $2 where id = $1 and status = any($3)", orderID, to, from)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 1 {
		_, err = tx.Exec(ctx, "insert into order_history (order_id, status) values ($1, $2)", orderID, to)
	}
	return err
}

// NewOrder là dữ liệu của một order mới.
type NewOrder struct {
	ID       string
	Amount   float64
	Sku      string
	Quantity int
}

// ErrDuplicateOrder được trả khi một order với cùng id đã tồn tại (HTTP 409).
var ErrDuplicateOrder = errors.New("order đã tồn tại")

// InvalidOrderError là lỗi khi body của POST /orders không hợp lệ (HTTP 400). Message dành cho người gọi API.
type InvalidOrderError struct{ Message string }

func (e *InvalidOrderError) Error() string { return e.Message }

var orderIDPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

// newOrderID sinh id dạng ord-<uuid v4>.
func newOrderID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("lab: crypto/rand failed: " + err.Error())
	}
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	return fmt.Sprintf("ord-%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}

// ParseOrderRequest kiểm tra body của POST /orders và điền giá trị mặc định. Hợp đồng (giống bản TypeScript):
//
//   - amount: số hữu hạn lớn hơn 0 (bắt buộc).
//   - sku: chuỗi không rỗng, tối đa 64 ký tự (bắt buộc).
//   - quantity: số nguyên từ 1 tới 1000000, mặc định 1.
//   - id: tùy chọn, 1 tới 64 ký tự trong A-Za-z0-9_-. Thiếu thì server sinh ord-<uuid>.
//
// Lỗi trả về luôn có kiểu *InvalidOrderError.
func ParseOrderRequest(raw []byte) (NewOrder, error) {
	var body any
	if err := json.Unmarshal(raw, &body); err != nil {
		return NewOrder{}, &InvalidOrderError{"body không phải JSON hợp lệ"}
	}
	m, ok := body.(map[string]any)
	if !ok {
		return NewOrder{}, &InvalidOrderError{"body phải là một JSON object"}
	}
	amount, ok := m["amount"].(float64)
	if !ok || amount <= 0 {
		return NewOrder{}, &InvalidOrderError{"amount phải là số lớn hơn 0"}
	}
	sku, ok := m["sku"].(string)
	if !ok || sku == "" || utf8.RuneCountInString(sku) > 64 {
		return NewOrder{}, &InvalidOrderError{"sku phải là chuỗi không rỗng, tối đa 64 ký tự"}
	}
	quantity := 1.0
	if v, present := m["quantity"]; present {
		q, ok := v.(float64)
		if !ok || q != math.Trunc(q) || q < 1 || q > 1_000_000 {
			return NewOrder{}, &InvalidOrderError{"quantity phải là số nguyên từ 1 tới 1000000"}
		}
		quantity = q
	}
	id := newOrderID()
	if v, present := m["id"]; present {
		s, ok := v.(string)
		if !ok || !orderIDPattern.MatchString(s) {
			return NewOrder{}, &InvalidOrderError{"id chỉ gồm 1 tới 64 ký tự trong A-Z a-z 0-9 _ -"}
		}
		id = s
	}
	return NewOrder{ID: id, Amount: amount, Sku: sku, Quantity: int(quantity)}, nil
}

// PlaceOrder ghi order, dòng history đầu tiên và event OrderPlaced vào outbox trong MỘT transaction:
// cả ba cùng commit hoặc cùng biến mất. Việc gửi lên Kafka nằm ở relay, không nằm ở đây.
// Trả ErrDuplicateOrder nếu id đã tồn tại.
func PlaceOrder(ctx context.Context, pool *pgxpool.Pool, order NewOrder) error {
	err := inTransaction(ctx, pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, "insert into orders (id, amount, sku, quantity) values ($1, $2, $3, $4)",
			order.ID, order.Amount, order.Sku, order.Quantity); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, "insert into order_history (order_id, status) values ($1, 'placed')", order.ID); err != nil {
			return err
		}
		return emit(ctx, tx, EventOrderPlaced, order.ID, OrderPlacedData{Amount: order.Amount, Sku: order.Sku, Quantity: order.Quantity})
	})
	var pgErr *pgconn.PgError
	// 23505 = unique_violation: duy nhất khóa chính orders.id có thể vi phạm trong transaction này.
	if errors.As(err, &pgErr) && pgErr.Code == "23505" {
		return fmt.Errorf("%w: %s", ErrDuplicateOrder, order.ID)
	}
	return err
}

// OutboxEvent là một event trong bảng outbox. EventID là id ổn định mà consumer dùng để dedupe.
type OutboxEvent struct {
	EventID     string
	AggregateID string
	Type        string
	Payload     json.RawMessage
}

// PublishFn do caller cung cấp. Trả lỗi nghĩa là event chưa được broker nhận (relay sẽ rollback batch).
type PublishFn func(ctx context.Context, event OutboxEvent) error

// ErrInvalidBatchSize được bọc vào lỗi khi batchSize của RunRelay không dương.
var ErrInvalidBatchSize = errors.New("batchSize không hợp lệ")

// RunRelay chạy một lượt của polling relay. Trong MỘT transaction:
//
//  1. Chọn tối đa batchSize dòng chưa publish theo thứ tự id với FOR UPDATE SKIP LOCKED
//     (relay khác đang giữ dòng nào thì bỏ qua dòng đó).
//  2. Gọi publish(event) lần lượt cho từng dòng và chờ nó xong (với Kafka là chờ ack của acks=all).
//  3. Đặt published_at cho cả batch rồi COMMIT.
//
// Trả về số event đã publish (0 khi outbox rỗng hoặc mọi dòng đang bị relay khác khóa).
//
// At-least-once: nếu process chết sau publish và trước COMMIT, Postgres rollback transaction, nhả lock và dòng
// quay lại trạng thái chưa publish, nên lượt sau publish lại event đó với CÙNG EventID. Consumer phải dedupe.
// Nếu publish trả lỗi, cả batch ROLLBACK và lỗi được trả ra. Đổi lại, transaction (và lock) mở suốt thời gian publish.
// Thứ tự: nhiều relay song song với SKIP LOCKED có thể publish đảo thứ tự, và một relay là điều kiện cần chứ chưa đủ
// (hai transaction commit đảo thứ tự id vẫn có thể làm id nhỏ được publish sau id lớn, xem chương 08 lab 05). Thứ tự mà use case
// này cần đến từ chỗ khác: event của một order cùng key nên cùng partition, và event follow-up chỉ được ghi sau khi event
// trước đó đã được publish và consumer đã xử lý nó (quan hệ nhân quả).
func RunRelay(ctx context.Context, pool *pgxpool.Pool, publish PublishFn, batchSize int) (int, error) {
	if batchSize < 1 {
		return 0, fmt.Errorf("%w: phải là số nguyên dương, nhận %d", ErrInvalidBatchSize, batchSize)
	}
	published := 0
	err := inTransaction(ctx, pool, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `
			select id, event_id, aggregate_id, type, payload
			  from outbox
			 where published_at is null
			 order by id
			 limit $1
			 for update skip locked`, batchSize)
		if err != nil {
			return err
		}
		// Đọc hết dòng trước khi chạy câu lệnh khác trên cùng transaction (connection đang bận tới khi rows đóng).
		type row struct {
			id    int64
			event OutboxEvent
		}
		batch, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (row, error) {
			var b row
			err := r.Scan(&b.id, &b.event.EventID, &b.event.AggregateID, &b.event.Type, &b.event.Payload)
			return b, err
		})
		if err != nil {
			return err
		}
		ids := make([]int64, 0, len(batch))
		for _, b := range batch {
			if err := publish(ctx, b.event); err != nil {
				return err
			}
			ids = append(ids, b.id)
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

// KafkaPublisher tạo hàm publish cho relay: gửi envelope JSON lên topic với key là order id (một order luôn nằm trong
// một partition, nên thứ tự event của order được giữ). Producer chờ ack của acks=all rồi mới trả về.
func KafkaPublisher(p *Producer, topic string) PublishFn {
	return func(ctx context.Context, e OutboxEvent) error {
		value, err := json.Marshal(DomainEvent{EventID: e.EventID, Type: e.Type, OrderID: e.AggregateID, Data: e.Payload})
		if err != nil {
			return err
		}
		return p.Send(ctx, topic, e.AggregateID, string(value))
	}
}

// DefaultPaymentLimit là hạn mức mặc định của luật lỗi thanh toán: amount lớn hơn hạn mức thì thanh toán thất bại.
const DefaultPaymentLimit = 1000.0

// HandlerOptions là tùy chọn của InventoryHandler.
type HandlerOptions struct {
	// AfterDedupInsert chỉ để mô phỏng lỗi: chạy NGAY SAU khi dòng dedup được chèn và TRƯỚC khi effect chạy, trong cùng
	// transaction. Trả lỗi ở đây phải rollback cả dòng dedup lẫn effect. Có thể nil.
	AfterDedupInsert func(ctx context.Context) error
}

// PaymentOptions là tùy chọn của PaymentHandler.
type PaymentOptions struct {
	// PaymentLimit: amount lớn hơn giá trị này thì thanh toán lỗi (luật lỗi xác định để test saga). 0 nghĩa là DefaultPaymentLimit.
	PaymentLimit float64
	// AfterDedupInsert như HandlerOptions.AfterDedupInsert.
	AfterDedupInsert func(ctx context.Context) error
}

// firstTime chèn dòng dedup (consumer, eventID). Trả true nếu đây là lần đầu consumer thấy event.
// Unique constraint là trọng tài: nếu hai bản của cùng một event chạy đồng thời, bản thứ hai chờ bản đầu commit
// rồi gặp conflict, nên chỉ một bản áp dụng effect.
func firstTime(ctx context.Context, tx pgx.Tx, consumer, eventID string) (bool, error) {
	tag, err := tx.Exec(ctx, "insert into processed_events (consumer, event_id) values ($1, $2) on conflict do nothing", consumer, eventID)
	return tag.RowsAffected() == 1, err
}

// InventoryHandler là consumer `inventory`. Xử lý hai loại event, mỗi event trong MỘT transaction gồm dòng dedup,
// effect và event follow-up:
//
//   - OrderPlaced: giữ hàng. Đủ hàng thì trừ kho, ghi reservation `reserved` và phát InventoryReserved.
//     Không đủ hàng thì ghi reservation `rejected`, hủy order và phát InventoryRejected.
//   - PaymentFailed: compensation của saga. Reservation `reserved` được chuyển thành `released`, kho được cộng lại,
//     order thành `cancelled` và phát InventoryReleased. Reservation không còn ở `reserved` thì không làm gì (không release hai lần).
func InventoryHandler(pool *pgxpool.Pool, opts HandlerOptions) Handler {
	return func(ctx context.Context, event DomainEvent) (HandleResult, error) {
		if event.Type != EventOrderPlaced && event.Type != EventPaymentFailed {
			return HandleIgnored, nil
		}
		result := HandleApplied
		err := inTransaction(ctx, pool, func(tx pgx.Tx) error {
			first, err := firstTime(ctx, tx, "inventory", event.EventID)
			if err != nil {
				return err
			}
			if !first {
				result = HandleDuplicate
				return nil
			}
			if opts.AfterDedupInsert != nil {
				if err := opts.AfterDedupInsert(ctx); err != nil {
					return err
				}
			}
			if event.Type == EventOrderPlaced {
				return reserve(ctx, tx, event)
			}
			return release(ctx, tx, event)
		})
		return result, err
	}
}

func reserve(ctx context.Context, tx pgx.Tx, event DomainEvent) error {
	data, _ := event.placed() // ParseEvent đã kiểm tra data của OrderPlaced
	inserted, err := tx.Exec(ctx, `insert into reservations (order_id, sku, quantity, status) values ($1, $2, $3, 'reserved')
		on conflict (order_id) do nothing`, event.OrderID, data.Sku, data.Quantity)
	if err != nil {
		return err
	}
	// Order đã có reservation (event khác id cho cùng order): không trừ kho lần nữa.
	if inserted.RowsAffected() == 0 {
		return nil
	}
	decremented, err := tx.Exec(ctx, "update stock set quantity = quantity - $2 where sku = $1 and quantity >= $2", data.Sku, data.Quantity)
	if err != nil {
		return err
	}
	reservation := map[string]any{"sku": data.Sku, "quantity": data.Quantity}
	if decremented.RowsAffected() == 1 {
		return emit(ctx, tx, EventInventoryReserved, event.OrderID, reservation)
	}
	if _, err := tx.Exec(ctx, "update reservations set status = 'rejected' where order_id = $1", event.OrderID); err != nil {
		return err
	}
	// Từ placed hoặc paid: payment chạy song song nên order có thể đã paid khi kho báo hết hàng.
	if err := setStatus(ctx, tx, event.OrderID, "cancelled", "placed", "paid"); err != nil {
		return err
	}
	reservation["reason"] = "insufficient_stock"
	return emit(ctx, tx, EventInventoryRejected, event.OrderID, reservation)
}

func release(ctx context.Context, tx pgx.Tx, event DomainEvent) error {
	var sku string
	var quantity int
	err := tx.QueryRow(ctx, `update reservations set status = 'released'
		where order_id = $1 and status = 'reserved' returning sku, quantity`, event.OrderID).Scan(&sku, &quantity)
	switch {
	case err == nil:
		if _, err := tx.Exec(ctx, "update stock set quantity = quantity + $2 where sku = $1", sku, quantity); err != nil {
			return err
		}
		if err := emit(ctx, tx, EventInventoryReleased, event.OrderID, map[string]any{"sku": sku, "quantity": quantity}); err != nil {
			return err
		}
	case !errors.Is(err, pgx.ErrNoRows):
		return err
	}
	return setStatus(ctx, tx, event.OrderID, "cancelled", "payment_failed")
}

// PaymentHandler là consumer `payment`. Chỉ xử lý OrderPlaced, trong MỘT transaction gồm dòng dedup, dòng payment và
// event follow-up. Amount không vượt hạn mức thì ghi payment `charged`, order thành `paid` và phát PaymentCompleted.
// Vượt hạn mức thì ghi payment `failed`, order thành `payment_failed` và phát PaymentFailed (kích hoạt compensation của kho).
// Use case giả lập cổng thanh toán bằng luật này nên không có lời gọi mạng nào nằm ngoài transaction:
// với cổng thật, lời gọi phải mang idempotency key (xem README).
func PaymentHandler(pool *pgxpool.Pool, opts PaymentOptions) Handler {
	limit := opts.PaymentLimit
	if limit == 0 {
		limit = DefaultPaymentLimit
	}
	return func(ctx context.Context, event DomainEvent) (HandleResult, error) {
		if event.Type != EventOrderPlaced {
			return HandleIgnored, nil
		}
		result := HandleApplied
		err := inTransaction(ctx, pool, func(tx pgx.Tx) error {
			first, err := firstTime(ctx, tx, "payment", event.EventID)
			if err != nil {
				return err
			}
			if !first {
				result = HandleDuplicate
				return nil
			}
			if opts.AfterDedupInsert != nil {
				if err := opts.AfterDedupInsert(ctx); err != nil {
					return err
				}
			}
			data, _ := event.placed() // ParseEvent đã kiểm tra data của OrderPlaced
			failed := data.Amount > limit
			status := "charged"
			if failed {
				status = "failed"
			}
			inserted, err := tx.Exec(ctx, `insert into payments (order_id, event_id, amount, status) values ($1, $2, $3, $4)
				on conflict (order_id) do nothing`, event.OrderID, event.EventID, data.Amount, status)
			if err != nil {
				return err
			}
			// Order đã có payment (event khác id cho cùng order): không charge lần nữa.
			if inserted.RowsAffected() == 0 {
				return nil
			}
			if failed {
				if err := setStatus(ctx, tx, event.OrderID, "payment_failed", "placed"); err != nil {
					return err
				}
				return emit(ctx, tx, EventPaymentFailed, event.OrderID,
					map[string]any{"amount": data.Amount, "limit": limit, "reason": "amount_exceeds_limit"})
			}
			if err := setStatus(ctx, tx, event.OrderID, "paid", "placed"); err != nil {
				return err
			}
			return emit(ctx, tx, EventPaymentCompleted, event.OrderID, map[string]any{"amount": data.Amount})
		})
		return result, err
	}
}

// Tên hai consumer của service.
const (
	ConsumerInventory = "inventory"
	ConsumerPayment   = "payment"
)

var consumerNames = []string{ConsumerInventory, ConsumerPayment}

// Config cấu hình Service.
type Config struct {
	Pool  *pgxpool.Pool
	Topic string
	// GroupPrefix: group id của consumer là <GroupPrefix>-inventory và <GroupPrefix>-payment.
	GroupPrefix string
	// BatchSize là số event tối đa một lượt relay. Mặc định 10.
	BatchSize int
}

// ConsumerOptions là tùy chọn của Service.StartConsumer.
type ConsumerOptions struct {
	// PaymentLimit chỉ có tác dụng với consumer payment, xem PaymentOptions.PaymentLimit.
	PaymentLimit float64
	// AfterDedupInsert chỉ để mô phỏng lỗi, xem HandlerOptions.AfterDedupInsert.
	AfterDedupInsert func(ctx context.Context) error
	// CrashBeforeCommit chỉ để mô phỏng lỗi, xem ConsumerSpec.CrashBeforeCommit.
	CrashBeforeCommit func(DomainEvent) bool
}

// maxBodyBytes là giới hạn kích thước body của POST /orders.
const maxBodyBytes = 64 * 1024

// Service nối HTTP API, relay, consumer và metrics thành một service nhỏ.
//
//   - POST /orders (xem ParseOrderRequest): 201 {id, status: "placed"}, 400 {error} khi body sai, 409 khi id đã tồn tại,
//     413 khi body quá 64 KiB.
//   - GET /metrics: registry riêng của instance với consumer_events_total{consumer,result} và gauge
//     consumer_group_lag{group,topic}. Lag được đo MỖI LẦN scrape (không cache); nếu Kafka không trả lời thì scrape lỗi 500.
type Service struct {
	cfg      Config
	producer *Producer
	publish  PublishFn
	events   *prometheus.CounterVec
	http     *http.Server
	url      string

	mu        sync.Mutex
	consumers []*Consumer
	closeOnce sync.Once
	closeErr  error
	httpDone  chan struct{}
}

// lagCollector đo lag của hai group mỗi lần Prometheus scrape.
type lagCollector struct {
	svc  *Service
	desc *prometheus.Desc
}

func (c *lagCollector) Describe(ch chan<- *prometheus.Desc) { ch <- c.desc }

func (c *lagCollector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, name := range consumerNames {
		group := c.svc.GroupID(name)
		lag, err := ConsumerLag(ctx, group, c.svc.cfg.Topic)
		if err != nil {
			ch <- prometheus.NewInvalidMetric(c.desc, err)
			return
		}
		ch <- prometheus.MustNewConstMetric(c.desc, prometheus.GaugeValue, float64(lag), group, c.svc.cfg.Topic)
	}
}

// NewService khởi động HTTP server (cổng do hệ điều hành chọn) và producer. Pool thuộc về caller, Close không đóng nó.
func NewService(_ context.Context, cfg Config) (*Service, error) {
	if cfg.Pool == nil || cfg.Topic == "" || cfg.GroupPrefix == "" {
		return nil, errors.New("thiếu cấu hình: cần Pool, Topic và GroupPrefix")
	}
	if cfg.BatchSize == 0 {
		cfg.BatchSize = 10
	}
	s := &Service{cfg: cfg, producer: NewProducer(), httpDone: make(chan struct{})}
	s.publish = KafkaPublisher(s.producer, cfg.Topic)

	registry := prometheus.NewRegistry()
	s.events = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "consumer_events_total",
		Help: "Số message consumer đã xử lý, theo kết quả (applied, duplicate, ignored, poison).",
	}, []string{"consumer", "result"})
	registry.MustRegister(s.events)
	// Khởi tạo mọi series bằng 0 để endpoint luôn in ra đủ, kể cả khi chưa có message nào.
	for _, consumer := range consumerNames {
		for _, result := range []HandleResult{HandleApplied, HandleDuplicate, HandleIgnored, ResultPoison} {
			s.events.WithLabelValues(consumer, string(result)).Add(0)
		}
	}
	registry.MustRegister(&lagCollector{svc: s, desc: prometheus.NewDesc(
		"consumer_group_lag",
		"Tổng số message chưa xử lý của group trên topic (log end offset trừ offset đã commit).",
		[]string{"group", "topic"}, nil)})

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		_ = s.producer.Close()
		return nil, err
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
	mux.HandleFunc("/orders", s.handleOrders)
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusNotFound, map[string]any{"error": "không có đường dẫn này"})
	})
	s.http = &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	s.url = "http://" + listener.Addr().String()
	go func() {
		defer close(s.httpDone)
		if err := s.http.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			fmt.Printf("[order-events] HTTP server lỗi: %v\n", err)
		}
	}()
	return s, nil
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func (s *Service) handleOrders(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "chỉ hỗ trợ POST"})
		return
	}
	raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeJSON(w, http.StatusRequestEntityTooLarge, map[string]any{"error": "body vượt quá 64 KiB"})
			return
		}
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "không đọc được body"})
		return
	}
	order, err := ParseOrderRequest(raw)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	if err := PlaceOrder(r.Context(), s.cfg.Pool, order); err != nil {
		if errors.Is(err, ErrDuplicateOrder) {
			writeJSON(w, http.StatusConflict, map[string]any{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"id": order.ID, "status": "placed"})
}

// URL trả về địa chỉ gốc của HTTP server (cổng do hệ điều hành chọn), ví dụ http://127.0.0.1:41234.
func (s *Service) URL() string { return s.url }

// MetricsURL trả về URL của GET /metrics.
func (s *Service) MetricsURL() string { return s.url + "/metrics" }

// GroupID trả về group id của consumer name.
func (s *Service) GroupID(name string) string { return s.cfg.GroupPrefix + "-" + name }

// RunRelay chạy một lượt relay: publish các dòng outbox chưa gửi lên topic. Trả về số event đã publish.
func (s *Service) RunRelay(ctx context.Context) (int, error) {
	return RunRelay(ctx, s.cfg.Pool, s.publish, s.cfg.BatchSize)
}

// StartConsumer chạy consumer name (ConsumerInventory hoặc ConsumerPayment). Offset chỉ được commit sau khi
// transaction của effect đã commit. Service dừng mọi consumer còn chạy khi Close.
func (s *Service) StartConsumer(name string, opts ConsumerOptions) (*Consumer, error) {
	var handle Handler
	switch name {
	case ConsumerInventory:
		handle = InventoryHandler(s.cfg.Pool, HandlerOptions{AfterDedupInsert: opts.AfterDedupInsert})
	case ConsumerPayment:
		handle = PaymentHandler(s.cfg.Pool, PaymentOptions{PaymentLimit: opts.PaymentLimit, AfterDedupInsert: opts.AfterDedupInsert})
	default:
		return nil, fmt.Errorf("consumer không tồn tại: %q", name)
	}
	c := StartConsumer(ConsumerSpec{
		Topic:             s.cfg.Topic,
		GroupID:           s.GroupID(name),
		Handle:            handle,
		OnResult:          func(result HandleResult) { s.events.WithLabelValues(name, string(result)).Inc() },
		CrashBeforeCommit: opts.CrashBeforeCommit,
	})
	s.mu.Lock()
	s.consumers = append(s.consumers, c)
	s.mu.Unlock()
	return c, nil
}

// Close dừng consumer, đóng HTTP server và producer. Không đóng pool (pool thuộc về caller).
// Gọi nhiều lần an toàn: lần sau trả lại kết quả của lần đầu.
func (s *Service) Close(ctx context.Context) error {
	s.closeOnce.Do(func() {
		var errs []error
		s.mu.Lock()
		consumers := s.consumers
		s.mu.Unlock()
		for _, c := range consumers {
			errs = append(errs, c.Stop())
		}
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		if err := s.http.Shutdown(shutdownCtx); err != nil {
			errs = append(errs, err, s.http.Close())
		}
		<-s.httpDone
		errs = append(errs, s.producer.Close())
		s.closeErr = errors.Join(errs...)
	})
	return s.closeErr
}
