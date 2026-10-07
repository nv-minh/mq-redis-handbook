package lab

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"slices"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/nv-minh/mq-redis-handbook/internal/testkit"
)

const (
	partitions = 3
	sku        = "sku-1"
	// helperEnv bật chế độ helper process: test binary được chạy lại để đóng vai relay bị kill.
	helperEnv = "UC02_HELPER"
)

var httpClient = &http.Client{Timeout: 10 * time.Second}

// env gom mọi thứ của một test: schema, pool, topic và service riêng (tên duy nhất).
type env struct {
	pool   *pgxpool.Pool
	schema string
	topic  string
	svc    *Service
}

// newEnv tạo schema, topic 3 partition và service. Cleanup chạy ngược thứ tự đăng ký, nên các pha teardown là:
// consumer và child (đăng ký sau, chạy trước), rồi service, rồi pool, rồi DROP SCHEMA, rồi xóa group và topic.
func newEnv(t *testing.T) *env {
	t.Helper()
	ctx := context.Background()
	topic := testkit.UniqueName("uc02-orders")
	// Đăng ký xóa topic trước khi tạo: xóa một topic chưa tồn tại chỉ là no-op.
	t.Cleanup(func() {
		if err := DeleteTopic(context.Background(), topic); err != nil {
			t.Errorf("DeleteTopic lỗi: %v", err)
		}
	})
	groupPrefix := testkit.UniqueName("uc02-group")
	for _, name := range []string{"inventory", "payment"} {
		group := groupPrefix + "-" + name
		t.Cleanup(func() {
			if err := DeleteGroup(context.Background(), group); err != nil {
				t.Errorf("DeleteGroup lỗi: %v", err)
			}
		})
	}
	schema := testkit.UniqueName("uc02")
	if err := CreateSchema(ctx, schema); err != nil {
		t.Fatalf("CreateSchema lỗi: %v", err)
	}
	t.Cleanup(func() {
		if err := DropSchema(context.Background(), schema); err != nil {
			t.Errorf("DropSchema lỗi: %v", err)
		}
	})
	pool, err := OpenPool(ctx, schema)
	if err != nil {
		t.Fatalf("OpenPool lỗi: %v", err)
	}
	t.Cleanup(pool.Close)
	if err := CreateTables(ctx, pool); err != nil {
		t.Fatalf("CreateTables lỗi: %v", err)
	}
	if err := CreateTopic(ctx, topic, partitions); err != nil {
		t.Fatalf("CreateTopic lỗi: %v", err)
	}
	svc, err := NewService(ctx, Config{Pool: pool, Topic: topic, GroupPrefix: groupPrefix})
	if err != nil {
		t.Fatalf("NewService lỗi: %v", err)
	}
	t.Cleanup(func() {
		if err := svc.Close(context.Background()); err != nil {
			t.Errorf("Service.Close lỗi: %v", err)
		}
	})
	return &env{pool: pool, schema: schema, topic: topic, svc: svc}
}

// startConsumer chạy consumer và đăng ký Stop vào cleanup (chạy trước khi service, pool và schema bị đóng).
func (e *env) startConsumer(t *testing.T, name string, opts ConsumerOptions) *Consumer {
	t.Helper()
	c, err := e.svc.StartConsumer(name, opts)
	if err != nil {
		t.Fatalf("StartConsumer %s lỗi: %v", name, err)
	}
	t.Cleanup(func() {
		if err := c.Stop(); err != nil {
			t.Errorf("Consumer.Stop %s lỗi: %v", name, err)
		}
	})
	return c
}

// post gửi POST /orders thật. body là chuỗi (gửi nguyên văn) hoặc giá trị được encode thành JSON.
func (e *env) post(t *testing.T, body any) (int, map[string]any) {
	t.Helper()
	var raw []byte
	if s, ok := body.(string); ok {
		raw = []byte(s)
	} else {
		var err error
		if raw, err = json.Marshal(body); err != nil {
			t.Fatalf("marshal body lỗi: %v", err)
		}
	}
	resp, err := httpClient.Post(e.svc.URL()+"/orders", "application/json", bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("POST /orders lỗi: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	var out map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("response không phải JSON (status %d): %v", resp.StatusCode, err)
	}
	return resp.StatusCode, out
}

func (e *env) count(t *testing.T, sql string, args ...any) int {
	t.Helper()
	var n int
	if err := e.pool.QueryRow(context.Background(), sql, args...).Scan(&n); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	return n
}

func (e *env) stock(t *testing.T) int {
	t.Helper()
	return e.count(t, "select quantity from stock where sku = $1", sku)
}

func (e *env) str(t *testing.T, sql string, args ...any) string {
	t.Helper()
	var s string
	if err := e.pool.QueryRow(context.Background(), sql, args...).Scan(&s); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	return s
}

func (e *env) trail(t *testing.T, orderID string) []string {
	t.Helper()
	rows, err := e.pool.Query(context.Background(), "select status from order_history where order_id = $1 order by id", orderID)
	if err != nil {
		t.Fatalf("đọc order_history lỗi: %v", err)
	}
	trail, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		t.Fatalf("đọc order_history lỗi: %v", err)
	}
	return trail
}

// metric đọc giá trị một series từ endpoint /metrics thật (định dạng text của Prometheus), 0 nếu series chưa có.
func (e *env) metric(t *testing.T, name string, labels map[string]string) float64 {
	t.Helper()
	resp, err := httpClient.Get(e.svc.MetricsURL())
	if err != nil {
		t.Fatalf("GET /metrics lỗi: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("đọc /metrics lỗi: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /metrics trả %d: %s", resp.StatusCode, body)
	}
	for _, line := range strings.Split(string(body), "\n") {
		if !strings.HasPrefix(line, name+"{") {
			continue
		}
		ok := true
		for k, v := range labels {
			ok = ok && strings.Contains(line, fmt.Sprintf(`%s=%q`, k, v))
		}
		if ok {
			var value float64
			if _, err := fmt.Sscanf(line[strings.LastIndex(line, " ")+1:], "%g", &value); err != nil {
				t.Fatalf("không đọc được giá trị của %q: %v", line, err)
			}
			return value
		}
	}
	return 0
}

func (e *env) events(t *testing.T, eventType string) []DomainEvent {
	t.Helper()
	records, err := ReadTopic(context.Background(), e.topic)
	if err != nil {
		t.Fatalf("ReadTopic lỗi: %v", err)
	}
	return eventsOf(records, eventType)
}

func eventsOf(records []TopicRecord, eventType string) []DomainEvent {
	var out []DomainEvent
	for _, r := range records {
		if r.Event != nil && (eventType == "" || r.Event.Type == eventType) {
			out = append(out, *r.Event)
		}
	}
	return out
}

// placedEnvelope là envelope của event OrderPlaced đang nằm trong outbox của order (để test tự publish bản trùng).
func (e *env) placedEnvelope(t *testing.T, orderID string) DomainEvent {
	t.Helper()
	var eventID string
	var payload []byte
	err := e.pool.QueryRow(context.Background(),
		"select event_id, payload from outbox where aggregate_id = $1 and type = 'OrderPlaced'", orderID).Scan(&eventID, &payload)
	if err != nil {
		t.Fatalf("đọc outbox của %s lỗi: %v", orderID, err)
	}
	return DomainEvent{EventID: eventID, Type: EventOrderPlaced, OrderID: orderID, Data: payload}
}

func wire(t *testing.T, e DomainEvent) string {
	t.Helper()
	b, err := json.Marshal(e)
	if err != nil {
		t.Fatalf("marshal event lỗi: %v", err)
	}
	return string(b)
}

func newProducer(t *testing.T) *Producer {
	t.Helper()
	p := NewProducer()
	t.Cleanup(func() {
		if err := p.Close(); err != nil {
			t.Errorf("Producer.Close lỗi: %v", err)
		}
	})
	return p
}

func mustSend(t *testing.T, p *Producer, topic, key, value string) {
	t.Helper()
	if err := p.Send(context.Background(), topic, key, value); err != nil {
		t.Fatalf("Send lỗi: %v", err)
	}
}

func mustRelay(t *testing.T, e *env) int {
	t.Helper()
	n, err := e.svc.RunRelay(context.Background())
	if err != nil {
		t.Fatalf("RunRelay lỗi: %v", err)
	}
	return n
}

func mustLag(t *testing.T, group, topic string) int64 {
	t.Helper()
	lag, err := ConsumerLag(context.Background(), group, topic)
	if err != nil {
		t.Fatalf("ConsumerLag lỗi: %v", err)
	}
	return lag
}

func TestPostOrdersEmitsExactlyOneOrderPlacedEvent(t *testing.T) {
	e := newEnv(t)
	status, body := e.post(t, map[string]any{"amount": 25.5, "sku": sku, "quantity": 2})
	if status != http.StatusCreated {
		t.Fatalf("POST /orders trả %d, mong đợi 201: %v", status, body)
	}
	id, _ := body["id"].(string)
	if !strings.HasPrefix(id, "ord-") || body["status"] != "placed" {
		t.Fatalf("response không đúng: %v", body)
	}

	// Order và event nằm trong outbox cùng lúc, event chưa publish.
	if n := e.count(t, "select count(*) from orders"); n != 1 {
		t.Fatalf("orders có %d dòng, mong đợi 1", n)
	}
	if n := e.count(t, "select count(*) from outbox where published_at is null"); n != 1 {
		t.Fatalf("outbox có %d dòng chưa publish, mong đợi 1", n)
	}
	eventID := e.str(t, "select event_id from outbox")

	if n := mustRelay(t, e); n != 1 {
		t.Fatalf("relay publish %d event, mong đợi 1", n)
	}

	// Đọc topic từ đầu: đúng một OrderPlaced, key là order id, payload đúng.
	records, err := ReadTopic(context.Background(), e.topic)
	if err != nil {
		t.Fatalf("ReadTopic lỗi: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("topic có %d message, mong đợi đúng 1", len(records))
	}
	if records[0].Key != id {
		t.Fatalf("key của message là %q, mong đợi order id %q", records[0].Key, id)
	}
	placed := eventsOf(records, EventOrderPlaced)
	if len(placed) != 1 || placed[0].EventID != eventID || placed[0].OrderID != id {
		t.Fatalf("event không đúng: %+v (eventId mong đợi %s)", placed, eventID)
	}
	data, ok := placed[0].placed()
	if !ok || data != (OrderPlacedData{Amount: 25.5, Sku: sku, Quantity: 2}) {
		t.Fatalf("payload không đúng: %s", placed[0].Data)
	}

	// Relay chạy lần hai không publish gì thêm và outbox đã được đánh dấu.
	if n := mustRelay(t, e); n != 0 {
		t.Fatalf("relay lần hai publish %d event, mong đợi 0", n)
	}
	if got, _ := ReadTopic(context.Background(), e.topic); len(got) != 1 {
		t.Fatalf("topic có %d message sau lần relay thứ hai, mong đợi 1", len(got))
	}
	if n := e.count(t, "select count(*) from outbox where published_at is null"); n != 0 {
		t.Fatalf("còn %d dòng chưa đánh dấu", n)
	}
}

func TestInvalidPostBodyReturns400AndInsertsNothing(t *testing.T) {
	e := newEnv(t)
	bad := []any{
		"{không phải json", "[]", "null", map[string]any{},
		map[string]any{"amount": 0, "sku": sku},
		map[string]any{"amount": -5, "sku": sku},
		map[string]any{"amount": "12", "sku": sku},
		map[string]any{"amount": 10},
		map[string]any{"amount": 10, "sku": ""},
		map[string]any{"amount": 10, "sku": sku, "quantity": 0},
		map[string]any{"amount": 10, "sku": sku, "quantity": 1.5},
		map[string]any{"amount": 10, "sku": sku, "id": "có khoảng trắng"},
	}
	for _, body := range bad {
		status, out := e.post(t, body)
		if status != http.StatusBadRequest {
			t.Errorf("body %v: status %d, mong đợi 400", body, status)
		}
		if _, ok := out["error"].(string); !ok {
			t.Errorf("body %v: response thiếu trường error: %v", body, out)
		}
	}
	for _, table := range []string{"orders", "outbox", "order_history"} {
		if n := e.count(t, "select count(*) from "+table); n != 0 {
			t.Fatalf("bảng %s có %d dòng sau các request sai, mong đợi 0", table, n)
		}
	}

	// Id do client chọn: lần đầu 201, lần hai cùng id là 409 và không ghi thêm gì.
	if status, _ := e.post(t, map[string]any{"id": "order-fixed", "amount": 10, "sku": sku}); status != http.StatusCreated {
		t.Fatalf("lần đầu trả %d, mong đợi 201", status)
	}
	if status, _ := e.post(t, map[string]any{"id": "order-fixed", "amount": 10, "sku": sku}); status != http.StatusConflict {
		t.Fatalf("lần hai trả %d, mong đợi 409", status)
	}
	if n := e.count(t, "select count(*) from orders"); n != 1 {
		t.Fatalf("orders có %d dòng, mong đợi 1", n)
	}
	if n := e.count(t, "select count(*) from outbox"); n != 1 {
		t.Fatalf("outbox có %d dòng, mong đợi 1", n)
	}

	// Đường dẫn và method không hỗ trợ.
	if resp, err := httpClient.Get(e.svc.URL() + "/nope"); err != nil || resp.StatusCode != http.StatusNotFound {
		t.Fatalf("GET /nope: %v %v", resp, err)
	} else {
		_ = resp.Body.Close()
	}
	if resp, err := httpClient.Get(e.svc.URL() + "/orders"); err != nil || resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("GET /orders: %v %v", resp, err)
	} else {
		_ = resp.Body.Close()
	}
}

func TestRelayWithAnEmptyOutboxReturnsZeroWithoutPublishing(t *testing.T) {
	e := newEnv(t)
	if n := mustRelay(t, e); n != 0 {
		t.Fatalf("relay publish %d event từ outbox rỗng", n)
	}
	called := 0
	n, err := RunRelay(context.Background(), e.pool, func(context.Context, OutboxEvent) error {
		called++
		return nil
	}, 5)
	if err != nil || n != 0 || called != 0 {
		t.Fatalf("RunRelay = %d, %v (publish được gọi %d lần), mong đợi 0, nil, 0", n, err, called)
	}
	if records, err := ReadTopic(context.Background(), e.topic); err != nil || len(records) != 0 {
		t.Fatalf("topic có %d message, lỗi %v, mong đợi rỗng", len(records), err)
	}
}

func TestDuplicateEventDoesNotDoubleCharge(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t)
	if err := SeedStock(ctx, e.pool, sku, 10); err != nil {
		t.Fatalf("SeedStock lỗi: %v", err)
	}
	const orderID = "order-dup"
	if err := PlaceOrder(ctx, e.pool, NewOrder{ID: orderID, Amount: 40, Sku: sku, Quantity: 2}); err != nil {
		t.Fatalf("PlaceOrder lỗi: %v", err)
	}
	placed := e.placedEnvelope(t, orderID)
	e.startConsumer(t, "inventory", ConsumerOptions{})
	e.startConsumer(t, "payment", ConsumerOptions{})

	// Cùng một event (cùng eventId) tới ba lần: hai lần cùng key (cùng partition, offset khác nhau)
	// và một lần với key khác (có thể là partition khác).
	p := newProducer(t)
	mustSend(t, p, e.topic, orderID, wire(t, placed))
	mustSend(t, p, e.topic, orderID, wire(t, placed))
	mustSend(t, p, e.topic, "key-khac", wire(t, placed))
	for _, consumer := range []string{"inventory", "payment"} {
		testkit.Eventually(t, 40*time.Second, func() (struct{}, bool) {
			applied := e.metric(t, "consumer_events_total", map[string]string{"consumer": consumer, "result": "applied"})
			dup := e.metric(t, "consumer_events_total", map[string]string{"consumer": consumer, "result": "duplicate"})
			return struct{}{}, applied+dup == 3
		})
		if got := e.metric(t, "consumer_events_total", map[string]string{"consumer": consumer, "result": "applied"}); got != 1 {
			t.Fatalf("%s: applied = %v, mong đợi 1", consumer, got)
		}
		if got := e.metric(t, "consumer_events_total", map[string]string{"consumer": consumer, "result": "duplicate"}); got != 2 {
			t.Fatalf("%s: duplicate = %v, mong đợi 2", consumer, got)
		}
	}

	// Một dòng payment, một reservation, kho trừ đúng một lần, mỗi consumer đúng một dòng dedup.
	if n := e.count(t, "select count(*) from payments"); n != 1 {
		t.Fatalf("payments có %d dòng, mong đợi 1", n)
	}
	if n := e.count(t, "select count(*) from reservations"); n != 1 {
		t.Fatalf("reservations có %d dòng, mong đợi 1", n)
	}
	if got := e.stock(t); got != 8 {
		t.Fatalf("kho còn %d, mong đợi 8", got)
	}
	if n := e.count(t, "select count(*) from processed_events"); n != 2 {
		t.Fatalf("processed_events có %d dòng, mong đợi 2", n)
	}
	if got := e.str(t, "select status from orders where id = $1", orderID); got != "paid" {
		t.Fatalf("order ở trạng thái %q, mong đợi paid", got)
	}

	// Dòng dedup và effect commit nguyên tử: lỗi chen giữa hai bước rollback cả hai, nên redelivery áp dụng được.
	const second = "order-atomic"
	if err := PlaceOrder(ctx, e.pool, NewOrder{ID: second, Amount: 30, Sku: sku, Quantity: 1}); err != nil {
		t.Fatalf("PlaceOrder lỗi: %v", err)
	}
	event := e.placedEnvelope(t, second)
	injected := errors.New("lỗi chèn giữa dedup và effect")
	failing := func(context.Context) error { return injected }

	payment := PaymentHandler(e.pool, PaymentOptions{AfterDedupInsert: failing})
	if _, err := payment(ctx, event); !errors.Is(err, injected) {
		t.Fatalf("handler trả %v, mong đợi lỗi đã chèn", err)
	}
	if n := e.count(t, "select count(*) from processed_events where event_id = $1", event.EventID); n != 0 {
		t.Fatalf("dòng dedup vẫn còn (%d) dù transaction lỗi", n)
	}
	if n := e.count(t, "select count(*) from payments where order_id = $1", second); n != 0 {
		t.Fatalf("payment vẫn được ghi (%d) dù transaction lỗi", n)
	}
	payment = PaymentHandler(e.pool, PaymentOptions{})
	for i, want := range []HandleResult{HandleApplied, HandleDuplicate} {
		if got, err := payment(ctx, event); err != nil || got != want {
			t.Fatalf("lần gọi %d của payment handler = %q, %v, mong đợi %q", i+1, got, err, want)
		}
	}
	if n := e.count(t, "select count(*) from payments where order_id = $1", second); n != 1 {
		t.Fatalf("payment có %d dòng, mong đợi 1", n)
	}

	stockBefore := e.stock(t)
	inventory := InventoryHandler(e.pool, HandlerOptions{AfterDedupInsert: failing})
	if _, err := inventory(ctx, event); !errors.Is(err, injected) {
		t.Fatalf("handler trả %v, mong đợi lỗi đã chèn", err)
	}
	if got := e.stock(t); got != stockBefore {
		t.Fatalf("kho đổi từ %d thành %d dù transaction lỗi", stockBefore, got)
	}
	if n := e.count(t, "select count(*) from reservations where order_id = $1", second); n != 0 {
		t.Fatalf("reservation vẫn được ghi (%d) dù transaction lỗi", n)
	}
	inventory = InventoryHandler(e.pool, HandlerOptions{})
	for i, want := range []HandleResult{HandleApplied, HandleDuplicate} {
		if got, err := inventory(ctx, event); err != nil || got != want {
			t.Fatalf("lần gọi %d của inventory handler = %q, %v, mong đợi %q", i+1, got, err, want)
		}
	}
	if got := e.stock(t); got != stockBefore-1 {
		t.Fatalf("kho còn %d, mong đợi %d", got, stockBefore-1)
	}
}

func TestPaymentFailureTriggersInventoryReleaseViaSaga(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t)
	if err := SeedStock(ctx, e.pool, sku, 10); err != nil {
		t.Fatalf("SeedStock lỗi: %v", err)
	}
	e.startConsumer(t, "inventory", ConsumerOptions{})
	// Luật lỗi xác định: thanh toán lỗi khi amount vượt hạn mức.
	e.startConsumer(t, "payment", ConsumerOptions{PaymentLimit: 1000})

	status, body := e.post(t, map[string]any{"amount": 5000, "sku": sku, "quantity": 3})
	if status != http.StatusCreated {
		t.Fatalf("POST /orders trả %d: %v", status, body)
	}
	id := body["id"].(string)

	// Bơm relay tới khi saga chạy xong: OrderPlaced, rồi PaymentFailed và InventoryReserved, rồi InventoryReleased.
	testkit.Eventually(t, 40*time.Second, func() (struct{}, bool) {
		if _, err := e.svc.RunRelay(ctx); err != nil {
			t.Logf("RunRelay lỗi (thử lại): %v", err)
			return struct{}{}, false
		}
		return struct{}{}, e.str(t, "select status from orders where id = $1", id) == "cancelled"
	})
	testkit.Eventually(t, 20*time.Second, func() (struct{}, bool) {
		n, err := e.svc.RunRelay(ctx)
		return struct{}{}, err == nil && n == 0
	})

	// Kho trở về giá trị ban đầu, reservation ở trạng thái released, payment ở trạng thái failed.
	if got := e.stock(t); got != 10 {
		t.Fatalf("kho còn %d, mong đợi trở về 10", got)
	}
	if got := e.str(t, "select status from reservations where order_id = $1", id); got != "released" {
		t.Fatalf("reservation ở trạng thái %q, mong đợi released", got)
	}
	if got := e.str(t, "select status from payments where order_id = $1", id); got != "failed" {
		t.Fatalf("payment ở trạng thái %q, mong đợi failed", got)
	}

	// Dấu vết trạng thái theo thứ tự, và event trên topic.
	if got := e.trail(t, id); !slices.Equal(got, []string{"placed", "payment_failed", "cancelled"}) {
		t.Fatalf("dấu vết trạng thái = %v", got)
	}
	var types []string
	for _, ev := range e.events(t, "") {
		types = append(types, ev.Type)
	}
	sorted := slices.Clone(types)
	slices.Sort(sorted)
	if types[0] != EventOrderPlaced || !slices.Equal(sorted, []string{EventInventoryReleased, EventInventoryReserved, EventOrderPlaced, EventPaymentFailed}) {
		t.Fatalf("event trên topic = %v", types)
	}
	if slices.Index(types, EventPaymentFailed) > slices.Index(types, EventInventoryReleased) {
		t.Fatalf("PaymentFailed phải đứng trước InventoryReleased: %v", types)
	}

	// Redelivery của PaymentFailed: cùng eventId (bị dedup) và một event mới cùng order (guard trạng thái reservation).
	failed := e.events(t, EventPaymentFailed)[0]
	p := newProducer(t)
	mustSend(t, p, e.topic, id, wire(t, failed))
	again := failed
	again.EventID = "pf-lan-hai"
	mustSend(t, p, e.topic, id, wire(t, again))
	testkit.Eventually(t, 40*time.Second, func() (struct{}, bool) {
		dup := e.metric(t, "consumer_events_total", map[string]string{"consumer": "inventory", "result": "duplicate"})
		applied := e.metric(t, "consumer_events_total", map[string]string{"consumer": "inventory", "result": "applied"})
		return struct{}{}, dup == 1 && applied == 3
	})
	mustRelay(t, e)
	if got := e.stock(t); got != 10 {
		t.Fatalf("kho còn %d sau redelivery, mong đợi 10", got)
	}
	if n := len(e.events(t, EventInventoryReleased)); n != 1 {
		t.Fatalf("có %d event InventoryReleased, mong đợi đúng 1", n)
	}
	if got := e.trail(t, id); !slices.Equal(got, []string{"placed", "payment_failed", "cancelled"}) {
		t.Fatalf("dấu vết trạng thái sau redelivery = %v", got)
	}
}

func TestConsumerLagReachesZeroAfterCatchingUp(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t)
	if err := SeedStock(ctx, e.pool, sku, 100); err != nil {
		t.Fatalf("SeedStock lỗi: %v", err)
	}
	const n = 5
	for i := range n {
		if status, body := e.post(t, map[string]any{"amount": 10 + i, "sku": sku}); status != http.StatusCreated {
			t.Fatalf("POST /orders trả %d: %v", status, body)
		}
	}
	if got := mustRelay(t, e); got != n {
		t.Fatalf("relay publish %d event, mong đợi %d", got, n)
	}

	// Consumer chưa chạy nên group chưa commit gì: lag tính từ offset đầu, đúng bằng N, với một topic không có offset đã commit.
	inventoryGroup, paymentGroup := e.svc.GroupID("inventory"), e.svc.GroupID("payment")
	if got := mustLag(t, inventoryGroup, e.topic); got != n {
		t.Fatalf("lag của inventory = %d, mong đợi %d", got, n)
	}
	if got := mustLag(t, paymentGroup, e.topic); got != n {
		t.Fatalf("lag của payment = %d, mong đợi %d", got, n)
	}
	if got := e.metric(t, "consumer_group_lag", map[string]string{"group": inventoryGroup, "topic": e.topic}); got != n {
		t.Fatalf("gauge lag của inventory = %v, mong đợi %d", got, n)
	}

	// Cho consumer chạy: lag về 0 và gauge trên /metrics theo đó.
	e.startConsumer(t, "inventory", ConsumerOptions{})
	e.startConsumer(t, "payment", ConsumerOptions{})
	for _, group := range []string{inventoryGroup, paymentGroup} {
		testkit.Eventually(t, 40*time.Second, func() (struct{}, bool) {
			lag, err := ConsumerLag(ctx, group, e.topic)
			return struct{}{}, err == nil && lag == 0
		})
	}
	for _, group := range []string{inventoryGroup, paymentGroup} {
		if got := e.metric(t, "consumer_group_lag", map[string]string{"group": group, "topic": e.topic}); got != 0 {
			t.Fatalf("gauge lag của %s = %v, mong đợi 0", group, got)
		}
	}
	if got := e.count(t, "select count(*) from payments"); got != n {
		t.Fatalf("payments có %d dòng, mong đợi %d", got, n)
	}
}

func TestConsumerCrashAfterEffectBeforeOffsetCommitRedeliversAndIsDeduped(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t)
	if err := SeedStock(ctx, e.pool, sku, 10); err != nil {
		t.Fatalf("SeedStock lỗi: %v", err)
	}
	status, body := e.post(t, map[string]any{"amount": 10, "sku": sku, "quantity": 4})
	if status != http.StatusCreated {
		t.Fatalf("POST /orders trả %d: %v", status, body)
	}
	id := body["id"].(string)
	if got := mustRelay(t, e); got != 1 {
		t.Fatalf("relay publish %d event, mong đợi 1", got)
	}

	// Consumer thứ nhất áp dụng effect (transaction đã commit) rồi "crash" TRƯỚC khi commit offset.
	first := e.startConsumer(t, "inventory", ConsumerOptions{CrashBeforeCommit: func(DomainEvent) bool { return true }})
	testkit.Eventually(t, 40*time.Second, func() (struct{}, bool) { return struct{}{}, first.Crashed() })
	if err := first.Stop(); err != nil {
		t.Fatalf("Stop lỗi: %v", err)
	}
	if got := e.stock(t); got != 6 {
		t.Fatalf("kho còn %d, mong đợi 6", got)
	}
	group := e.svc.GroupID("inventory")
	if got := mustLag(t, group, e.topic); got != 1 {
		t.Fatalf("lag sau crash = %d, mong đợi 1 (offset chưa được commit)", got)
	}

	// Consumer thay thế cùng group nhận lại event từ offset đã commit: dedup bỏ qua, effect vẫn đúng một lần.
	e.startConsumer(t, "inventory", ConsumerOptions{})
	testkit.Eventually(t, 40*time.Second, func() (struct{}, bool) {
		lag, err := ConsumerLag(ctx, group, e.topic)
		return struct{}{}, err == nil && lag == 0
	})
	if got := e.stock(t); got != 6 {
		t.Fatalf("kho còn %d sau redelivery, mong đợi 6", got)
	}
	if n := e.count(t, "select count(*) from reservations where order_id = $1", id); n != 1 {
		t.Fatalf("reservations có %d dòng, mong đợi 1", n)
	}
	if got := e.metric(t, "consumer_events_total", map[string]string{"consumer": "inventory", "result": "applied"}); got != 1 {
		t.Fatalf("applied = %v, mong đợi 1", got)
	}
	if got := e.metric(t, "consumer_events_total", map[string]string{"consumer": "inventory", "result": "duplicate"}); got != 1 {
		t.Fatalf("duplicate = %v, mong đợi 1", got)
	}
}

func TestPoisonEventIsSkippedCountedAndDoesNotBlockThePartition(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t)
	if err := SeedStock(ctx, e.pool, sku, 10); err != nil {
		t.Fatalf("SeedStock lỗi: %v", err)
	}
	status, body := e.post(t, map[string]any{"amount": 10, "sku": sku})
	if status != http.StatusCreated {
		t.Fatalf("POST /orders trả %d: %v", status, body)
	}
	id := body["id"].(string)
	// Event hỏng nằm TRƯỚC event hợp lệ trong cùng partition (cùng key).
	p := newProducer(t)
	mustSend(t, p, e.topic, id, "{không phải json")
	mustSend(t, p, e.topic, id, `{"eventId":7}`)
	if got := mustRelay(t, e); got != 1 {
		t.Fatalf("relay publish %d event, mong đợi 1", got)
	}
	e.startConsumer(t, "inventory", ConsumerOptions{})
	e.startConsumer(t, "payment", ConsumerOptions{})

	// Lag về 0 nghĩa là cả hai event hỏng đã bị bỏ qua (offset đã commit) và event hợp lệ phía sau đã được xử lý.
	for _, name := range []string{"inventory", "payment"} {
		testkit.Eventually(t, 40*time.Second, func() (struct{}, bool) {
			lag, err := ConsumerLag(ctx, e.svc.GroupID(name), e.topic)
			return struct{}{}, err == nil && lag == 0
		})
		if got := e.metric(t, "consumer_events_total", map[string]string{"consumer": name, "result": "poison"}); got != 2 {
			t.Fatalf("%s: poison = %v, mong đợi 2", name, got)
		}
	}
	if got := e.str(t, "select status from orders where id = $1", id); got != "paid" {
		t.Fatalf("order ở trạng thái %q, mong đợi paid", got)
	}
	if got := e.stock(t); got != 9 {
		t.Fatalf("kho còn %d, mong đợi 9", got)
	}
}

// outOfStockScenario: kho chỉ có 1 mà order cần 3. Hai consumer chạy độc lập nên có hai thứ tự xen kẽ, và test chạy cả hai
// bằng cách chỉ khởi động MỘT consumer trước (cổng): first chạy trước, consumer còn lại chạy sau.
// Kết quả phải giống nhau: reservation rejected, kho không đổi, InventoryRejected trên topic, order cancelled.
func outOfStockScenario(t *testing.T, first string) {
	ctx := context.Background()
	e := newEnv(t)
	if err := SeedStock(ctx, e.pool, sku, 1); err != nil {
		t.Fatalf("SeedStock lỗi: %v", err)
	}
	status, body := e.post(t, map[string]any{"amount": 10, "sku": sku, "quantity": 3})
	if status != http.StatusCreated {
		t.Fatalf("POST /orders trả %d: %v", status, body)
	}
	id := body["id"].(string)
	if got := mustRelay(t, e); got != 1 {
		t.Fatalf("relay publish %d event, mong đợi 1", got)
	}
	applied := func(consumer string) float64 {
		return e.metric(t, "consumer_events_total", map[string]string{"consumer": consumer, "result": "applied"})
	}

	e.startConsumer(t, first, ConsumerOptions{})
	testkit.Eventually(t, 40*time.Second, func() (struct{}, bool) { return struct{}{}, applied(first) == 1 })
	second := ConsumerInventory
	if first == ConsumerInventory {
		second = ConsumerPayment
	}
	e.startConsumer(t, second, ConsumerOptions{})
	testkit.Eventually(t, 40*time.Second, func() (struct{}, bool) {
		return struct{}{}, e.str(t, "select status from orders where id = $1", id) == "cancelled" && applied(second) == 1
	})
	testkit.Eventually(t, 20*time.Second, func() (struct{}, bool) {
		n, err := e.svc.RunRelay(ctx)
		return struct{}{}, err == nil && n == 0
	})

	if got := e.str(t, "select status from reservations where order_id = $1", id); got != "rejected" {
		t.Fatalf("reservation ở trạng thái %q, mong đợi rejected", got)
	}
	if got := e.stock(t); got != 1 {
		t.Fatalf("kho còn %d, mong đợi không đổi (1)", got)
	}
	if n := len(e.events(t, EventInventoryRejected)); n != 1 {
		t.Fatalf("có %d event InventoryRejected, mong đợi đúng 1", n)
	}
	// Giới hạn đã biết, README ghi rõ: payment chạy song song nên vẫn charge order bị hủy và không có bước hoàn tiền.
	if got := e.str(t, "select status from payments where order_id = $1", id); got != "charged" {
		t.Fatalf("payment ở trạng thái %q, mong đợi charged (giới hạn đã biết: không hoàn tiền)", got)
	}
	want := []string{"placed", "cancelled"}
	if first == ConsumerPayment {
		want = []string{"placed", "paid", "cancelled"}
	}
	if got := e.trail(t, id); !slices.Equal(got, want) {
		t.Fatalf("dấu vết trạng thái = %v, mong đợi %v", got, want)
	}

	// Giao lại OrderPlaced (cùng eventId rồi eventId khác): không hủy lần hai, không event follow-up mới.
	placed := e.placedEnvelope(t, id)
	p := newProducer(t)
	mustSend(t, p, e.topic, id, wire(t, placed))
	again := placed
	again.EventID = "op-lan-hai"
	mustSend(t, p, e.topic, id, wire(t, again))
	testkit.Eventually(t, 40*time.Second, func() (struct{}, bool) {
		dup := e.metric(t, "consumer_events_total", map[string]string{"consumer": "inventory", "result": "duplicate"})
		return struct{}{}, dup == 1 && applied("inventory") == 2
	})
	mustRelay(t, e)
	if n := len(e.events(t, EventInventoryRejected)); n != 1 {
		t.Fatalf("có %d event InventoryRejected sau redelivery, mong đợi 1", n)
	}
	if n := e.count(t, "select count(*) from outbox where type = 'InventoryRejected'"); n != 1 {
		t.Fatalf("outbox có %d dòng InventoryRejected, mong đợi 1", n)
	}
	if got := e.stock(t); got != 1 {
		t.Fatalf("kho còn %d sau redelivery, mong đợi 1", got)
	}
	if got := e.str(t, "select status from orders where id = $1", id); got != "cancelled" {
		t.Fatalf("order ở trạng thái %q, mong đợi cancelled", got)
	}
}

func TestOutOfStockOrderIsRejectedAndCancelledWhenPaymentRanFirst(t *testing.T) {
	outOfStockScenario(t, ConsumerPayment)
}

func TestOutOfStockOrderIsRejectedAndCancelledWhenInventoryRanFirst(t *testing.T) {
	outOfStockScenario(t, ConsumerInventory)
}

func TestMetricsEndpointReturns500WhenKafkaIsUnreachable(t *testing.T) {
	e := newEnv(t)
	// Cổng 1 không có ai nghe: bộ thu lag bị từ chối kết nối. Producer của service đã được tạo từ trước.
	t.Setenv("KAFKA_BROKERS", "127.0.0.1:1")
	started := time.Now()
	resp, err := httpClient.Get(e.svc.MetricsURL())
	if err != nil {
		t.Fatalf("GET /metrics lỗi: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusInternalServerError || len(body) == 0 {
		t.Fatalf("GET /metrics trả %d (%d byte), mong đợi 500 kèm thông báo lỗi", resp.StatusCode, len(body))
	}
	// Lỗi phải có hạn: scrape không được treo hàng phút khi broker không với tới.
	if elapsed := time.Since(started); elapsed > 30*time.Second {
		t.Fatalf("scrape lỗi sau %s, mong đợi dưới 30 giây", elapsed)
	}
}

func TestSkuLengthCountsCodePointsNotUTF16Units(t *testing.T) {
	e := newEnv(t)
	// Ký tự ngoài BMP chiếm 2 đơn vị UTF-16 nhưng là 1 code point: 64 ký tự được nhận, 65 ký tự bị từ chối.
	if status, out := e.post(t, map[string]any{"amount": 10, "sku": strings.Repeat("😀", 64)}); status != http.StatusCreated {
		t.Fatalf("64 code point trả %d, mong đợi 201: %v", status, out)
	}
	if status, _ := e.post(t, map[string]any{"amount": 10, "sku": strings.Repeat("😀", 65)}); status != http.StatusBadRequest {
		t.Fatalf("65 code point trả %d, mong đợi 400", status)
	}
	if n := e.count(t, "select count(*) from orders"); n != 1 {
		t.Fatalf("orders có %d dòng, mong đợi 1", n)
	}
}

func TestParseEventTreatsOrderPlacedWithoutValidAmountAsPoison(t *testing.T) {
	wrap := func(data string) []byte {
		return []byte(`{"eventId":"e1","type":"OrderPlaced","orderId":"o1","data":` + data + `}`)
	}
	if _, ok := ParseEvent(wrap(`{"sku":"sku-1","quantity":1,"amount":5}`)); !ok {
		t.Fatal("event hợp lệ bị coi là poison")
	}
	for _, bad := range []string{
		`{"sku":"sku-1","quantity":1}`,
		`{"sku":"sku-1","quantity":1,"amount":null}`,
		`{"sku":"sku-1","quantity":1,"amount":"5"}`,
	} {
		if _, ok := ParseEvent(wrap(bad)); ok {
			t.Errorf("data %s đáng lẽ là poison", bad)
		}
	}
}

// safeBuffer là bytes.Buffer an toàn khi vừa được os/exec ghi vừa được test đọc.
type safeBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *safeBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *safeBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func TestChaosRelayKilledBetweenPublishAndMarkStillYieldsSingleEffectDownstream(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t)
	if err := SeedStock(ctx, e.pool, sku, 10); err != nil {
		t.Fatalf("SeedStock lỗi: %v", err)
	}
	if _, err := e.pool.Exec(ctx, "create table chaos_marker (event_id text primary key)"); err != nil {
		t.Fatalf("tạo chaos_marker lỗi: %v", err)
	}
	// Consumer chạy trong parent: chúng nhận bản publish của child rồi nhận bản republish.
	e.startConsumer(t, "inventory", ConsumerOptions{})
	e.startConsumer(t, "payment", ConsumerOptions{})
	status, body := e.post(t, map[string]any{"amount": 10, "sku": sku, "quantity": 2})
	if status != http.StatusCreated {
		t.Fatalf("POST /orders trả %d: %v", status, body)
	}
	id := body["id"].(string)
	placed := e.placedEnvelope(t, id)

	// Helper process: chạy lại chính test binary này, chỉ chạy TestHelperProcess. Đó là một relay thật:
	// publish event lên Kafka (acks=all), ghi marker vào Postgres rồi treo TRƯỚC khi đánh dấu published_at.
	cmd := exec.Command(os.Args[0], "-test.run=^TestHelperProcess$")
	cmd.Env = append(os.Environ(), helperEnv+"=1", "SCHEMA="+e.schema, "TOPIC="+e.topic)
	stderr := &safeBuffer{}
	cmd.Stdout, cmd.Stderr = stderr, stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("không khởi động được helper process: %v", err)
	}
	// Cleanup chạy ngược thứ tự đăng ký: khối này đăng ký TRƯỚC reap nên chạy SAU nó, khi buffer stderr đã đầy đủ.
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("stderr của child:\n%s", stderr.String())
		}
	})
	// Luôn kill và reap child, kể cả khi test fail: không để process mồ côi.
	waited := make(chan error, 1)
	exited := make(chan struct{}) // đóng khi child đã thoát, để test fail nhanh nếu child chết lúc khởi động
	go func() {
		waited <- cmd.Wait()
		close(exited)
	}()
	reaped := false
	reap := func() error {
		if reaped {
			return nil
		}
		reaped = true
		_ = cmd.Process.Kill() // SIGKILL, bỏ qua lỗi "process đã thoát"
		select {
		case err := <-waited:
			return err
		case <-time.After(10 * time.Second):
			return errors.New("child không thoát sau SIGKILL")
		}
	}
	t.Cleanup(func() { _ = reap() })
	fail := func(format string, args ...any) {
		t.Helper()
		t.Fatalf(format, args...) // stderr của child được in bởi Cleanup
	}

	// Chờ child báo "đã publish và confirm xong". Child thoát thì báo lỗi ngay, không đợi hết timeout.
	marked := testkit.Eventually(t, 40*time.Second, func() (string, bool) {
		var eventID string
		if err := e.pool.QueryRow(ctx, "select event_id from chaos_marker").Scan(&eventID); err == nil {
			return eventID, true
		}
		select {
		case <-exited:
			fail("child đã thoát trước khi báo marker (xem stderr của child bên dưới)")
		default:
		}
		return "", false
	})
	if marked != placed.EventID {
		fail("child báo đã publish event %q, mong đợi %q", marked, placed.EventID)
	}
	// Bản giao đầu tiên đã được áp dụng một lần ở cả hai consumer.
	for _, consumer := range []string{"inventory", "payment"} {
		testkit.Eventually(t, 40*time.Second, func() (struct{}, bool) {
			return struct{}{}, e.metric(t, "consumer_events_total", map[string]string{"consumer": consumer, "result": "applied"}) == 1
		})
	}
	// Child đang giữ transaction mở: outbox row chưa được đánh dấu.
	if n := e.count(t, "select count(*) from outbox where type = 'OrderPlaced' and published_at is null"); n != 1 {
		fail("có %d dòng OrderPlaced chưa đánh dấu, mong đợi 1", n)
	}

	// SIGKILL: relay chết sau publish, trước UPDATE và COMMIT.
	var exitErr *exec.ExitError
	if err := reap(); !errors.As(err, &exitErr) {
		fail("Wait trả %v, mong đợi *exec.ExitError", err)
	}
	if ws, ok := exitErr.Sys().(syscall.WaitStatus); !ok || !ws.Signaled() || ws.Signal() != syscall.SIGKILL {
		fail("child không chết vì SIGKILL: %v", exitErr)
	}

	// Relay mới trong parent. Postgres rollback transaction của child và nhả lock, nhưng không tức thì:
	// trong lúc lock còn giữ, SKIP LOCKED trả 0 dòng. Bơm relay bằng Eventually tới khi republish xong
	// và mọi event follow-up (InventoryReserved, PaymentCompleted) cũng đã được publish.
	testkit.Eventually(t, 40*time.Second, func() (struct{}, bool) {
		if _, err := e.svc.RunRelay(ctx); err != nil {
			return struct{}{}, false
		}
		unpublished := e.count(t, "select count(*) from outbox where published_at is null")
		total := e.count(t, "select count(*) from outbox")
		return struct{}{}, unpublished == 0 && total == 3
	})

	// Event OrderPlaced nằm trên topic ít nhất hai lần (một từ child, một từ relay mới).
	records, err := ReadTopic(ctx, e.topic)
	if err != nil {
		t.Fatalf("ReadTopic lỗi: %v", err)
	}
	copies := 0
	for _, ev := range eventsOf(records, EventOrderPlaced) {
		if ev.EventID == placed.EventID {
			copies++
		}
	}
	if copies < 2 {
		t.Fatalf("event %s nằm trên topic %d lần, mong đợi ít nhất 2", placed.EventID, copies)
	}
	// Consumer thấy bản trùng nhưng effect chỉ một lần.
	for _, consumer := range []string{"inventory", "payment"} {
		testkit.Eventually(t, 40*time.Second, func() (struct{}, bool) {
			return struct{}{}, e.metric(t, "consumer_events_total", map[string]string{"consumer": consumer, "result": "duplicate"}) >= 1
		})
		if got := e.metric(t, "consumer_events_total", map[string]string{"consumer": consumer, "result": "applied"}); got != 1 {
			t.Fatalf("%s: applied = %v, mong đợi 1", consumer, got)
		}
	}
	if n := e.count(t, "select count(*) from payments"); n != 1 {
		t.Fatalf("payments có %d dòng, mong đợi 1", n)
	}
	if n := e.count(t, "select count(*) from reservations"); n != 1 {
		t.Fatalf("reservations có %d dòng, mong đợi 1", n)
	}
	if got := e.stock(t); got != 8 {
		t.Fatalf("kho còn %d, mong đợi 8", got)
	}
	if got := e.str(t, "select status from orders where id = $1", id); got != "paid" {
		t.Fatalf("order ở trạng thái %q, mong đợi paid", got)
	}
	// Follow-up cũng đúng một lần: bản trùng không sinh thêm event.
	if n := len(eventsOf(records, EventInventoryReserved)); n != 1 {
		t.Fatalf("có %d event InventoryReserved, mong đợi 1", n)
	}
	if n := len(eventsOf(records, EventPaymentCompleted)); n != 1 {
		t.Fatalf("có %d event PaymentCompleted, mong đợi 1", n)
	}
}

// TestHelperProcess không phải test thật: nó chỉ chạy khi test binary được chạy lại với UC02_HELPER=1.
// Khi đó nó là một relay thật: publish event lên Kafka (acks=all), ghi marker vào Postgres rồi treo trong callback
// publish cho tới khi bị SIGKILL, nên relay không bao giờ đánh dấu published_at.
// Có thời hạn cứng 60 giây để không bao giờ thành process mồ côi.
func TestHelperProcess(t *testing.T) {
	if os.Getenv(helperEnv) != "1" {
		t.Skip("chỉ chạy như helper process của test chaos")
	}
	ctx := context.Background()
	pool, err := OpenPool(ctx, os.Getenv("SCHEMA"))
	if err != nil {
		t.Fatalf("OpenPool lỗi: %v", err)
	}
	defer pool.Close()
	producer := NewProducer()
	defer func() { _ = producer.Close() }()
	publish := KafkaPublisher(producer, os.Getenv("TOPIC"))
	hang := make(chan struct{})
	go func() {
		_, err := RunRelay(ctx, pool, func(ctx context.Context, event OutboxEvent) error {
			if err := publish(ctx, event); err != nil {
				return err
			}
			if _, err := pool.Exec(ctx, "insert into chaos_marker (event_id) values ($1)", event.EventID); err != nil {
				return err
			}
			<-hang // treo cho tới khi bị kill
			return nil
		}, 10)
		fmt.Fprintln(os.Stderr, "helper: RunRelay kết thúc:", err)
		os.Exit(1)
	}()
	// Không bao giờ thả hang: hết 60 giây thì thoát, process cha lẽ ra đã kill từ lâu.
	<-time.After(60 * time.Second)
}
