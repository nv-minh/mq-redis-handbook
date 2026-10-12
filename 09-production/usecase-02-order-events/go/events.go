package lab

import (
	"bytes"
	"encoding/json"
	"math"
)

// Các loại event của luồng order. Mọi event đi qua cùng một topic, key của message là order id.
const (
	EventOrderPlaced       = "OrderPlaced"
	EventInventoryReserved = "InventoryReserved"
	EventInventoryRejected = "InventoryRejected"
	EventPaymentCompleted  = "PaymentCompleted"
	EventPaymentFailed     = "PaymentFailed"
	EventInventoryReleased = "InventoryReleased"
)

// DomainEvent là envelope của một event trên Kafka (JSON trong value của message).
// EventID được Postgres sinh khi ghi dòng outbox, ổn định qua mọi lần relay publish lại: consumer dedupe theo nó.
type DomainEvent struct {
	EventID string          `json:"eventId"`
	Type    string          `json:"type"`
	OrderID string          `json:"orderId"`
	Data    json.RawMessage `json:"data"`
}

// OrderPlacedData là data của event OrderPlaced.
type OrderPlacedData struct {
	Amount   float64 `json:"amount"`
	Sku      string  `json:"sku"`
	Quantity int     `json:"quantity"`
}

// HandleResult là kết quả xử lý một event của consumer: HandleApplied là effect đã được áp dụng,
// HandleDuplicate là eventId đã xử lý (bỏ qua), HandleIgnored là loại event consumer không quan tâm.
type HandleResult string

// Các kết quả của HandleResult. ResultPoison chỉ dùng cho metric: message không parse được.
const (
	HandleApplied   HandleResult = "applied"
	HandleDuplicate HandleResult = "duplicate"
	HandleIgnored   HandleResult = "ignored"
	ResultPoison    HandleResult = "poison"
)

// ParseEvent đọc value của message thành event. ok=false cho poison event: không phải JSON, không phải object,
// thiếu eventId / type / orderId dạng chuỗi hay data dạng object, hoặc OrderPlaced thiếu sku, quantity, amount hợp lệ.
// Loại event lạ vẫn hợp lệ: consumer bỏ qua chúng để tương thích về sau.
func ParseEvent(value []byte) (DomainEvent, bool) {
	var e DomainEvent
	if err := json.Unmarshal(value, &e); err != nil {
		return DomainEvent{}, false
	}
	if e.EventID == "" || e.Type == "" || e.OrderID == "" {
		return DomainEvent{}, false
	}
	if !bytes.HasPrefix(bytes.TrimSpace(e.Data), []byte("{")) {
		return DomainEvent{}, false
	}
	if e.Type == EventOrderPlaced {
		// Consumer cần các trường này để reserve và charge. Thiếu trường thì retry mãi cũng không xử lý được, nên là poison.
		if _, ok := e.placed(); !ok {
			return DomainEvent{}, false
		}
	}
	return e, true
}

// placed đọc data của OrderPlaced. ok=false nếu thiếu hoặc null một trong sku, quantity, amount, hoặc sku rỗng,
// quantity không phải số nguyên trong [1, MaxInt32] (2.0 và 2e0 vẫn là số nguyên, như Number.isInteger của bản
// TypeScript; cột `integer` của Postgres là int32 nên quantity lớn hơn sẽ làm insert lỗi mãi), amount không hữu hạn. Kiểm tra sự có mặt bằng con trỏ: nếu không, amount
// thiếu sẽ thành 0 và consumer charge 0 (bản TypeScript từ chối vì typeof amount khác number).
func (e DomainEvent) placed() (OrderPlacedData, bool) {
	var raw struct {
		Amount   *float64 `json:"amount"`
		Sku      *string  `json:"sku"`
		Quantity *float64 `json:"quantity"`
	}
	if err := json.Unmarshal(e.Data, &raw); err != nil {
		return OrderPlacedData{}, false
	}
	if raw.Amount == nil || raw.Sku == nil || raw.Quantity == nil {
		return OrderPlacedData{}, false
	}
	q := *raw.Quantity
	if *raw.Sku == "" || q != math.Trunc(q) || q < 1 || q > math.MaxInt32 || math.IsNaN(*raw.Amount) || math.IsInf(*raw.Amount, 0) {
		return OrderPlacedData{}, false
	}
	return OrderPlacedData{Amount: *raw.Amount, Sku: *raw.Sku, Quantity: int(q)}, true
}
