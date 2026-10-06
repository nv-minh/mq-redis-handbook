// Command demo chạy saga đặt hàng qua ba service giả lập (kho, thanh toán, vận chuyển), mỗi service giữ trạng thái
// trong bộ nhớ. Ba kịch bản: thành công, thanh toán lỗi (kho được bù), và vận chuyển lỗi cùng lúc hoàn tiền cũng lỗi
// (saga báo CompensationError, cần người vận hành). Giao dịch bù idempotent: gọi lại không làm sai trạng thái.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"

	lab "github.com/nv-minh/mq-redis-handbook/08-patterns/lab-06-saga/go"
)

type failures struct{ payment, shipment, refund bool }

func orderSteps(orderID string, f failures) ([]lab.SagaStep, func() string) {
	reserved, charged, shipped := map[string]bool{}, map[string]bool{}, map[string]bool{}
	say := func(format string, args ...any) { fmt.Printf("    "+format+"\n", args...) }
	steps := []lab.SagaStep{
		{
			Name: "reserve-inventory",
			Run: func(context.Context) error {
				reserved[orderID] = true
				say("kho: giữ hàng cho %s", orderID)
				return nil
			},
			Compensate: func(context.Context) error {
				// Idempotent: nhả hàng hai lần vẫn cho cùng trạng thái (đã nhả).
				delete(reserved, orderID)
				say("kho: nhả hàng của %s (bù)", orderID)
				return nil
			},
		},
		{
			Name: "charge-payment",
			Run: func(context.Context) error {
				if f.payment {
					return errors.New("thẻ bị từ chối")
				}
				charged[orderID] = true
				say("thanh toán: trừ tiền %s", orderID)
				return nil
			},
			Compensate: func(context.Context) error {
				if f.refund {
					return errors.New("cổng thanh toán không phản hồi khi hoàn tiền")
				}
				delete(charged, orderID)
				say("thanh toán: hoàn tiền %s (bù)", orderID)
				return nil
			},
		},
		{
			Name: "create-shipment",
			Run: func(context.Context) error {
				if f.shipment {
					return errors.New("hãng vận chuyển hết xe")
				}
				shipped[orderID] = true
				say("vận chuyển: tạo đơn giao %s", orderID)
				return nil
			},
			Compensate: func(context.Context) error {
				delete(shipped, orderID)
				say("vận chuyển: hủy đơn giao %s (bù)", orderID)
				return nil
			},
		},
	}
	state := func() string {
		return fmt.Sprintf("giữ hàng=%t, đã trừ tiền=%t, đã tạo đơn giao=%t", reserved[orderID], charged[orderID], shipped[orderID])
	}
	return steps, state
}

func scenario(title, orderID string, f failures) error {
	fmt.Printf("\n== %s ==\n", title)
	steps, state := orderSteps(orderID, f)
	outcome, err := lab.RunOrderSaga(context.Background(), steps)
	var report *lab.CompensationError
	switch {
	case errors.As(err, &report):
		fmt.Printf("  saga KHÔNG hoàn tác được hết: %v\n", report)
		compensated := "(không có)"
		if len(report.Compensated) > 0 {
			compensated = strings.Join(report.Compensated, ", ")
		}
		fmt.Printf("  bước đã bù được: %s\n", compensated)
		fmt.Println("  -> cần người vận hành hoặc hàng đợi retry cho compensation")
	case err != nil:
		return err
	default:
		fmt.Printf("  kết quả saga: %s\n", outcome)
	}
	fmt.Printf("  trạng thái cuối: %s\n", state())
	return nil
}

func main() {
	for _, s := range []struct {
		title, id string
		f         failures
	}{
		{"1. Mọi bước thành công", "order-1", failures{}},
		{"2. Thanh toán lỗi: chỉ bước kho (đã xong) được bù", "order-2", failures{payment: true}},
		{"3. Vận chuyển lỗi và hoàn tiền cũng lỗi", "order-3", failures{shipment: true, refund: true}},
	} {
		if err := scenario(s.title, s.id, s.f); err != nil {
			log.Fatal(err)
		}
	}
	fmt.Println("\nKết luận: bù theo thứ tự ngược, bước lỗi không được bù, và một compensation hỏng không chặn compensation còn lại nhưng phải được báo.")
}
