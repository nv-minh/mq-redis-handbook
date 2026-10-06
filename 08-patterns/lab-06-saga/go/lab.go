// Package lab cài đặt saga kiểu orchestration: một hàm điều phối chạy các local transaction theo thứ tự
// và, khi một bước lỗi, chạy giao dịch bù (compensation) của các bước đã xong theo thứ tự ngược.
package lab

import (
	"context"
	"fmt"
	"strings"
)

// SagaStep là một bước của saga. Run là local transaction của một service, Compensate là giao dịch bù hoàn tác
// tác dụng của Run. Compensate phải idempotent: saga thật có thể phải gọi lại nó sau khi lần đầu lỗi hoặc không rõ kết quả.
type SagaStep struct {
	Name       string
	Run        func(ctx context.Context) error
	Compensate func(ctx context.Context) error
}

// Outcome là kết quả khi mọi thứ diễn ra sạch sẽ.
type Outcome string

const (
	// Completed: mọi bước đã xong.
	Completed Outcome = "completed"
	// Compensated: một bước lỗi và mọi bước đã xong trước nó đều đã được bù.
	Compensated Outcome = "compensated"
)

// CompensationFailure là một giao dịch bù bị lỗi.
type CompensationFailure struct {
	// Step là tên bước mà giao dịch bù bị lỗi.
	Step string
	Err  error
}

// CompensationError cho biết saga không hoàn tác được hết: có bước chạy lỗi VÀ ít nhất một giao dịch bù cũng lỗi,
// nên hệ thống đang ở trạng thái không nhất quán và cần người vận hành (hoặc một hàng đợi retry cho compensation)
// xử lý tiếp. Đây là lý do RunOrderSaga trả lỗi thay vì Compensated: kết quả "compensated" chỉ được trả khi nó đúng.
type CompensationError struct {
	// FailedStep là tên bước chạy lỗi đã kích hoạt compensation.
	FailedStep string
	// StepErr là lỗi gốc của bước chạy lỗi.
	StepErr error
	// CompensationErrors là các bước bù thất bại, theo thứ tự bù (ngược).
	CompensationErrors []CompensationFailure
	// Compensated là các bước đã được bù thành công, theo thứ tự bù.
	Compensated []string
}

func (e *CompensationError) Error() string {
	stuck := make([]string, 0, len(e.CompensationErrors))
	for _, f := range e.CompensationErrors {
		stuck = append(stuck, fmt.Sprintf("%q (%v)", f.Step, f.Err))
	}
	return fmt.Sprintf("saga lỗi ở bước %q (%v) và không bù được các bước: %s",
		e.FailedStep, e.StepErr, strings.Join(stuck, ", "))
}

// Unwrap cho phép errors.Is và errors.As tìm thấy lỗi gốc lẫn từng lỗi bù.
func (e *CompensationError) Unwrap() []error {
	errs := []error{e.StepErr}
	for _, f := range e.CompensationErrors {
		errs = append(errs, f.Err)
	}
	return errs
}

// RunOrderSaga chạy saga theo kiểu orchestration: bước nào chạy trước, bước nào bù, đều do hàm này quyết định ở một chỗ.
//
//   - Chạy các bước lần lượt theo thứ tự. Hết bước thì trả Completed (kể cả khi không có bước nào).
//   - Bước đầu tiên lỗi: các bước SAU nó không chạy, và các bước ĐÃ XONG được bù theo thứ tự NGƯỢC rồi trả Compensated.
//     Bước lỗi không được bù: ta coi local transaction của nó đã tự rollback. Nếu một bước có thể xong dở thì phải
//     tự dọn bên trong Run.
//   - Giao dịch bù lỗi: mỗi bước còn lại VẪN được bù đúng một lần (một compensation hỏng không được chặn các bước kia),
//     rồi hàm trả outcome rỗng cùng *CompensationError mang tên bước lỗi, lỗi gốc, mọi lỗi bù và danh sách bước đã bù.
//
// Compensation chạy với context.WithoutCancel(ctx): ctx bị hủy thường chính là lý do một bước lỗi, và việc bù vẫn
// phải chạy tới cùng. Lab không retry: saga thật nên retry bước lỗi như lỗi tạm thời (có backoff) trước khi bù,
// và retry compensation (hoặc đẩy vào hàng đợi retry) trước khi gọi người vận hành.
func RunOrderSaga(ctx context.Context, steps []SagaStep) (Outcome, error) {
	completed := make([]SagaStep, 0, len(steps))
	for _, s := range steps {
		if err := s.Run(ctx); err != nil {
			return compensate(context.WithoutCancel(ctx), s.Name, err, completed)
		}
		completed = append(completed, s)
	}
	return Completed, nil
}

func compensate(ctx context.Context, failedStep string, stepErr error, completed []SagaStep) (Outcome, error) {
	report := &CompensationError{FailedStep: failedStep, StepErr: stepErr}
	for i := len(completed) - 1; i >= 0; i-- {
		s := completed[i]
		if err := s.Compensate(ctx); err != nil {
			report.CompensationErrors = append(report.CompensationErrors, CompensationFailure{Step: s.Name, Err: err})
			continue
		}
		report.Compensated = append(report.Compensated, s.Name)
	}
	if len(report.CompensationErrors) > 0 {
		return "", report
	}
	return Compensated, nil
}
