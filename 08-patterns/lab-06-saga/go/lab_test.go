package lab

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
)

type options struct{ failRun, failCompensate bool }

// step tạo một bước ghi lại mọi lần gọi vào events. failRun và failCompensate ép bước đó trả lỗi.
func step(events *[]string, name string, o options) SagaStep {
	return SagaStep{
		Name: name,
		Run: func(context.Context) error {
			*events = append(*events, "run:"+name)
			if o.failRun {
				return errors.New(name + " chạy lỗi")
			}
			return nil
		},
		Compensate: func(context.Context) error {
			*events = append(*events, "compensate:"+name)
			if o.failCompensate {
				return errors.New(name + " bù lỗi")
			}
			return nil
		},
	}
}

func TestAllStepsSucceedReturnsCompleted(t *testing.T) {
	var events []string
	outcome, err := RunOrderSaga(context.Background(), []SagaStep{
		step(&events, "a", options{}), step(&events, "b", options{}), step(&events, "c", options{}),
	})
	if err != nil || outcome != Completed {
		t.Fatalf("RunOrderSaga = %q, %v, mong đợi completed, nil", outcome, err)
	}
	// Chạy đúng thứ tự khai báo và không có compensation nào.
	if want := []string{"run:a", "run:b", "run:c"}; !slices.Equal(events, want) {
		t.Fatalf("events = %v, mong đợi %v", events, want)
	}
}

func TestFailureCompensatesCompletedStepsInReverseOrder(t *testing.T) {
	var events []string
	outcome, err := RunOrderSaga(context.Background(), []SagaStep{
		step(&events, "a", options{}), step(&events, "b", options{}),
		step(&events, "c", options{failRun: true}), step(&events, "d", options{}),
	})
	if err != nil || outcome != Compensated {
		t.Fatalf("RunOrderSaga = %q, %v, mong đợi compensated, nil", outcome, err)
	}
	// Bước c lỗi nên d không bao giờ chạy. Chỉ a và b (đã xong) được bù, theo thứ tự ngược, và bước lỗi c không được bù.
	want := []string{"run:a", "run:b", "run:c", "compensate:b", "compensate:a"}
	if !slices.Equal(events, want) {
		t.Fatalf("events = %v, mong đợi %v", events, want)
	}
}

func TestCompensationFailureIsReportedAndRemainingCompensationsStillRun(t *testing.T) {
	var events []string
	outcome, err := RunOrderSaga(context.Background(), []SagaStep{
		step(&events, "a", options{failCompensate: true}),
		step(&events, "b", options{}),
		step(&events, "c", options{failCompensate: true}),
		step(&events, "d", options{failRun: true}),
	})

	// Saga không được nói "compensated" khi còn bước chưa bù được: trả outcome rỗng cùng *CompensationError.
	var report *CompensationError
	if !errors.As(err, &report) {
		t.Fatalf("err = %v, mong đợi *CompensationError", err)
	}
	if outcome != "" {
		t.Fatalf("outcome = %q, mong đợi rỗng khi compensation lỗi", outcome)
	}

	// c lỗi khi bù nhưng b và a vẫn được bù: mỗi bước đã xong được thử bù đúng một lần, theo thứ tự ngược.
	want := []string{"run:a", "run:b", "run:c", "run:d", "compensate:c", "compensate:b", "compensate:a"}
	if !slices.Equal(events, want) {
		t.Fatalf("events = %v, mong đợi %v", events, want)
	}
	if report.FailedStep != "d" || report.StepErr == nil || report.StepErr.Error() != "d chạy lỗi" {
		t.Fatalf("FailedStep = %q, StepErr = %v", report.FailedStep, report.StepErr)
	}
	if !slices.Equal(report.Compensated, []string{"b"}) {
		t.Fatalf("Compensated = %v, mong đợi [b]", report.Compensated)
	}
	var steps, messages []string
	for _, f := range report.CompensationErrors {
		steps = append(steps, f.Step)
		messages = append(messages, f.Err.Error())
	}
	if !slices.Equal(steps, []string{"c", "a"}) || !slices.Equal(messages, []string{"c bù lỗi", "a bù lỗi"}) {
		t.Fatalf("CompensationErrors = %v %v, mong đợi [c a] [c bù lỗi a bù lỗi]", steps, messages)
	}
	// errors.Is thấy được cả lỗi gốc lẫn từng lỗi bù.
	if !errors.Is(err, report.StepErr) || !errors.Is(err, report.CompensationErrors[0].Err) {
		t.Fatalf("errors.Is không tìm thấy lỗi thành phần trong %v", err)
	}
}

func TestCompensationErrorsNameTheStepsInTheErrorMessage(t *testing.T) {
	var events []string
	_, err := RunOrderSaga(context.Background(), []SagaStep{
		step(&events, "reserve-inventory", options{failCompensate: true}),
		step(&events, "charge-payment", options{failRun: true}),
	})
	if err == nil {
		t.Fatal("mong đợi lỗi")
	}
	// Người vận hành đọc message này: phải thấy bước nào lỗi, lỗi gì, và bước nào chưa bù được.
	for _, want := range []string{"charge-payment", "charge-payment chạy lỗi", "reserve-inventory", "reserve-inventory bù lỗi"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("message %q thiếu %q", err.Error(), want)
		}
	}
}

func TestSagaWithZeroStepsReturnsCompleted(t *testing.T) {
	outcome, err := RunOrderSaga(context.Background(), nil)
	if err != nil || outcome != Completed {
		t.Fatalf("RunOrderSaga = %q, %v, mong đợi completed, nil", outcome, err)
	}
}

func TestFirstStepFailingCompensatesNothing(t *testing.T) {
	var events []string
	outcome, err := RunOrderSaga(context.Background(), []SagaStep{
		step(&events, "a", options{failRun: true}), step(&events, "b", options{}),
	})
	if err != nil || outcome != Compensated {
		t.Fatalf("RunOrderSaga = %q, %v, mong đợi compensated, nil", outcome, err)
	}
	if want := []string{"run:a"}; !slices.Equal(events, want) {
		t.Fatalf("events = %v, mong đợi %v", events, want)
	}
}

// Chỉ có ở Go (TypeScript không có context): ctx bị hủy là lý do phổ biến khiến một bước lỗi,
// và compensation vẫn phải chạy dù ctx đã hủy, nếu không saga bỏ dở các bước đã xong.
func TestCompensationRunsEvenWhenTheContextIsCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var events []string
	compensateSawCancelled := false
	a := step(&events, "a", options{})
	a.Compensate = func(ctx context.Context) error {
		events = append(events, "compensate:a")
		compensateSawCancelled = ctx.Err() != nil
		return nil
	}
	b := step(&events, "b", options{})
	b.Run = func(ctx context.Context) error {
		cancel()
		return ctx.Err()
	}
	outcome, err := RunOrderSaga(ctx, []SagaStep{a, b})
	if err != nil || outcome != Compensated {
		t.Fatalf("RunOrderSaga = %q, %v, mong đợi compensated, nil", outcome, err)
	}
	if !slices.Equal(events, []string{"run:a", "compensate:a"}) || compensateSawCancelled {
		t.Fatalf("events = %v, compensate thấy ctx đã hủy = %v", events, compensateSawCancelled)
	}
}
