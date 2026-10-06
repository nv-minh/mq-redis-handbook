// Package lab cài đặt full jitter backoff của AWS Architecture Blog:
// delay = rand * min(cap, base * 2^attempt). Đây là hàm thuần, nguồn ngẫu nhiên là tham số.
package lab

import (
	"errors"
	"fmt"
	"math"
	"time"
)

// ErrInvalidArgument bọc mọi lỗi do tham số không hợp lệ (errors.Is khớp được).
var ErrInvalidArgument = errors.New("tham số backoff không hợp lệ")

// Options là hai tham số của công thức.
type Options struct {
	// Base là cận trên của delay ở attempt 0. Phải lớn hơn 0.
	Base time.Duration
	// Cap là trần của cận trên. Phải lớn hơn 0.
	Cap time.Duration
}

// BackoffBound trả về cận trên của delay ở lần retry thứ attempt: min(Cap, Base * 2^attempt).
//
// Hàm dùng số thực chứ không dịch bit: dịch bit trên số nguyên tràn từ attempt 63 và cho kết quả
// sai (có thể âm), còn math.Pow(2, 1100) là +Inf và math.Min với Cap vẫn ra Cap.
func BackoffBound(attempt int, opts Options) (time.Duration, error) {
	if attempt < 0 {
		return 0, fmt.Errorf("%w: attempt phải không âm, nhận được %d", ErrInvalidArgument, attempt)
	}
	if opts.Base <= 0 {
		return 0, fmt.Errorf("%w: Base phải lớn hơn 0, nhận được %s", ErrInvalidArgument, opts.Base)
	}
	if opts.Cap <= 0 {
		return 0, fmt.Errorf("%w: Cap phải lớn hơn 0, nhận được %s", ErrInvalidArgument, opts.Cap)
	}
	bound := math.Min(float64(opts.Cap), float64(opts.Base)*math.Pow(2, float64(attempt)))
	return time.Duration(bound), nil
}

// BackoffDelay trả về full jitter: rand() * min(Cap, Base * 2^attempt), nằm trong [0, BackoffBound).
// rand phải trả về số trong [0, 1), ví dụ rand.Float64 hoặc một PRNG có seed.
// Khi lỗi hàm trả 0 và một lỗi bọc ErrInvalidArgument.
func BackoffDelay(attempt int, opts Options, rand func() float64) (time.Duration, error) {
	if rand == nil {
		return 0, fmt.Errorf("%w: rand không được nil", ErrInvalidArgument)
	}
	bound, err := BackoffBound(attempt, opts)
	if err != nil {
		return 0, err
	}
	r := rand()
	// rand ngoài [0, 1) sẽ phá cận trên, nên từ chối thay vì âm thầm kẹp lại.
	if !(r >= 0 && r < 1) {
		return 0, fmt.Errorf("%w: rand() phải trả về số trong [0, 1), nhận được %v", ErrInvalidArgument, r)
	}
	return time.Duration(r * float64(bound)), nil
}
