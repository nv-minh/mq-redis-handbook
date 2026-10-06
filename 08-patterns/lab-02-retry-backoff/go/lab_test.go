package lab

import (
	"errors"
	"math"
	"math/rand/v2"
	"testing"
	"time"
)

var opts = Options{Base: 100 * time.Millisecond, Cap: 1_000_000 * time.Millisecond}

// seeded trả về PRNG có seed: test lặp lại được và không phụ thuộc nguồn ngẫu nhiên toàn cục.
func seeded(seed uint64) func() float64 {
	r := rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))
	return r.Float64
}

func constant(v float64) func() float64 { return func() float64 { return v } }

// justBelowOne là số thực lớn nhất nhỏ hơn 1, để thử biên trên của rand.
var justBelowOne = math.Nextafter(1, 0)

func mustDelay(t *testing.T, attempt int, o Options, r func() float64) time.Duration {
	t.Helper()
	d, err := BackoffDelay(attempt, o, r)
	if err != nil {
		t.Fatalf("BackoffDelay(%d) lỗi: %v", attempt, err)
	}
	return d
}

func mustBound(t *testing.T, attempt int, o Options) time.Duration {
	t.Helper()
	b, err := BackoffBound(attempt, o)
	if err != nil {
		t.Fatalf("BackoffBound(%d) lỗi: %v", attempt, err)
	}
	return b
}

func TestDelayIsWithinZeroAndExponentialBound(t *testing.T) {
	r := seeded(42)
	for attempt := 0; attempt <= 8; attempt++ {
		bound := opts.Base * time.Duration(1<<attempt)
		for i := 0; i < 500; i++ {
			d := mustDelay(t, attempt, opts, r)
			if d < 0 || d >= bound {
				t.Fatalf("attempt %d: delay %s ngoài [0, %s)", attempt, d, bound)
			}
		}
		// rand điều khiển được: delay đúng bằng rand * bound.
		if d := mustDelay(t, attempt, opts, constant(0)); d != 0 {
			t.Fatalf("attempt %d: rand=0 cho %s, mong đợi 0", attempt, d)
		}
		if d := mustDelay(t, attempt, opts, constant(0.5)); d != bound/2 {
			t.Fatalf("attempt %d: rand=0.5 cho %s, mong đợi %s", attempt, d, bound/2)
		}
		nearMax := mustDelay(t, attempt, opts, constant(justBelowOne))
		if nearMax >= bound || float64(nearMax) <= float64(bound)*0.999999 {
			t.Fatalf("attempt %d: rand sát 1 cho %s, mong đợi ngay dưới %s", attempt, nearMax, bound)
		}
	}
}

func TestDelayNeverExceedsCap(t *testing.T) {
	capped := Options{Base: 100 * time.Millisecond, Cap: 5 * time.Second}
	r := seeded(7)
	for _, attempt := range []int{6, 10, 30, 52, 100} {
		for i := 0; i < 200; i++ {
			if d := mustDelay(t, attempt, capped, r); d >= capped.Cap {
				t.Fatalf("attempt %d: delay %s vượt cap %s", attempt, d, capped.Cap)
			}
		}
		// Khi 2^attempt * base đã vượt cap thì cận trên là chính cap.
		if b := mustBound(t, attempt, capped); b != capped.Cap {
			t.Fatalf("attempt %d: bound %s, mong đợi cap %s", attempt, b, capped.Cap)
		}
		if d := mustDelay(t, attempt, capped, constant(0.5)); d != capped.Cap/2 {
			t.Fatalf("attempt %d: rand=0.5 cho %s, mong đợi %s", attempt, d, capped.Cap/2)
		}
	}
}

func TestAttemptZeroUsesBase(t *testing.T) {
	if b := mustBound(t, 0, opts); b != opts.Base {
		t.Fatalf("bound attempt 0 = %s, mong đợi base %s", b, opts.Base)
	}
	if d := mustDelay(t, 0, opts, constant(0.5)); d != opts.Base/2 {
		t.Fatalf("rand=0.5 cho %s, mong đợi %s", d, opts.Base/2)
	}
	if d := mustDelay(t, 0, opts, constant(0)); d != 0 {
		t.Fatalf("rand=0 cho %s, mong đợi 0", d)
	}
	if d := mustDelay(t, 0, opts, constant(justBelowOne)); d >= opts.Base {
		t.Fatalf("rand sát 1 cho %s, mong đợi nhỏ hơn base %s", d, opts.Base)
	}
	// Cap nhỏ hơn base thì cap thắng ngay từ attempt 0.
	if b := mustBound(t, 0, Options{Base: 100 * time.Millisecond, Cap: 30 * time.Millisecond}); b != 30*time.Millisecond {
		t.Fatalf("bound = %s, mong đợi 30ms", b)
	}
}

func TestVeryLargeAttemptDoesNotOverflow(t *testing.T) {
	capped := Options{Base: 100 * time.Millisecond, Cap: 5 * time.Second}
	// Dịch bit trên số nguyên sẽ tràn từ attempt 63. Hàm dùng float nên 2^1100 là +Inf và min với cap vẫn đúng.
	for _, attempt := range []int{62, 63, 64, 1023, 1024, 1100, 1_000_000, math.MaxInt} {
		if b := mustBound(t, attempt, capped); b != 5*time.Second {
			t.Fatalf("attempt %d: bound %s, mong đợi 5s", attempt, b)
		}
		if d := mustDelay(t, attempt, capped, constant(0.25)); d != 1250*time.Millisecond {
			t.Fatalf("attempt %d: delay %s, mong đợi 1.25s", attempt, d)
		}
	}
}

func TestDelaysSpreadAcrossTheWholeRangeInsteadOfClustering(t *testing.T) {
	// Đây là lý do có jitter: 2000 client cùng retry lần 5 không nhận cùng một delay.
	r := seeded(2026)
	bound := float64(mustBound(t, 5, opts))
	const samples = 2000
	seen := map[float64]struct{}{}
	lo, hi, sum := math.Inf(1), math.Inf(-1), 0.0
	for range samples {
		d := float64(mustDelay(t, 5, opts, r))
		seen[d] = struct{}{}
		lo, hi, sum = math.Min(lo, d), math.Max(hi, d), sum+d
	}
	mean := sum / samples
	if lo >= bound*0.01 || hi <= bound*0.99 {
		t.Fatalf("delay không phủ cả khoảng: min %.0f, max %.0f, bound %.0f", lo, hi, bound)
	}
	if mean < bound*0.45 || mean > bound*0.55 {
		t.Fatalf("mean %.0f ngoài 45%%..55%% của bound %.0f", mean, bound)
	}
	if len(seen) < 1990 {
		t.Fatalf("chỉ có %d delay khác nhau trong 2000 mẫu", len(seen))
	}
}

func TestInvalidInputsAreRejectedWithAClearError(t *testing.T) {
	half := constant(0.5)
	cases := []struct {
		name    string
		attempt int
		opts    Options
		rand    func() float64
	}{
		{"attempt âm", -1, opts, half},
		{"base bằng 0", 0, Options{Base: 0, Cap: time.Second}, half},
		{"base âm", 0, Options{Base: -time.Millisecond, Cap: time.Second}, half},
		{"cap bằng 0", 0, Options{Base: time.Millisecond, Cap: 0}, half},
		{"cap âm", 0, Options{Base: time.Millisecond, Cap: -time.Second}, half},
		{"rand bằng 1", 0, opts, constant(1)},
		{"rand âm", 0, opts, constant(-0.1)},
		{"rand NaN", 0, opts, constant(math.NaN())},
		{"rand nil", 0, opts, nil},
	}
	for _, c := range cases {
		d, err := BackoffDelay(c.attempt, c.opts, c.rand)
		if !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("%s: err = %v, mong đợi ErrInvalidArgument", c.name, err)
		}
		if d != 0 {
			t.Errorf("%s: delay %s, mong đợi 0 khi lỗi", c.name, d)
		}
	}
}
