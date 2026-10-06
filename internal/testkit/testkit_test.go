package testkit

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// fakeTB ghi lại các lần gọi Fatalf thay vì dừng test, để có thể assert đường lỗi.
// FailNow panic bằng một sentinel để Fatalf giữ hành vi "không return".
type fakeTB struct {
	testing.TB
	fatal string
}

type fatalSentinel struct{}

func (f *fakeTB) Helper() {}

func (f *fakeTB) Fatalf(format string, args ...any) {
	f.fatal = fmt.Sprintf(format, args...)
	panic(fatalSentinel{})
}

func (f *fakeTB) Logf(format string, args ...any) {}

// runFatal chạy fn và trả về message Fatalf đã ghi lại ("" nếu fn return bình thường).
func runFatal(f *fakeTB, fn func()) (msg string) {
	defer func() {
		if r := recover(); r != nil {
			if _, ok := r.(fatalSentinel); !ok {
				panic(r)
			}
			msg = f.fatal
		}
	}()
	fn()
	return ""
}

func freePort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	return addr
}

func TestWaitForPortFailsWithAddrInMessage(t *testing.T) {
	addr := freePort(t)
	_, port, _ := net.SplitHostPort(addr)
	fake := &fakeTB{TB: t}

	start := time.Now()
	msg := runFatal(fake, func() { WaitForPort(fake, addr, 300*time.Millisecond) })

	if msg == "" {
		t.Fatal("mong đợi WaitForPort gọi Fatalf trên port đã đóng")
	}
	if !strings.Contains(msg, "127.0.0.1:") || !strings.Contains(msg, port) {
		t.Fatalf("message %q phải chứa %q", msg, addr)
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("WaitForPort bị treo %s, mong đợi nó fail nhanh", elapsed)
	}
}

func TestWaitForPortSucceedsWhenListenerOpens(t *testing.T) {
	addr := freePort(t)
	done := make(chan net.Listener, 1)
	go func() {
		time.Sleep(100 * time.Millisecond)
		l, err := net.Listen("tcp", addr)
		if err != nil {
			close(done)
			return
		}
		done <- l
	}()

	WaitForPort(t, addr, 5*time.Second)

	if l, ok := <-done; ok {
		_ = l.Close()
	} else {
		t.Fatal("listener không mở được")
	}
}

func TestUniqueNameDiffers(t *testing.T) {
	a, b := UniqueName("orders"), UniqueName("orders")
	if a == b {
		t.Fatalf("mong đợi hai tên khác nhau, nhận %q hai lần", a)
	}
	for _, n := range []string{a, b} {
		if !strings.HasPrefix(n, "orders-") {
			t.Fatalf("tên %q phải bắt đầu bằng prefix", n)
		}
	}
}

func TestEventuallyReturnsValue(t *testing.T) {
	var calls atomic.Int32
	got := Eventually(t, 2*time.Second, func() (string, bool) {
		if calls.Add(1) >= 3 {
			return "ready-" + strconv.Itoa(int(calls.Load())), true
		}
		return "", false
	})
	if got != "ready-3" {
		t.Fatalf("nhận %q", got)
	}
}

func TestEventuallyFailsAfterTimeout(t *testing.T) {
	fake := &fakeTB{TB: t}
	msg := runFatal(fake, func() {
		Eventually(fake, 200*time.Millisecond, func() (int, bool) { return 0, false })
	})
	if msg == "" {
		t.Fatal("mong đợi Eventually gọi Fatalf sau timeout")
	}
}
