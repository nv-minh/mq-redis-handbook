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

// fakeTB records Fatalf calls instead of aborting the test, so failure paths can be asserted.
// FailNow panics with a sentinel so that Fatalf keeps its "does not return" behavior.
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

// runFatal runs fn and returns the recorded Fatalf message ("" if fn returned normally).
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
		t.Fatal("expected WaitForPort to call Fatalf on a closed port")
	}
	if !strings.Contains(msg, "127.0.0.1:") || !strings.Contains(msg, port) {
		t.Fatalf("message %q should contain %q", msg, addr)
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("WaitForPort hung for %s, expected to fail fast", elapsed)
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
		t.Fatal("listener failed to open")
	}
}

func TestUniqueNameDiffers(t *testing.T) {
	a, b := UniqueName("orders"), UniqueName("orders")
	if a == b {
		t.Fatalf("expected different names, got %q twice", a)
	}
	for _, n := range []string{a, b} {
		if !strings.HasPrefix(n, "orders-") {
			t.Fatalf("name %q should start with prefix", n)
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
		t.Fatalf("got %q", got)
	}
}

func TestEventuallyFailsAfterTimeout(t *testing.T) {
	fake := &fakeTB{TB: t}
	msg := runFatal(fake, func() {
		Eventually(fake, 200*time.Millisecond, func() (int, bool) { return 0, false })
	})
	if msg == "" {
		t.Fatal("expected Eventually to call Fatalf after timeout")
	}
}
