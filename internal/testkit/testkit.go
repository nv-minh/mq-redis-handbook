// Package testkit holds the helpers every lab test uses: wait for a broker port,
// generate collision-free resource names, and poll instead of sleeping.
package testkit

import (
	"crypto/rand"
	"net"
	"strconv"
	"testing"
	"time"
)

const pollInterval = 100 * time.Millisecond

// WaitForPort blocks until a TCP listener accepts connections on addr, or fails the test
// with addr and the last dial error once timeout elapses. It never hangs.
func WaitForPort(t testing.TB, addr string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var lastErr error
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			break
		}
		conn, err := net.DialTimeout("tcp", addr, min(remaining, time.Second))
		if err == nil {
			_ = conn.Close()
			return
		}
		lastErr = err
		time.Sleep(min(pollInterval, max(time.Until(deadline), 0)))
	}
	t.Fatalf("timed out after %s waiting for %s (last error: %v). Is Docker running and the service up? Try: make up",
		timeout, addr, lastErr)
}

const alphabet = "0123456789abcdefghijklmnopqrstuvwxyz"

// UniqueName returns prefix-<time base36>-<6 random chars>, so re-running a lab on a dirty
// broker never collides with keys, queues or topics left by an earlier run.
func UniqueName(prefix string) string {
	buf := make([]byte, 6)
	if _, err := rand.Read(buf); err != nil {
		panic("testkit: crypto/rand failed: " + err.Error())
	}
	for i, b := range buf {
		buf[i] = alphabet[int(b)%len(alphabet)]
	}
	return prefix + "-" + strconv.FormatInt(time.Now().UnixMilli(), 36) + "-" + string(buf)
}

// Eventually polls fn every 100ms until it reports ok, then returns its value.
// It fails the test if timeout elapses first. Use this instead of fixed sleeps.
func Eventually[T any](t testing.TB, timeout time.Duration, fn func() (T, bool)) T {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if v, ok := fn(); ok {
			return v
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("eventually: condition not met within %s", timeout)
		}
		time.Sleep(min(pollInterval, max(time.Until(deadline), 0)))
	}
}
