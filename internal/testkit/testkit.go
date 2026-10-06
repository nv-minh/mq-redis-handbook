// Package testkit chứa các helper mà mọi test của lab dùng: chờ port của broker,
// sinh tên resource không đụng nhau, và poll thay vì sleep.
package testkit

import (
	"crypto/rand"
	"net"
	"strconv"
	"testing"
	"time"
)

const pollInterval = 100 * time.Millisecond

// WaitForPort chặn tới khi một TCP listener nhận kết nối trên addr, hoặc làm test fail
// kèm addr và lỗi dial gần nhất khi hết timeout. Hàm không bao giờ treo.
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
	t.Fatalf("hết %s vẫn chưa kết nối được tới %s (lỗi gần nhất: %v). Docker đã chạy và service đã lên chưa? Thử: make up",
		timeout, addr, lastErr)
}

const alphabet = "0123456789abcdefghijklmnopqrstuvwxyz"

// UniqueName trả về prefix-<time base36>-<6 ký tự ngẫu nhiên>, để chạy lại lab trên broker
// còn dữ liệu cũ không bao giờ đụng key, queue hay topic mà lần chạy trước để lại.
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

// Eventually poll fn mỗi 100ms tới khi fn báo ok rồi trả về giá trị của nó.
// Hàm làm test fail nếu hết timeout trước. Dùng hàm này thay cho sleep cố định.
func Eventually[T any](t testing.TB, timeout time.Duration, fn func() (T, bool)) T {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if v, ok := fn(); ok {
			return v
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("eventually: điều kiện chưa đạt sau %s", timeout)
		}
		time.Sleep(min(pollInterval, max(time.Until(deadline), 0)))
	}
}
