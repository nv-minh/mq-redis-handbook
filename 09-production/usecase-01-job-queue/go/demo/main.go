// Command demo cho thấy job queue gửi email với asynq: retry với exponential backoff, task hết attempts
// nằm trong archive rồi được operator replay, và graceful shutdown chờ task đang chạy. Queue bị xóa ở cuối.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/hibiken/asynq"
	lab "github.com/nv-minh/mq-redis-handbook/09-production/usecase-01-job-queue/go"
	"github.com/nv-minh/mq-redis-handbook/internal/testkit"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

// waitFor poll tới khi cond đúng hoặc hết timeout.
func waitFor(timeout time.Duration, cond func() (bool, error)) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ok, err := cond()
		if err != nil {
			return err
		}
		if ok {
			return nil
		}
		time.Sleep(20 * time.Millisecond)
	}
	return errors.New("hết thời gian chờ")
}

// reachable cho biết endpoint còn trả lời HTTP hay không.
func reachable(url string) bool {
	resp, err := http.Get(url)
	if err != nil {
		return false
	}
	_ = resp.Body.Close()
	return true
}

func metrics(system *lab.JobSystem) (string, error) {
	resp, err := http.Get(system.MetricsURL())
	if err != nil {
		return "", err
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	var lines []string
	for _, line := range strings.Split(string(body), "\n") {
		if strings.HasPrefix(line, "jobs_processed_total") {
			lines = append(lines, line)
		}
	}
	return strings.Join(lines, "  |  "), nil
}

func run() (err error) {
	ctx := context.Background()
	url := os.Getenv("REDIS_URL")
	if url == "" {
		url = "redis://127.0.0.1:6379"
	}
	queue := testkit.UniqueName("demo-email")
	startedAt := time.Now()
	at := func() string { return fmt.Sprintf("+%5d ms", time.Since(startedAt).Milliseconds()) }

	var smtpHealthy atomic.Bool
	var welcomeFailures atomic.Int32
	welcomeFailures.Store(2)
	slowStarted := make(chan struct{}, 1)
	releaseSlow := make(chan struct{})
	var released atomic.Bool
	release := func() {
		if released.CompareAndSwap(false, true) {
			close(releaseSlow)
		}
	}
	sender := func(_ context.Context, email lab.Email, attempt lab.Attempt) error {
		fmt.Printf("%s  [sender] lần %d gửi %q tới %s\n", at(), attempt.Number, email.Subject, email.To)
		switch {
		case email.Subject == "Email chậm":
			slowStarted <- struct{}{}
			<-releaseSlow
			fmt.Printf("%s  [sender] email chậm đã gửi xong\n", at())
		case email.Subject == "Hóa đơn tháng 10" && !smtpHealthy.Load():
			return errors.New("SMTP từ chối: 421 try again")
		case email.Subject == "Chào mừng" && welcomeFailures.Add(-1) >= 0:
			return errors.New("SMTP timeout")
		}
		return nil
	}

	var systems []*lab.JobSystem
	defer func() {
		release()
		for _, system := range systems {
			err = errors.Join(err, system.Shutdown())
		}
		// DestroyQueue chạy sau khi mọi server đã đóng.
		err = errors.Join(err, lab.DestroyQueue(url, queue))
	}()
	system, err := lab.NewJobSystem(lab.Options{
		RedisURL: url, Queue: queue, Sender: sender,
		Attempts: 3, RetryBase: time.Second, ShutdownTimeout: 5 * time.Second,
	})
	if err != nil {
		return err
	}
	systems = append(systems, system)
	fmt.Printf("Metrics endpoint: %s\n\n", system.MetricsURL())
	inspector := system.Inspector()
	state := func(id string) (asynq.TaskState, error) {
		info, err := inspector.GetTaskInfo(queue, id)
		if err != nil {
			return 0, err
		}
		return info.State, nil
	}
	waitState := func(id string, want asynq.TaskState) error {
		return waitFor(20*time.Second, func() (bool, error) {
			s, err := state(id)
			return err == nil && s == want, nil
		})
	}

	fmt.Println("== 1. Sender lỗi hai lần rồi thành công: retry với exponential backoff (1 s, 2 s) ==")
	welcomeID, err := system.EnqueueEmail(ctx, "an@example.com", "Chào mừng")
	if err != nil {
		return err
	}
	if err := waitState(welcomeID, asynq.TaskStateCompleted); err != nil {
		return err
	}
	line, err := metrics(system)
	if err != nil {
		return err
	}
	fmt.Printf("task %s: completed sau 3 lần chạy (Retried = 2)\n", welcomeID[:8])
	fmt.Printf("metrics: %s\n", line)
	fmt.Println("failed đếm từng lần thử lỗi (2), completed đếm lần thành công (1).")
	fmt.Println("asynq lên lịch retry theo Unix giây (cắt phần lẻ): lần retry đầu có thể chạy sớm hơn 1 s, tối đa gần 1 s.")

	fmt.Println("\n== 2. Sender luôn lỗi: hết 3 attempts, task nằm trong archive, operator replay sau khi sửa SMTP ==")
	invoiceID, err := system.EnqueueEmail(ctx, "binh@example.com", "Hóa đơn tháng 10")
	if err != nil {
		return err
	}
	if err := waitState(invoiceID, asynq.TaskStateArchived); err != nil {
		return err
	}
	dead, err := inspector.GetTaskInfo(queue, invoiceID)
	if err != nil {
		return err
	}
	fmt.Printf("task %s: archived, Retried=%d, MaxRetry=%d, lý do: %q\n", invoiceID[:8], dead.Retried, dead.MaxRetry, dead.LastErr)
	fmt.Println("Task vẫn còn trong Redis (archive giữ tối đa 10000 task và xóa sau 90 ngày) để điều tra.")
	smtpHealthy.Store(true)
	fmt.Println("-- operator sửa SMTP rồi replay task (Inspector.RunTask) --")
	if err := inspector.RunTask(queue, invoiceID); err != nil {
		return err
	}
	if err := waitState(invoiceID, asynq.TaskStateCompleted); err != nil {
		return err
	}
	fmt.Printf("task %s: completed sau khi replay (RunTask không đặt lại Retried nên sender thấy lần 3).\n", invoiceID[:8])
	if line, err = metrics(system); err != nil {
		return err
	}
	fmt.Printf("metrics: %s\n", line)

	fmt.Println("\n== 3. Graceful shutdown: Shutdown() chờ task đang chạy xong (ShutdownTimeout 5 s) ==")
	slowID, err := system.EnqueueEmail(ctx, "chi@example.com", "Email chậm")
	if err != nil {
		return err
	}
	select {
	case <-slowStarted:
	case <-time.After(10 * time.Second):
		return errors.New("email chậm không bắt đầu")
	}
	fmt.Printf("%s  gọi Shutdown() trong lúc email chậm đang chạy\n", at())
	done := make(chan error, 1)
	go func() {
		err := system.Shutdown()
		fmt.Printf("%s  Shutdown đã trả về\n", at())
		done <- err
	}()
	time.Sleep(700 * time.Millisecond)
	if line, err = metrics(system); err != nil {
		return err
	}
	fmt.Printf("%s  sau 700 ms: metrics vẫn truy cập được -> %s\n", at(), line)
	release()
	if err := <-done; err != nil {
		return err
	}
	closed := "đã đóng"
	if reachable(system.MetricsURL()) {
		closed = "vẫn mở (lỗi)"
	}
	fmt.Printf("metrics endpoint sau Shutdown: %s\n", closed)
	reader, err := lab.NewJobSystem(lab.Options{
		RedisURL: url, Queue: queue,
		Sender: func(context.Context, lab.Email, lab.Attempt) error { return nil },
	})
	if err != nil {
		return err
	}
	systems = append(systems, reader)
	info, err := reader.Inspector().GetTaskInfo(queue, slowID)
	if err != nil {
		return err
	}
	fmt.Printf("task %s: %v, Retried=%d: không retry, không mất.\n", slowID[:8], info.State, info.Retried)
	fmt.Println("\nGiao hàng là at-least-once: nếu server chết giữa chừng hoặc Shutdown hết hạn, task chạy lại, nên sender phải idempotent.")
	return nil
}
