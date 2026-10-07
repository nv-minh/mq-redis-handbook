package lab

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hibiken/asynq"
	"github.com/nv-minh/mq-redis-handbook/internal/testkit"
	"github.com/redis/go-redis/v9"
)

func redisURL() string {
	if url := os.Getenv("REDIS_URL"); url != "" {
		return url
	}
	return "redis://127.0.0.1:6379"
}

// gate giữ sender lại cho tới khi test gọi release.
type gate struct {
	once   sync.Once
	opened chan struct{}
}

func (g *gate) wait()    { <-g.opened }
func (g *gate) release() { g.once.Do(func() { close(g.opened) }) }

type env struct {
	t     *testing.T
	redis *redis.Client
}

func newEnv(t *testing.T) *env {
	t.Helper()
	opts, err := redis.ParseURL(redisURL())
	if err != nil {
		t.Fatalf("REDIS_URL không hợp lệ: %v", err)
	}
	e := &env{t: t, redis: redis.NewClient(opts)}
	t.Cleanup(func() { _ = e.redis.Close() })
	return e
}

func (e *env) scan(pattern string) []string {
	e.t.Helper()
	keys, err := e.redis.Keys(context.Background(), pattern).Result()
	if err != nil {
		e.t.Fatalf("KEYS %s lỗi: %v", pattern, err)
	}
	return keys
}

// queueName sinh tên queue duy nhất và đăng ký việc dọn. Cleanup chạy ngược thứ tự đăng ký,
// nên hàm này phải được gọi trước newSystem để việc dọn queue chạy SAU khi mọi server đã đóng.
func (e *env) queueName() string {
	e.t.Helper()
	name := testkit.UniqueName("job-queue")
	e.t.Cleanup(func() {
		if err := DestroyQueue(redisURL(), name); err != nil {
			e.t.Errorf("DestroyQueue lỗi: %v", err)
		}
		if left := e.scan("asynq:{" + name + "}*"); len(left) > 0 {
			e.t.Errorf("còn key của queue %s sau khi dọn: %v", name, left)
		}
	})
	return name
}

// gate tạo một gate và mở nó khi test kết thúc để sender bị giữ không làm treo Shutdown.
func (e *env) gate() *gate {
	g := &gate{opened: make(chan struct{})}
	e.t.Cleanup(g.release)
	return g
}

func (e *env) system(queue string, sender Sender, mutate func(*Options)) *JobSystem {
	e.t.Helper()
	opts := Options{RedisURL: redisURL(), Queue: queue, Sender: sender}
	if mutate != nil {
		mutate(&opts)
	}
	system, err := NewJobSystem(opts)
	if err != nil {
		e.t.Fatalf("NewJobSystem lỗi: %v", err)
	}
	e.t.Cleanup(func() {
		if err := system.Shutdown(); err != nil {
			e.t.Errorf("Shutdown trong cleanup lỗi: %v", err)
		}
	})
	return system
}

func (e *env) enqueue(system *JobSystem, to, subject string, opts ...asynq.Option) string {
	e.t.Helper()
	id, err := system.EnqueueEmail(context.Background(), to, subject, opts...)
	if err != nil {
		e.t.Fatalf("EnqueueEmail lỗi: %v", err)
	}
	return id
}

func (e *env) info(system *JobSystem, id string) *asynq.TaskInfo {
	e.t.Helper()
	info, err := system.Inspector().GetTaskInfo(system.Queue(), id)
	if err != nil {
		e.t.Fatalf("GetTaskInfo %s lỗi: %v", id, err)
	}
	return info
}

// timeline gom thời điểm mỗi lần sender chạy, an toàn khi nhiều worker ghi đồng thời.
type timeline struct {
	mu    sync.Mutex
	times []time.Time
}

func (l *timeline) add() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.times = append(l.times, time.Now())
}

func (l *timeline) snapshot() []time.Time {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Clone(l.times)
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

func fetchMetrics(t *testing.T, system *JobSystem) string {
	t.Helper()
	client := &http.Client{Timeout: 3 * time.Second, Transport: &http.Transport{DisableKeepAlives: true}}
	resp, err := client.Get(system.MetricsURL())
	if err != nil {
		t.Fatalf("GET /metrics lỗi: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /metrics trả %d", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("đọc body /metrics lỗi: %v", err)
	}
	return string(body)
}

func metricValue(text, status string) (string, bool) {
	re := regexp.MustCompile(`(?m)^jobs_processed_total\{status="` + status + `"\} (\d+)$`)
	m := re.FindStringSubmatch(text)
	if m == nil {
		return "", false
	}
	return m[1], true
}

func metricsAre(t *testing.T, system *JobSystem, completed, failed string) (string, bool) {
	body := fetchMetrics(t, system)
	c, okC := metricValue(body, "completed")
	f, okF := metricValue(body, "failed")
	return body, okC && okF && c == completed && f == failed
}

func TestFailedJobIsRetriedWithExponentialBackoffUpToAttempts(t *testing.T) {
	e := newEnv(t)
	queue := e.queueName()
	// asynq lên lịch retry theo GIÂY nguyên (xem README), nên base là 1 giây.
	const base = time.Second
	calls := &timeline{}
	system := e.system(queue, func(_ context.Context, _ Email, attempt Attempt) error {
		calls.add()
		if attempt.Number <= 2 {
			return fmt.Errorf("smtp down (lần %d)", attempt.Number)
		}
		return nil
	}, func(o *Options) { o.Attempts = 3; o.RetryBase = base })

	id := e.enqueue(system, "an@example.com", "Chào mừng")
	testkit.Eventually(t, 9*time.Second, func() (struct{}, bool) {
		info, err := system.Inspector().GetTaskInfo(queue, id)
		return struct{}{}, err == nil && info.State == asynq.TaskStateCompleted
	})

	times := calls.snapshot()
	if len(times) != 3 {
		t.Fatalf("sender chạy %d lần, muốn 3", len(times))
	}
	// RetryDelayFunc của lab trả base * 2^n với n là số lần đã retry (0 ở lần fail đầu): 1 s rồi 2 s.
	// asynq ghi thời điểm retry bằng Unix SECONDS (cắt bỏ phần lẻ), nên cận dưới đúng là
	// floor_to_second(thời điểm lần trước + delay), không phải lần trước + delay.
	for i := 0; i < 2; i++ {
		delay := base << i
		due := times[i].Add(delay).Truncate(time.Second)
		if times[i+1].Before(due) {
			t.Errorf("lần %d chạy lúc %s, sớm hơn mốc tối thiểu %s (delay %s)",
				i+2, times[i+1].Format(time.StampMilli), due.Format(time.StampMilli), delay)
		}
	}
	info := e.info(system, id)
	if info.Retried != 2 {
		t.Errorf("Retried = %d, muốn 2 (hai lần retry sau hai lần fail)", info.Retried)
	}
	// Metric đếm từng lần thử: 2 lần fail và 1 lần completed.
	body, ok := metricsAre(t, system, "1", "2")
	if !ok {
		t.Errorf("metrics không phải completed=1 failed=2:\n%s", body)
	}
}

func TestJobExceedingAttemptsIsKeptInFailedSetOrArchive(t *testing.T) {
	e := newEnv(t)
	queue := e.queueName()
	var mu sync.Mutex
	calls := 0
	system := e.system(queue, func(context.Context, Email, Attempt) error {
		mu.Lock()
		defer mu.Unlock()
		calls++
		return errors.New("mailbox full")
	}, func(o *Options) { o.Attempts = 3; o.RetryBase = time.Second })

	id := e.enqueue(system, "an@example.com", "Hóa đơn")
	archived := testkit.Eventually(t, 12*time.Second, func() ([]*asynq.TaskInfo, bool) {
		tasks, err := system.Inspector().ListArchivedTasks(queue)
		return tasks, err == nil && len(tasks) > 0
	})

	if len(archived) != 1 || archived[0].ID != id {
		t.Fatalf("archive = %+v, muốn đúng task %s", archived, id)
	}
	task := archived[0]
	if task.LastErr != "mailbox full" {
		t.Errorf("LastErr = %q, muốn %q", task.LastErr, "mailbox full")
	}
	// asynq đếm số lần RETRY: attempts = 3 nghĩa là MaxRetry = 2, tức 3 lần chạy.
	if task.Retried != 2 || task.MaxRetry != 2 {
		t.Errorf("Retried = %d, MaxRetry = %d, muốn cả hai bằng 2", task.Retried, task.MaxRetry)
	}
	if task.State != asynq.TaskStateArchived {
		t.Errorf("State = %v, muốn archived", task.State)
	}
	mu.Lock()
	defer mu.Unlock()
	if calls != 3 {
		t.Errorf("sender chạy %d lần, muốn 3", calls)
	}
}

func TestScheduledJobRunsNotBeforeItsDelay(t *testing.T) {
	e := newEnv(t)
	queue := e.queueName()
	const delay = 2 * time.Second
	calls := &timeline{}
	system := e.system(queue, func(context.Context, Email, Attempt) error {
		calls.add()
		return nil
	}, nil)

	before := time.Now()
	id := e.enqueue(system, "an@example.com", "Nhắc lịch", asynq.ProcessIn(delay))
	afterEnqueue := time.Now()

	// Chưa tới hạn thì task ở trạng thái scheduled và sender chưa được gọi.
	info := e.info(system, id)
	if info.State != asynq.TaskStateScheduled {
		t.Fatalf("State = %v, muốn scheduled", info.State)
	}
	if got := calls.snapshot(); len(got) != 0 {
		t.Fatalf("sender đã chạy trước hạn: %v", got)
	}
	// asynq lưu thời điểm chạy theo Unix giây (cắt phần lẻ): NextProcessAt nằm trong (enqueue + delay - 1 s, enqueue + delay].
	if info.NextProcessAt.Before(before.Add(delay - time.Second)) {
		t.Errorf("NextProcessAt %s sớm hơn %s", info.NextProcessAt, before.Add(delay-time.Second))
	}
	if info.NextProcessAt.After(afterEnqueue.Add(delay)) {
		t.Errorf("NextProcessAt %s muộn hơn %s", info.NextProcessAt, afterEnqueue.Add(delay))
	}

	testkit.Eventually(t, 9*time.Second, func() (struct{}, bool) {
		return struct{}{}, len(calls.snapshot()) > 0
	})
	ranAt := calls.snapshot()[0]
	if ranAt.Before(info.NextProcessAt) {
		t.Errorf("sender chạy lúc %s, trước mốc %s", ranAt, info.NextProcessAt)
	}
	if ranAt.Sub(before) < delay-time.Second {
		t.Errorf("sender chạy sau %s, ít hơn cận dưới delay - 1 s", ranAt.Sub(before))
	}
	t.Logf("delay %s, thực tế chạy sau %s kể từ lúc enqueue", delay, ranAt.Sub(before))
}

func TestGracefulShutdownWaitsForInFlightJobToFinish(t *testing.T) {
	e := newEnv(t)
	queue := e.queueName()
	g := e.gate()
	var mu sync.Mutex
	var events []string
	record := func(event string) {
		mu.Lock()
		defer mu.Unlock()
		events = append(events, event)
	}
	snapshot := func() []string {
		mu.Lock()
		defer mu.Unlock()
		return slices.Clone(events)
	}
	system := e.system(queue, func(context.Context, Email, Attempt) error {
		record("sender-start")
		g.wait()
		record("sender-end")
		return nil
	}, func(o *Options) { o.ShutdownTimeout = 10 * time.Second })

	id := e.enqueue(system, "an@example.com", "Đang gửi")
	testkit.Eventually(t, 9*time.Second, func() (struct{}, bool) {
		return struct{}{}, slices.Contains(snapshot(), "sender-start")
	})

	done := make(chan error, 1)
	go func() {
		err := system.Shutdown()
		record("shutdown-resolved")
		done <- err
	}()
	// Một round trip tới Redis cho Shutdown (nếu sai) cơ hội resolve sớm.
	if info := e.info(system, id); info.State != asynq.TaskStateActive {
		t.Fatalf("State = %v, muốn active", info.State)
	}
	if got := snapshot(); !slices.Equal(got, []string{"sender-start"}) {
		t.Fatalf("events = %v, Shutdown không được resolve khi job còn chạy", got)
	}
	// Metrics endpoint vẫn truy cập được trong lúc đang shutdown.
	if _, ok := metricsAre(t, system, "0", "0"); !ok {
		t.Errorf("metrics trong lúc shutdown không phải completed=0 failed=0")
	}

	g.release()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Shutdown lỗi: %v", err)
		}
	case <-time.After(9 * time.Second):
		t.Fatal("Shutdown không resolve sau khi gate được mở")
	}

	if got := snapshot(); !slices.Equal(got, []string{"sender-start", "sender-end", "shutdown-resolved"}) {
		t.Errorf("events = %v", got)
	}
	// Task completed (lab đặt Retention nên task completed còn đọc được): không retry, không fail, không mất.
	reader := e.system(queue, func(context.Context, Email, Attempt) error { return nil }, nil)
	info := e.info(reader, id)
	if info.State != asynq.TaskStateCompleted || info.Retried != 0 {
		t.Errorf("State = %v, Retried = %d, muốn completed và 0", info.State, info.Retried)
	}
	if archived, err := reader.Inspector().ListArchivedTasks(queue); err != nil || len(archived) != 0 {
		t.Errorf("archive = %v, err = %v, muốn rỗng", archived, err)
	}
	// Endpoint đã đóng sau Shutdown.
	if reachable(system.MetricsURL()) {
		t.Error("metrics endpoint vẫn mở sau Shutdown")
	}
}

func TestInFlightJobOutlastingShutdownTimeoutIsRequeued(t *testing.T) {
	e := newEnv(t)
	queue := e.queueName()
	g := e.gate()
	started := make(chan struct{}, 1)
	var running sync.WaitGroup
	// Sender cố ý không nhìn ctx: giả lập một lần gửi bị treo, không hợp tác với shutdown.
	first := e.system(queue, func(context.Context, Email, Attempt) error {
		running.Add(1)
		defer running.Done()
		started <- struct{}{}
		g.wait()
		return nil
	}, func(o *Options) { o.ShutdownTimeout = 300 * time.Millisecond })
	// Dọn goroutine sender còn treo: mở gate rồi chờ nó thoát (chạy trước mọi cleanup đã đăng ký).
	t.Cleanup(func() { g.release(); running.Wait() })

	id := e.enqueue(first, "an@example.com", "Gửi chậm")
	select {
	case <-started:
	case <-time.After(9 * time.Second):
		t.Fatal("sender không bắt đầu")
	}

	began := time.Now()
	if err := first.Shutdown(); err != nil {
		t.Fatalf("Shutdown lỗi: %v", err)
	}
	if elapsed := time.Since(began); elapsed < 250*time.Millisecond {
		t.Errorf("Shutdown resolve sau %s, sớm hơn ShutdownTimeout 300 ms", elapsed)
	}

	// Hết ShutdownTimeout thì asynq đẩy task về pending: không mất, và Retried vẫn 0 (không tính là lần fail).
	recovered := &timeline{}
	var mu sync.Mutex
	var attempts []int
	second := e.system(queue, func(_ context.Context, _ Email, attempt Attempt) error {
		mu.Lock()
		attempts = append(attempts, attempt.Number)
		mu.Unlock()
		recovered.add()
		return nil
	}, nil)
	// Worker thứ hai nhận lại task và hoàn tất nó.
	testkit.Eventually(t, 9*time.Second, func() (struct{}, bool) {
		info, err := second.Inspector().GetTaskInfo(queue, id)
		return struct{}{}, err == nil && info.State == asynq.TaskStateCompleted
	})
	info := e.info(second, id)
	if info.Retried != 0 {
		t.Errorf("Retried = %d, muốn 0: requeue không phải một lần fail", info.Retried)
	}
	mu.Lock()
	if len(recovered.snapshot()) != 1 || !slices.Equal(attempts, []int{1}) {
		t.Errorf("worker thứ hai chạy %d lần với attempts %v, muốn đúng một lần ở lần chạy 1", len(recovered.snapshot()), attempts)
	}
	mu.Unlock()

	// Sender cũ cuối cùng cũng xong: kết quả của nó không làm đổi trạng thái task.
	g.release()
	running.Wait()
	if after := e.info(second, id); after.State != asynq.TaskStateCompleted || after.Retried != 0 {
		t.Errorf("sau khi sender cũ xong: State = %v, Retried = %d, muốn completed và 0", after.State, after.Retried)
	}
}

func TestMetricsEndpointReportsJobsProcessedTotal(t *testing.T) {
	e := newEnv(t)
	queue := e.queueName()
	system := e.system(queue, func(_ context.Context, email Email, _ Attempt) error {
		if strings.HasPrefix(email.To, "bad") {
			return errors.New("địa chỉ không tồn tại")
		}
		return nil
	}, nil)

	for _, to := range []string{"a@example.com", "b@example.com", "c@example.com"} {
		e.enqueue(system, to, "ok")
	}
	// Attempts = 1: mỗi task lỗi chỉ chạy một lần, nên số lần fail bằng số task fail.
	for _, to := range []string{"bad-1@example.com", "bad-2@example.com"} {
		e.enqueue(system, to, "lỗi", WithAttempts(1))
	}

	body := testkit.Eventually(t, 9*time.Second, func() (string, bool) {
		return metricsAre(t, system, "3", "2")
	})
	for _, want := range []string{
		"# HELP jobs_processed_total",
		"# TYPE jobs_processed_total counter",
		`jobs_processed_total{status="completed"} 3`,
		`jobs_processed_total{status="failed"} 2`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("metrics thiếu %q:\n%s", want, body)
		}
	}
}

func TestTwoWorkersDoNotProcessTheSameJobTwice(t *testing.T) {
	e := newEnv(t)
	queue := e.queueName()
	var mu sync.Mutex
	seen := map[string]int{}
	sender := func(_ context.Context, _ Email, attempt Attempt) error {
		mu.Lock()
		defer mu.Unlock()
		seen[attempt.TaskID]++
		return nil
	}
	one := e.system(queue, sender, func(o *Options) { o.Concurrency = 3 })
	e.system(queue, sender, func(o *Options) { o.Concurrency = 3 })

	const total = 30
	ids := map[string]bool{}
	for i := 0; i < total; i++ {
		ids[e.enqueue(one, fmt.Sprintf("u%d@example.com", i), "bulk")] = true
	}
	testkit.Eventually(t, 15*time.Second, func() (struct{}, bool) {
		mu.Lock()
		defer mu.Unlock()
		return struct{}{}, len(seen) >= total
	})

	mu.Lock()
	defer mu.Unlock()
	if len(ids) != total || len(seen) != total {
		t.Fatalf("ids = %d, seen = %d, muốn cùng bằng %d", len(ids), len(seen), total)
	}
	for id, n := range seen {
		if !ids[id] || n != 1 {
			t.Errorf("task %s được xử lý %d lần, muốn đúng 1", id, n)
		}
	}
}

func TestJobWithSingleAttemptFailsStraightToArchive(t *testing.T) {
	e := newEnv(t)
	queue := e.queueName()
	var mu sync.Mutex
	calls := 0
	system := e.system(queue, func(context.Context, Email, Attempt) error {
		mu.Lock()
		defer mu.Unlock()
		calls++
		return errors.New("từ chối ngay")
	}, nil)

	id := e.enqueue(system, "an@example.com", "một lần", WithAttempts(1))
	archived := testkit.Eventually(t, 9*time.Second, func() ([]*asynq.TaskInfo, bool) {
		tasks, err := system.Inspector().ListArchivedTasks(queue)
		return tasks, err == nil && len(tasks) > 0
	})
	if archived[0].ID != id || archived[0].Retried != 0 || archived[0].MaxRetry != 0 {
		t.Errorf("archive = %+v, muốn task %s với Retried = 0 và MaxRetry = 0", archived[0], id)
	}
	if archived[0].LastErr != "từ chối ngay" {
		t.Errorf("LastErr = %q", archived[0].LastErr)
	}
	mu.Lock()
	defer mu.Unlock()
	if calls != 1 {
		t.Errorf("sender chạy %d lần, muốn 1", calls)
	}
}

// Go không có "throw giá trị bất kỳ", tương đương gần nhất là panic với giá trị không phải error.
func TestSenderPanickingWithANonErrorValueCountsAsFailed(t *testing.T) {
	e := newEnv(t)
	queue := e.queueName()
	system := e.system(queue, func(context.Context, Email, Attempt) error {
		panic("không phải error")
	}, nil)

	id := e.enqueue(system, "an@example.com", "panic", WithAttempts(1))
	archived := testkit.Eventually(t, 9*time.Second, func() ([]*asynq.TaskInfo, bool) {
		tasks, err := system.Inspector().ListArchivedTasks(queue)
		return tasks, err == nil && len(tasks) > 0
	})
	if archived[0].ID != id || !strings.Contains(archived[0].LastErr, "không phải error") {
		t.Errorf("archive = %+v, muốn task %s với LastErr chứa nội dung panic", archived[0], id)
	}
	if body, ok := metricsAre(t, system, "0", "1"); !ok {
		t.Errorf("metrics không phải completed=0 failed=1:\n%s", body)
	}
}

func TestShutdownWithNoJobInFlightResolvesImmediatelyAndClosesMetricsEndpoint(t *testing.T) {
	e := newEnv(t)
	queue := e.queueName()
	goroutinesBefore := runtime.NumGoroutine()
	system := e.system(queue, func(context.Context, Email, Attempt) error { return nil },
		func(o *Options) { o.ShutdownTimeout = 30 * time.Second })
	if _, ok := metricsAre(t, system, "0", "0"); !ok {
		t.Fatal("metrics ban đầu không phải completed=0 failed=0")
	}

	began := time.Now()
	if err := system.Shutdown(); err != nil {
		t.Fatalf("Shutdown lỗi: %v", err)
	}
	if elapsed := time.Since(began); elapsed > 3*time.Second {
		t.Errorf("Shutdown mất %s dù không có job đang chạy", elapsed)
	}
	if reachable(system.MetricsURL()) {
		t.Error("metrics endpoint vẫn mở sau Shutdown")
	}
	// Gọi lần hai vẫn an toàn.
	if err := system.Shutdown(); err != nil {
		t.Errorf("Shutdown lần hai lỗi: %v", err)
	}
	// Không rò goroutine: số goroutine quay về mức trước khi tạo hệ thống.
	testkit.Eventually(t, 5*time.Second, func() (struct{}, bool) {
		return struct{}{}, runtime.NumGoroutine() <= goroutinesBefore
	})
}
