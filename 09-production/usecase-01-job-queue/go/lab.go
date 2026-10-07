// Package lab là use case job queue gửi email với asynq: retry với exponential backoff, task hết
// attempts nằm trong archive, task hoãn, graceful shutdown và endpoint Prometheus /metrics.
package lab

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/hibiken/asynq"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
)

// taskType là loại task duy nhất của lab.
const taskType = "email:send"

// retention giữ task thành công 1 giờ để còn kiểm tra được trạng thái completed.
// Mặc định asynq xóa task ngay khi handler trả nil (khác BullMQ giữ vô thời hạn).
const retention = time.Hour

// Email là nội dung một email cần gửi. Lab không gửi email thật: việc gửi nằm trong Sender.
type Email struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
}

// Attempt mô tả lần chạy hiện tại của task.
type Attempt struct {
	// TaskID là id của task trong queue.
	TaskID string
	// Number là lần chạy thứ mấy (bắt đầu từ 1) tính theo số lần RETRY của asynq cộng 1.
	// Task bị đẩy lại queue khi shutdown hết hạn không được tính là một lần retry nên giữ nguyên số.
	Number int
}

// Sender là hàm gửi email có thể thay thế: test cho nó thành công, lỗi N lần hoặc bị giữ lại bởi một gate.
// Trả error nghĩa là lần chạy này thất bại. Panic cũng được tính là thất bại.
type Sender func(ctx context.Context, email Email, attempt Attempt) error

// Options cấu hình JobSystem.
type Options struct {
	// RedisURL là địa chỉ Redis (REDIS_URL).
	RedisURL string
	// Queue là tên queue. Mọi key của asynq nằm dưới asynq:{<Queue>}.
	Queue  string
	Sender Sender
	// Concurrency là số task một server chạy song song. Mặc định 1.
	Concurrency int
	// Attempts là tổng số lần chạy tối đa của một task (lần đầu cộng các lần retry). Mặc định 3.
	// asynq đếm số lần retry, nên lab đặt MaxRetry = Attempts - 1 để cùng nghĩa với BullMQ.
	Attempts int
	// RetryBase là base của exponential backoff: chờ RetryBase * 2^n sau lần fail thứ n+1 (n đếm từ 0).
	// asynq lên lịch theo Unix giây nên độ phân giải thực tế là 1 giây (xem README). Mặc định 1 giây.
	RetryBase time.Duration
	// PollInterval là chu kỳ server kiểm tra task mới và task scheduled/retry đã tới hạn.
	// Mặc định của asynq là 1 giây và 5 giây; lab đặt 100 ms để retry không bị trễ vì forwarder. Mặc định 100 ms.
	PollInterval time.Duration
	// ShutdownTimeout là thời gian tối đa Shutdown chờ task đang chạy. asynq cố định giá trị này khi tạo server,
	// nên nó là option chứ không phải tham số của Shutdown. Hết hạn thì task chưa xong được đẩy lại queue.
	// Mặc định 8 giây (mặc định của asynq).
	ShutdownTimeout time.Duration
}

// WithAttempts đặt số lần chạy tối đa của riêng một task (MaxRetry = n - 1).
func WithAttempts(n int) asynq.Option { return asynq.MaxRetry(n - 1) }

// JobSystem nối client, server, inspector, metrics và shutdown thành một hệ thống job nhỏ.
//
// Metric jobs_processed_total{status} đếm kết quả của TỪNG LẦN chạy sender: completed là số lần sender
// thành công, failed là số lần sender lỗi hoặc panic (kể cả lần lỗi sẽ được retry). Task lỗi hai lần rồi
// thành công cho failed = 2 và completed = 1. Đếm ở mức sender chứ không ở mức broker, nên task bị đẩy lại
// queue khi shutdown hết hạn sẽ chạy lại và được đếm lại.
type JobSystem struct {
	opts      Options
	client    *asynq.Client
	server    *asynq.Server
	inspector *asynq.Inspector
	completed prometheus.Counter
	failed    prometheus.Counter
	http      *http.Server
	httpDone  chan struct{}
	metrics   string

	once sync.Once
	err  error
}

// NewJobSystem khởi động server asynq và metrics endpoint (cổng do hệ điều hành chọn).
func NewJobSystem(opts Options) (*JobSystem, error) {
	if opts.Sender == nil || opts.Queue == "" {
		return nil, errors.New("Sender và Queue là bắt buộc")
	}
	opts = withDefaults(opts)
	redisOpt, err := asynq.ParseRedisURI(opts.RedisURL)
	if err != nil {
		return nil, err
	}

	registry := prometheus.NewRegistry()
	processed := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "jobs_processed_total",
		Help: "Số lần chạy sender, theo kết quả (mỗi lần thử được đếm một lần).",
	}, []string{"status"})
	registry.MustRegister(processed)
	s := &JobSystem{
		opts:      opts,
		client:    asynq.NewClient(redisOpt),
		inspector: asynq.NewInspector(redisOpt),
		// WithLabelValues tạo sẵn cả hai nhãn để endpoint luôn in ra completed và failed, kể cả khi còn bằng 0.
		completed: processed.WithLabelValues("completed"),
		failed:    processed.WithLabelValues("failed"),
		httpDone:  make(chan struct{}),
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		_ = s.client.Close()
		_ = s.inspector.Close()
		return nil, err
	}
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
	s.http = &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	s.metrics = "http://" + listener.Addr().String() + "/metrics"
	go func() {
		defer close(s.httpDone)
		if err := s.http.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			fmt.Printf("[job-queue] metrics server lỗi: %v\n", err)
		}
	}()

	s.server = asynq.NewServer(redisOpt, asynq.Config{
		Concurrency: opts.Concurrency,
		Queues:      map[string]int{opts.Queue: 1},
		// n là số lần đã retry (0 ở lần fail đầu): chờ base, 2*base, 4*base...
		// Trùng công thức của BullMQ exponential: base * 2^(attemptsMade - 1).
		RetryDelayFunc:           func(n int, _ error, _ *asynq.Task) time.Duration { return opts.RetryBase << n },
		TaskCheckInterval:        opts.PollInterval,
		DelayedTaskCheckInterval: opts.PollInterval,
		ShutdownTimeout:          opts.ShutdownTimeout,
		LogLevel:                 asynq.ErrorLevel,
	})
	if err := s.server.Start(asynq.HandlerFunc(s.handle)); err != nil {
		_ = s.Shutdown()
		return nil, err
	}
	return s, nil
}

func withDefaults(o Options) Options {
	if o.Concurrency <= 0 {
		o.Concurrency = 1
	}
	if o.Attempts <= 0 {
		o.Attempts = 3
	}
	if o.RetryBase <= 0 {
		o.RetryBase = time.Second
	}
	if o.PollInterval <= 0 {
		o.PollInterval = 100 * time.Millisecond
	}
	if o.ShutdownTimeout <= 0 {
		o.ShutdownTimeout = 8 * time.Second
	}
	return o
}

// handle chạy sender cho một task và đếm kết quả của lần chạy này.
func (s *JobSystem) handle(ctx context.Context, task *asynq.Task) (err error) {
	var email Email
	if err := json.Unmarshal(task.Payload(), &email); err != nil {
		s.failed.Inc()
		// Payload hỏng thì retry cũng vô ích: đưa thẳng vào archive.
		return fmt.Errorf("payload không hợp lệ: %w: %w", err, asynq.SkipRetry)
	}
	retried, _ := asynq.GetRetryCount(ctx)
	taskID, _ := asynq.GetTaskID(ctx)
	defer func() {
		// Sender có thể panic với giá trị bất kỳ (không nhất thiết là error): vẫn tính là một lần thất bại.
		if r := recover(); r != nil {
			s.failed.Inc()
			err = fmt.Errorf("sender panic: %v", r)
		}
	}()
	if err := s.opts.Sender(ctx, email, Attempt{TaskID: taskID, Number: retried + 1}); err != nil {
		s.failed.Inc()
		return err
	}
	s.completed.Inc()
	return nil
}

// EnqueueEmail đưa một email vào queue và trả về id của task. opts ghi đè mặc định của lab
// (ví dụ asynq.ProcessIn để hoãn, WithAttempts để đổi số lần chạy).
func (s *JobSystem) EnqueueEmail(ctx context.Context, to, subject string, opts ...asynq.Option) (string, error) {
	payload, err := json.Marshal(Email{To: to, Subject: subject})
	if err != nil {
		return "", err
	}
	defaults := []asynq.Option{
		asynq.Queue(s.opts.Queue),
		asynq.MaxRetry(s.opts.Attempts - 1),
		asynq.Retention(retention),
	}
	info, err := s.client.EnqueueContext(ctx, asynq.NewTask(taskType, payload), append(defaults, opts...)...)
	if err != nil {
		return "", err
	}
	return info.ID, nil
}

// Inspector trả về asynq.Inspector để kiểm tra trạng thái task (GetTaskInfo, ListArchivedTasks, ...).
func (s *JobSystem) Inspector() *asynq.Inspector { return s.inspector }

// Queue trả về tên queue.
func (s *JobSystem) Queue() string { return s.opts.Queue }

// MetricsURL trả về URL của endpoint GET /metrics.
func (s *JobSystem) MetricsURL() string { return s.metrics }

// Shutdown tắt êm: server asynq ngừng nhận task mới và chờ task đang chạy tối đa ShutdownTimeout.
// Hết hạn mà task chưa xong thì asynq đẩy task đó về pending (không tính là lần retry) để server khác chạy lại.
// Sau đó đóng metrics endpoint, client và inspector. Không có task nào đang chạy thì trả về ngay.
// Gọi nhiều lần an toàn: lần sau trả lại kết quả của lần đầu.
func (s *JobSystem) Shutdown() error {
	s.once.Do(func() {
		var errs []error
		if s.server != nil {
			s.server.Shutdown()
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		errs = append(errs, s.http.Shutdown(ctx))
		<-s.httpDone
		errs = append(errs, s.client.Close(), s.inspector.Close())
		s.err = errors.Join(errs...)
	})
	return s.err
}

// DestroyQueue xóa mọi key của một queue (Inspector.DeleteQueue với force), dùng khi dọn test và demo.
// Gọi sau khi mọi server của queue đã đóng (asynq từ chối xóa queue còn task active).
// Các key còn sót dưới asynq:{<queue>} (ví dụ bộ đếm thống kê theo ngày) cũng bị xóa, theo pattern đã escape
// ký tự glob nên tên queue không thể làm hàm này đụng tới key của queue khác.
func DestroyQueue(redisURL, queue string) error {
	if queue == "" {
		return errors.New("tên queue không được để trống")
	}
	redisOpt, err := asynq.ParseRedisURI(redisURL)
	if err != nil {
		return err
	}
	inspector := asynq.NewInspector(redisOpt)
	err = inspector.DeleteQueue(queue, true)
	if errors.Is(err, asynq.ErrQueueNotFound) {
		err = nil
	}
	err = errors.Join(err, inspector.Close())

	opts, parseErr := redis.ParseURL(redisURL)
	if parseErr != nil {
		return errors.Join(err, parseErr)
	}
	client := redis.NewClient(opts)
	ctx := context.Background()
	iter := client.Scan(ctx, 0, keyPattern(queue), 200).Iterator()
	for iter.Next(ctx) {
		err = errors.Join(err, client.Del(ctx, iter.Val()).Err())
	}
	return errors.Join(err, iter.Err(), client.Close())
}

// globEscaper thêm dấu `\` trước các ký tự đặc biệt của pattern MATCH (`\ * ? [ ]`).
var globEscaper = strings.NewReplacer(`\`, `\\`, `*`, `\*`, `?`, `\?`, `[`, `\[`, `]`, `\]`)

// keyPattern trả về pattern SCAN MATCH cho mọi key của một queue. Tên queue được escape để một tên
// như `*` chỉ khớp đúng chính nó chứ không khớp key của queue khác.
func keyPattern(queue string) string { return "asynq:{" + globEscaper.Replace(queue) + "}*" }
