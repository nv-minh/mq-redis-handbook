# Lab 03 - Retry với DLX, TTL và dead letter queue

## Mục tiêu

Dựng retry có delay bằng dead letter exchange (DLX) và message TTL, không cần plugin, và chứng minh bằng test:

- Message luôn thất bại được xử lý `maxRetries + 1` lần (khi không có crash) rồi rơi vào dead letter queue (DLQ), đúng một bản.
- Message trong DLQ giữ nguyên header `x-death` (kèm `count`, `reason`) và có thêm lý do lỗi cuối cùng, nên người vận hành đọc được lịch sử.
- Mỗi lần retry phải chờ ít nhất `retryDelayMs`.
- `maxRetries = 0` nghĩa là lỗi đầu tiên đi thẳng vào DLQ.

Lab cần RabbitMQ chạy bằng `make up` (RabbitMQ 4.3.6).
Lab đọc `AMQP_URL` (mặc định `amqp://guest:guest@127.0.0.1:5672`).
Mỗi test tạo exchange và queue có tên duy nhất và xóa chúng khi kết thúc, kể cả khi test fail.
Lý thuyết nằm ở [theory.md](../theory.md).

## Kiến trúc

```mermaid
flowchart LR
    P[Publisher] -->|"key task"| XW{{"exchange work"}}
    XW --> W[("queue work<br/>quorum<br/>DLX = exchange retry")]
    W -->|"prefetch 1"| C[Worker]
    C -->|"thành công: ack"| Done((xong))
    C -->|"lỗi, còn lượt:<br/>reject requeue=false"| W
    W -.->|"dead-letter<br/>reason rejected"| XR{{"exchange retry"}}
    XR --> R[("queue retry<br/>x-message-ttl = delay<br/>không có consumer")]
    R -.->|"hết TTL: dead-letter<br/>reason expired"| XW
    C -->|"lỗi, hết lượt:<br/>publish + confirm rồi ack"| D[("queue dlq<br/>quorum")]
```

Đường lỗi của một message luôn thất bại (`maxRetries = 2`):

```mermaid
sequenceDiagram
    participant W as Worker
    participant K as Queue work
    participant R as Queue retry (TTL)
    participant D as DLQ
    K->>W: deliver (không có x-death)
    W->>K: handler lỗi, attempts=0 < 2: reject requeue=false
    K->>R: dead-letter (x-death: work/rejected count=1)
    Note over R: chờ retryDelayMs, không ai consume
    R->>K: hết TTL, dead-letter về work (x-death: retry/expired count=1)
    K->>W: deliver lần 2
    W->>K: handler lỗi, attempts=1 < 2: reject requeue=false
    K->>R: dead-letter (count=2)
    R->>K: hết TTL (count=2)
    K->>W: deliver lần 3
    Note over W: handler lỗi, attempts=2 không còn nhỏ hơn 2
    W->>D: publish bản sao + x-failure-reason + x-attempts, chờ confirm
    D-->>W: confirm
    W->>K: ack bản gốc
```

Vòng đời của một message trong vòng retry:

```mermaid
stateDiagram-v2
    [*] --> InWork: publish
    InWork --> Processing: deliver (prefetch 1)
    Processing --> Done: handler thành công, ack
    Processing --> Waiting: lỗi và attempts nhỏ hơn maxRetries, reject requeue=false
    Waiting --> InWork: hết TTL của queue retry
    Processing --> InDlq: lỗi và attempts bằng maxRetries, publish DLQ rồi ack
    Done --> [*]
    InDlq --> [*]
```

## Giao diện

TypeScript (`ts/lab.ts`, amqplib 2.2.0):

```ts
setupRetryTopology(ch, name, { maxRetries, retryDelayMs }): Promise<{ work, retry, dlq, maxRetries }>;
publishWork(ch: ConfirmChannel, queues, body: string): Promise<void>;
startWorker(ch: ConfirmChannel, queues, handler: (msg: Buffer) => Promise<void>, { consumerTag? }): Promise<string>;
attemptsOf(headers, workQueue): number; // tổng count của x-death có queue = work và reason = rejected
```

Go (`go/lab.go`, amqp091-go v1.15.0): `SetupRetryTopology(ch, name, Options{MaxRetries, RetryDelay})` trả `Queues{Work, Retry, Dlq, MaxRetries}`, `PublishWork(ctx, ch, queues, body)`, `StartWorker(ch, queues, handler func([]byte) error, consumerTag)` trả kênh `done`, và `Attempts(headers, workQueue)`.

Interface trong task chỉ nêu `{ work, retry, dlq }` là tên queue, lab thêm trường `maxRetries` vào kết quả trả về để `startWorker(ch, queues, handler)` giữ đúng ba tham số mà vẫn biết khi nào bỏ cuộc.
Mỗi queue có một direct exchange cùng tên (`<name>.work`, `<name>.retry`, `<name>.dlq`) nên `queues.work` vừa là tên queue vừa là tên exchange để publish.

## Các điểm quan trọng

Quy ước đếm: `maxRetries` là số lần retry sau lần xử lý đầu tiên, nên khi không có crash và publish sang DLQ không thất bại thì handler chạy `maxRetries + 1` lần.
Worker lấy số lần đã thất bại từ `x-death`: tổng `count` của phần tử có `queue` là work queue và `reason` là `rejected`.
`count` đã được gom theo cặp {queue, reason}, nên không cần cộng nhiều phần tử, nhưng hàm vẫn cộng để an toàn.
Phần tử của queue retry (`reason = expired`) đếm cùng các lần đó, nên không dùng để tránh đếm đôi.

Khi hết lượt, worker không để broker dead-letter message sang DLQ (dead-letter của work queue luôn trỏ về exchange retry).
Thay vào đó worker tự publish một bản sao sang DLQ và chỉ ack bản gốc sau khi broker confirm:

- Chặng cuối này không làm mất message: nếu publish sang DLQ không được confirm thì worker `reject` với `requeue=true`.
  Các chặng work sang retry và retry sang work do broker dead-letter, mặc định at-most-once theo tài liệu, nên bảo đảm "không mất" chỉ áp dụng cho chặng cuối.
- Nếu worker chết giữa lúc confirm và ack, message có thể xuất hiện hai lần ở DLQ (at-least-once), không bao giờ không có bản nào.
- Bản sao giữ nguyên mọi header, kể cả `x-death`, nên DLQ vẫn có lịch sử retry.
  Worker thêm `x-failure-reason` (lỗi cuối cùng của handler) và `x-attempts` (tổng số lần đã xử lý).
- Message ở DLQ không có entry cho lần thất bại cuối, vì lần đó broker không dead-letter nó.
  `x-attempts` bù lại chỗ thiếu này.
- Khi `maxRetries = 0` message chưa từng đi qua queue retry nên không có `x-death`, chỉ có hai header của worker.

Worker dùng `reject(requeue=false)`, còn `nack(requeue=false)` dead-letter và tăng `x-death` y hệt (test `nack_without_requeue_dead_letters_and_raises_x_death_count` khóa điều này).
Lab chọn `reject` vì nó không có cờ `multiple`.
Sự khác biệt giữa `nack` và `reject` của RabbitMQ 4.3 chỉ nằm ở `requeue=true`: `reject` tính vào delivery limit còn `nack` thì không (lab 01 đo và khóa điều này).
Handler ném giá trị bất kỳ, kể cả `undefined`, đều được coi là thất bại (test `handler_throwing_undefined_still_goes_through_the_retry_path`, chỉ có ở TypeScript vì handler Go báo lỗi bằng giá trị trả về).
Vòng retry của lab dựa vào `x-death`, nên delivery limit mặc định 20 của quorum queue không liên quan, vì `reject(requeue=false)` không phải requeue.

Hình dạng header `x-death` (đo bằng test trên RabbitMQ 4.3.6):

| Trường                        | amqplib 2.2.0                                       | amqp091-go v1.15.0                            |
| ----------------------------- | --------------------------------------------------- | --------------------------------------------- |
| `x-death`                     | array các object, lần dead-letter gần nhất đứng đầu | `[]interface{}` các `amqp.Table`, cùng thứ tự |
| `count`                       | `number`                                            | `int64`                                       |
| `time`                        | `{ '!': 'timestamp', value: <số giây> }`            | `time.Time`                                   |
| `routing-keys`                | array string                                        | `[]interface{}` các string                    |
| `queue`, `reason`, `exchange` | string                                              | string                                        |

File kiểu của amqplib 2.2.0 có sẵn kiểu `XDeath` cho `headers['x-death']`, nhưng union `reason` của nó chưa liệt kê `delivery_limit` mà tài liệu RabbitMQ có ghi.

Với một message luôn thất bại và `maxRetries = 2`, DLQ nhận `x-death` gồm hai phần tử: `{queue: retry, reason: expired, count: 2}` đứng đầu và `{queue: work, reason: rejected, count: 2}` đứng sau.
Header `x-first-death-queue` và `x-first-death-reason` là work và `rejected`, và không bao giờ đổi.

Về delay:

- Queue retry có `x-message-ttl` cố định và `x-dead-letter-exchange` trỏ về exchange work, không có consumer.
- Một queue retry cho mỗi mức delay.
  Message hết hạn ở đầu queue, nên trộn TTL khác nhau trong cùng một queue làm message TTL ngắn bị chặn sau message TTL dài.
- Plugin delayed-message-exchange không dùng ở đây vì đã bị ngừng maintain và repo đã archive.
- Test `retry_respects_retry_delay` đo từ lúc handler thất bại (ngay trước reject) tới lúc handler được gọi lại, cùng đồng hồ trong một process, và chỉ khẳng định cận dưới `retryDelayMs - 20 ms`.
- Test không khẳng định cận trên, vì tải máy làm thời gian dài ra.
  Khi chạy demo với TTL 500 ms, lần retry đo được khoảng 500 đến 600 ms (vài lần đo, không phải bảo đảm).

Queue là quorum (`x-queue-type: quorum` tường minh), durable, không exclusive, không auto-delete, và được xóa thủ công khi dọn dẹp.
Theo tài liệu, dead-letter của quorum queue mặc định là at-most-once (lab không thử): nếu exchange đích không route được thì message bị mất.
Lab không bật `dead-letter-strategy: at-least-once` để giữ topology đơn giản, và theory giải thích lựa chọn này.

## Chạy

```bash
make up
pnpm vitest run 05-rabbitmq/lab-03-retry-dlx/ts
go test -race ./05-rabbitmq/lab-03-retry-dlx/go/...
make lab-ts LAB=05-rabbitmq/lab-03-retry-dlx
make lab-go LAB=05-rabbitmq/lab-03-retry-dlx
```

## Kết quả mong đợi

Test (cùng tên ở TS và Go, Go dùng CamelCase):

| Test                                                             | Chứng minh                                                                                                                 |
| ---------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------- |
| `failing_message_is_retried_max_retries_times_then_lands_in_dlq` | Handler luôn lỗi, `maxRetries = 3`: handler được gọi 4 lần, DLQ có đúng 1 message, work và retry rỗng                      |
| `dlq_message_carries_x_death_count_and_reason`                   | `x-death` có hai entry (work/`rejected`/2 và retry/`expired`/2), đúng kiểu dữ liệu từng client, kèm `x-failure-reason`     |
| `retry_respects_retry_delay`                                     | Khoảng cách giữa hai lần gọi handler ít nhất `retryDelayMs - 20 ms` (delay 300 ms)                                         |
| `nack_without_requeue_dead_letters_and_raises_x_death_count`     | `nack(requeue=false)` dead-letter như `reject`: message đi qua queue retry và `x-death` của queue work là 1 ở lần giao lại |
| `handler_throwing_undefined_still_goes_through_the_retry_path`   | Chỉ TypeScript: handler ném `undefined` vẫn đi qua vòng retry và rơi vào DLQ với `x-attempts = 2`                          |
| `max_retries_zero_sends_first_failure_straight_to_dlq`           | `maxRetries = 0`: handler được gọi 1 lần, DLQ có 1 message không có `x-death`, queue retry rỗng                            |

Không test nào dùng sleep cố định: test chờ bằng `eventually` trên số message của DLQ.
Demo cho ba message đi qua một worker (`ok` thành công, `flaky` lỗi hai lần rồi thành công, `poison` luôn lỗi) và in mốc thời gian từng lần xử lý, rồi in `x-death` của message trong DLQ.

## Bài tập mở rộng

1. Thêm một queue retry thứ hai với delay dài hơn và để worker chọn queue theo số lần đã thất bại (backoff tăng dần).
2. Đặt `x-dead-letter-strategy: at-least-once` kèm `x-overflow: reject-publish` cho work queue và quan sát khác biệt khi exchange retry bị xóa.
3. Viết một tiến trình đọc DLQ, sửa nguyên nhân lỗi rồi publish lại message vào exchange work (redrive).
4. Thử `x-delayed-retry-type` của quorum queue 4.3 để thay queue retry và so sánh số queue cần quản lý.
