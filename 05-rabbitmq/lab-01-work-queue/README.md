# Lab 01 - Work queue: prefetch, ack và redelivery

## Mục tiêu

Xây một work queue trên RabbitMQ và chứng minh bằng test ba điều:

- Với `prefetch 1` và manual ack, worker chậm chỉ giữ đúng 1 message chưa ack, phần việc còn lại chảy về worker nhanh (fair dispatch).
- Message đã giao nhưng chưa ack được broker requeue khi connection của worker đóng, và worker khác nhận lại đúng message đó với cờ `redelivered`.
- `x-delivery-count` và `x-acquired-count` hoạt động đúng như tài liệu 4.3 mô tả, và quorum queue chặn prefetch không giới hạn ở 2000.

Lab cần RabbitMQ chạy bằng `make up` (RabbitMQ 4.3.6).
Lab đọc `AMQP_URL` (mặc định `amqp://guest:guest@127.0.0.1:5672`).
Mỗi test tạo queue có tên duy nhất và xóa nó khi kết thúc, kể cả khi test fail.
Lý thuyết nằm ở [theory.md](../theory.md).

## Kiến trúc

```mermaid
flowchart LR
    P[Publisher<br/>confirm mode] -->|"default exchange<br/>routing key = tên queue"| Q[("work queue<br/>quorum")]
    Q -->|"giao tới khi unacked = prefetch"| W1["Worker nhanh<br/>prefetch 1"]
    Q -->|"giao tới khi unacked = prefetch"| W2["Worker chậm<br/>prefetch 1"]
    W1 -->|ack| Q
    W2 -.->|"chưa ack: broker chờ,<br/>không giao thêm cho worker này"| Q
```

Đường lỗi: worker A nhận message rồi connection đóng trước khi ack, broker requeue message cho worker B.

```mermaid
sequenceDiagram
    participant P as Publisher
    participant B as Broker (quorum queue)
    participant A as Worker A
    participant W as Worker B
    P->>B: publish job-1 (persistent)
    B-->>P: confirm (basic.ack)
    B->>A: deliver job-1 (redelivered=false)
    Note over A: A crash, connection đóng, chưa ack
    B->>B: requeue job-1 (unacked của connection đã đóng)
    B->>W: deliver job-1 (redelivered=true, x-delivery-count=1)
    W->>B: ack
```

## Giao diện

TypeScript (`ts/lab.ts`, amqplib 2.2.0):

```ts
openConnection(): Promise<ChannelModel>;
declareWorkQueue(channel, queue): Promise<void>; // quorum, durable
publishTasks(channel: ConfirmChannel, queue, bodies: string[]): Promise<void>; // resolve sau khi broker confirm
startWorker(channel, queue, handler: (msg) => Promise<void>, { prefetch?, consumerTag? }): Promise<string>;
```

Go (`go/lab.go`, amqp091-go v1.15.0): `URL()`, `DeclareWorkQueue(ch, queue)`, `PublishTasks(ctx, ch, queue, bodies)` và `StartWorker(ch, WorkerConfig{Queue, ConsumerTag, Prefetch, Handler})` trả về kênh `done` đóng khi goroutine của worker thoát.
Handler trả `nil` thì worker ack, trả lỗi (hoặc ném lỗi trong TypeScript) thì worker `reject` với `requeue=true`.
Nhánh lỗi này của handler không có test riêng.

Hình dạng API của amqplib 2.2.0 (đọc từ `index.d.ts` và chạy thử):

- API dựa trên Promise: `connect`, `createChannel`, `createConfirmChannel`, `assertQueue`, `consume`, `prefetch`, `waitForConfirms` đều trả Promise.
- `ack`, `nack`, `reject`, `publish` và `sendToQueue` là hàm đồng bộ trả `void` hoặc `boolean`.
- Package là CommonJS, có sẵn file kiểu `index.d.ts`, nên `import { connect } from "amqplib"` chạy được dưới ESM và `tsc` (không cần `@types/amqplib`).
- Callback của `consume` nhận `null` khi consumer bị cancel từ phía broker, nên phải kiểm tra `null` trước khi dùng message.
- `channel.get` trả `false` (không phải `null`) khi queue rỗng.
- Phải gắn listener `'error'` cho connection và channel, nếu không lỗi sẽ làm sập process.

amqp091-go v1.15.0: `PublishWithDeferredConfirmWithContext` trả `*DeferredConfirmation` và `WaitContext` chờ confirm, `Channel.Get` trả `ok=false` trên queue rỗng, `QueueDeclarePassive` trả `Queue.Messages` là số message ready.

## Các điểm quan trọng

Queue luôn được khai báo với `x-queue-type: quorum` tường minh.
Compose của handbook đặt `default_queue_type = quorum`, nhưng RabbitMQ chạy không cấu hình thì mặc định là classic.
Khai báo lại một queue đã có với type khác sẽ lỗi `406 PRECONDITION_FAILED` (suy ra từ tài liệu về queue type immutable, lab không thử điều này).
Quorum queue không thể exclusive, auto-delete hay transient, nên mọi queue của lab là durable, không exclusive và được xóa thủ công khi dọn dẹp.

`prefetch_1_gives_slow_worker_fewer_messages` không dựa vào thời gian:

- Worker chậm giữ message đầu tiên của nó tới khi test cho phép (handler bị chặn bằng một promise hoặc channel).
- Worker nhanh ack ngay, nhưng chỉ bắt đầu sau khi worker chậm đã nhận message.
  Nhờ vậy kết quả không phụ thuộc broker giao message đầu tiên cho ai.
- Với 10 message, test chờ tới khi worker nhanh xử lý đủ 9, rồi kiểm tra worker chậm nhận đúng 1 và queue không còn message ready.
  Nếu prefetch bị bỏ, worker chậm sẽ giữ nhiều hơn 1 và worker nhanh không thể đạt 9.

`unacked_message_is_redelivered_after_worker_disconnects` mô phỏng crash bằng cách đóng connection của worker A khi handler của nó chưa xong.
Message được giao lại cho worker B với `redelivered=true`.
Hệ quả: consumer phải idempotent, vì message có thể được xử lý dở rồi chạy lại.

Các giá trị dưới đây đo trên RabbitMQ 4.3.6 (test khóa chúng lại):

- `reject` với `requeue=true`: lần giao lại đầu tiên có `x-delivery-count = 1` (không phải 0) cùng `x-acquired-count = 1`, lần giao lại thứ hai là 2 và 2.
  Lần giao đầu tiên không có hai header này.
- `nack` với `requeue=true`: chỉ tăng `x-acquired-count`, không tạo `x-delivery-count`.
  Đây là thay đổi của 4.3: delivery limit dựa trên delivery-count nên `nack` lặp mãi không bao giờ chạm limit 20.
- Consumer không gọi `basic.qos`: quorum queue giao tối đa 2000 message chưa ack rồi dừng.
  Test publish 2100 message và thấy đúng 100 message còn ready.
- Cùng thử nghiệm với classic queue (đo một lần bằng script thử, không có test): consumer nhận hết 2600 message, tức là không có giới hạn nào.

Giá trị mặc định built-in của `rabbit.default_consumer_prefetch` chưa xác minh được từ tài liệu.
Kết quả đo ở trên là hành vi quan sát được trên broker của compose (không đặt cấu hình này), không phải khẳng định về giá trị built-in.

## Chạy

```bash
make up
pnpm vitest run 05-rabbitmq/lab-01-work-queue/ts
go test -race ./05-rabbitmq/lab-01-work-queue/go/...
make lab-ts LAB=05-rabbitmq/lab-01-work-queue
make lab-go LAB=05-rabbitmq/lab-01-work-queue
```

## Kết quả mong đợi

Test (cùng tên ở TS và Go, Go dùng CamelCase):

| Test                                                            | Chứng minh                                                                                                               |
| --------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------ |
| `prefetch_1_gives_slow_worker_fewer_messages`                   | Worker chậm giữ đúng 1 message chưa ack, worker nhanh xử lý 9 message còn lại, queue không còn message ready             |
| `unacked_message_is_redelivered_after_worker_disconnects`       | Worker A nhận mà không ack rồi connection đóng, worker B nhận lại đúng message với `redelivered=true`                    |
| `get_on_empty_queue_returns_nothing`                            | `basic.get` trên queue rỗng không trả message và không lỗi                                                               |
| `reject_requeue_counts_toward_delivery_count_but_nack_does_not` | `reject(requeue=true)` đưa `x-delivery-count` lên 1 ở lần giao lại đầu, `nack(requeue=true)` chỉ tăng `x-acquired-count` |
| `without_prefetch_quorum_queue_delivers_at_most_2000_unacked`   | Không có prefetch, quorum queue vẫn chỉ giao 2000 message chưa ack                                                       |

Không test nào dùng sleep cố định: test chờ bằng `eventually`, promise, channel và publisher confirm.
Demo in số message mỗi worker xử lý với prefetch không giới hạn so với prefetch 1 (6 và 6 so với khoảng 10 và 2), rồi kịch bản worker crash và message được giao lại.

## Bài tập mở rộng

1. Đổi prefetch thành 5 và 100 trong demo, quan sát worker chậm giữ bao nhiêu message và tổng thời gian thay đổi thế nào.
2. Bật auto-ack (`noAck: true`) rồi cho worker crash, so sánh với kết quả của test redelivery.
3. Đặt `consumer_timeout` nhỏ trên broker thử nghiệm riêng và quan sát channel bị đóng khi worker giữ message quá lâu.
4. Thêm một handler idempotent bằng message id, rồi publish trùng một message để kiểm tra.
