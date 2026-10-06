# Lab 02 - Topic routing và message không route được

## Mục tiêu

Dùng topic exchange với binding wildcard và chứng minh bằng test ba điều:

- `*` thay thế đúng một từ: binding `orders.*.created` nhận `orders.eu.created`, không nhận `orders.created` và `orders.eu.vn.created`.
- `#` thay thế không hoặc nhiều từ: binding `orders.#` nhận `orders`, `orders.eu` và `orders.eu.vn.created`.
- Message không route được vẫn được broker confirm nhưng bị bỏ lặng lẽ, trừ khi publish với `mandatory=true` và nghe `basic.return` (reply code 312 `NO_ROUTE`).

Lab cần RabbitMQ chạy bằng `make up` (RabbitMQ 4.3.6).
Lab đọc `AMQP_URL` (mặc định `amqp://guest:guest@127.0.0.1:5672`).
Mỗi test tạo exchange và queue có tên duy nhất và xóa chúng khi kết thúc, kể cả khi test fail.
Lý thuyết nằm ở [theory.md](../theory.md).

## Kiến trúc

```mermaid
flowchart LR
    P[Publisher<br/>confirm mode] -->|"routing key"| X{{"topic exchange"}}
    X -->|"binding orders.*.created"| Q1[("queue A<br/>quorum")]
    X -->|"binding orders.#"| Q2[("queue B<br/>quorum")]
    X -.->|"không binding nào khớp"| Drop((bỏ lặng lẽ))
    Drop -.->|"chỉ khi mandatory=true: basic.return 312"| P
```

Đường lỗi: message không route được.
Không có `mandatory` thì publisher chỉ thấy confirm, có `mandatory` thì thấy `basic.return` trước confirm.

```mermaid
sequenceDiagram
    participant P as Publisher
    participant X as Topic exchange
    participant Q as Queue (binding orders.eu.created)
    P->>X: publish payments.refund (mandatory=false)
    X-->>X: không có binding khớp, message bị bỏ
    X-->>P: basic.ack (confirm)
    Note over Q: queue vẫn có 0 message, publisher không biết gì
    P->>X: publish payments.refund (mandatory=true)
    X-->>P: basic.return 312 NO_ROUTE
    X-->>P: basic.ack (confirm, đến sau return)
    P->>X: publish orders.eu.created (mandatory=true)
    X->>Q: route được
    X-->>P: basic.ack (không có return)
```

## Giao diện

TypeScript (`ts/lab.ts`, amqplib 2.2.0):

```ts
openConnection(): Promise<ChannelModel>;
declareTopicRouting(channel, name, pattern): Promise<{ exchange; queue }>; // topic exchange <name>, quorum queue <name>.queue
new ConfirmedPublisher(channel: ConfirmChannel); // nghe sự kiện 'return', giữ danh sách .returned
publisher.publish(exchange, routingKey, body, { mandatory? }): Promise<ReturnedMessage | null>; // resolve sau confirm
receiveRoutingKeys(channel, queue): Promise<string[]>; // basic.get tới khi queue rỗng
```

Go (`go/lab.go`, amqp091-go v1.15.0): `URL()`, `DeclareTopicRouting(ch, name, pattern)`, `NewPublisher(ch)` với `Publish(ctx, exchange, key, body, mandatory) (*amqp.Return, error)` và `ReceiveRoutingKeys(ch, queue)`.
Go trả `nil` thay cho `null` khi không có return.

## Các điểm quan trọng

Quy tắc của topic exchange:

- Routing key là các từ phân tách bằng dấu chấm, tối đa 255 byte.
- `*` thay đúng một từ, `#` thay không hoặc nhiều từ.
- Binding chỉ có `#` nhận mọi message như fanout, binding không có wildcard hoạt động như direct.
- `orders.#` khớp cả `orders` trơn (không có từ nào sau `orders`), điểm này hay bị nhầm với `orders.*`.

Confirm không chứng minh message đã vào queue.
Broker vẫn confirm message mà exchange không route được vào queue nào.
Test `unrouted_message_is_dropped_without_mandatory_flag` publish `payments.refund` khi chỉ có binding `orders.eu.created`:

- Không có `mandatory`: confirm về, không có return, và `messageCount` của queue là 0 ngay sau confirm.
  Vì confirm đã về nên không cần chờ gì thêm, đây là lý do test không dùng sleep.
- Có `mandatory=true`: broker gửi `basic.return` với reply code 312 và reply text `NO_ROUTE` trước `basic.ack` của chính message đó, rồi mới confirm.
- Có `mandatory=true` mà message route được: không có return.

Cách client nhận return:

- amqplib: sự kiện `channel.on('return', ...)` với `message.fields.replyCode`, `replyText`, `exchange`, `routingKey`.
  File kiểu của amqplib 2.2.0 chưa khai báo `replyCode` và `replyText` trong `MessageFields`, nên lab ép kiểu hẹp để đọc chúng (giá trị có mặt lúc chạy, test xác nhận).
- amqp091-go: `Channel.NotifyReturn(chan amqp.Return)`.
  Kênh phải có đệm, vì goroutine đọc frame của thư viện gửi vào kênh này và sẽ bị chặn nếu không ai đọc.

Vì return đến trước ack, `ConfirmedPublisher.publish` đọc danh sách return ngay sau khi confirm về mà không cần timeout.
Publish tới exchange không tồn tại là lỗi khác hẳn: broker đóng cả channel với `404 NOT_FOUND`.
Có một cách phòng thủ khác là alternate exchange (lab không làm).

Queue là quorum (`x-queue-type: quorum` tường minh), durable, không exclusive, không auto-delete, và được xóa thủ công khi dọn dẹp.
Mỗi test dùng một channel confirm riêng để các lần return không lẫn vào nhau.

## Chạy

```bash
make up
pnpm vitest run 05-rabbitmq/lab-02-topic-routing/ts
go test -race ./05-rabbitmq/lab-02-topic-routing/go/...
make lab-ts LAB=05-rabbitmq/lab-02-topic-routing
make lab-go LAB=05-rabbitmq/lab-02-topic-routing
```

## Kết quả mong đợi

Test (cùng tên ở TS và Go, Go dùng CamelCase):

| Test                                                 | Chứng minh                                                                                                              |
| ---------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------- |
| `orders_star_created_matches_one_word_only`          | Binding `orders.*.created` chỉ nhận `orders.eu.created`, không nhận `orders.created` và `orders.eu.vn.created`          |
| `orders_hash_matches_zero_or_more_words`             | Binding `orders.#` nhận `orders`, `orders.eu`, `orders.eu.vn.created` theo thứ tự và không nhận `payments.created`      |
| `unrouted_message_is_dropped_without_mandatory_flag` | Không mandatory: có confirm, không có return, queue rỗng. Mandatory: return 312 `NO_ROUTE`. Route được: không có return |

Không test nào dùng sleep cố định: test chờ bằng publisher confirm và `eventually`.
Demo publish năm routing key tới ba pattern và in key nào vào được queue, rồi so sánh publish không mandatory với mandatory trên message không route được.

## Bài tập mở rộng

1. Thêm một binding thứ hai `orders.#` vào cùng một exchange và kiểm tra message khớp cả hai binding được nhân bản vào cả hai queue.
2. Khai báo exchange với alternate exchange và cho message không route được rơi vào một queue "unrouted", rồi so sánh với `mandatory`.
3. Thử exchange kiểu `direct`, `fanout` và `headers` với cùng bộ message và so sánh kết quả routing.
4. Publish tới một exchange không tồn tại và quan sát channel bị đóng với `404 NOT_FOUND`, rồi mở channel mới để tiếp tục.
