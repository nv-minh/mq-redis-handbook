# Use case 2 - Order và payment hướng sự kiện: outbox, Kafka, consumer idempotent và saga

## Mục tiêu và kịch bản

Một cửa hàng nhận order qua `POST /orders`.
Sau khi order được nhận, hai việc phải xảy ra mà không bắt request chờ: giữ hàng trong kho (`inventory`) và thanh toán (`payment`).
Nếu thanh toán lỗi thì hàng đã giữ phải được trả lại và order bị hủy.
Use case này dựng đúng đường đi đó bằng Postgres (outbox), Kafka, hai consumer idempotent và một saga, ở cả TypeScript (KafkaJS wrapper của `@confluentinc/kafka-javascript`) và Go (kafka-go), rồi chứng minh bằng test:

- `POST /orders` ghi order và event `OrderPlaced` vào outbox trong MỘT transaction, rồi relay publish đúng một event lên topic Kafka với key là order id.
- Relay chết (SIGKILL) giữa lúc publish và lúc đánh dấu `published_at` thì event được publish lại, nhưng hai consumer chỉ áp dụng effect một lần.
- Hai consumer dedupe theo `eventId` bằng một dòng trong bảng `processed_events`, chèn trong CÙNG transaction với effect.
- Thanh toán lỗi (luật xác định: amount vượt hạn mức) thì saga hoàn kho, order thành `cancelled`, kho về giá trị ban đầu, và event lỗi được giao lại cũng không hoàn kho hai lần.
- Lag của consumer group (log end offset trừ offset đã commit) tính được đúng, về 0 sau khi consumer bắt kịp, và nằm trên `/metrics` dưới dạng gauge.

Use case cần Postgres và Kafka chạy bằng `make up`, đọc `DATABASE_URL` (mặc định `postgres://handbook:handbook@127.0.0.1:5432/handbook`) và `KAFKA_BROKERS` (mặc định `127.0.0.1:9092`).
Mỗi test có schema Postgres riêng, topic Kafka riêng (3 partition, replication factor 1) và consumer group riêng, tất cả có tên duy nhất và bị xóa khi test kết thúc.
Lý thuyết của cả chương nằm ở theory.md của chủ đề 09.
Use case này dùng lại cách làm của lab 08-05 (outbox), lab 08-06 (saga) và lab 08-01 (dedup), viết lại gọn bên trong use case và không import chéo.

## Kiến trúc

```mermaid
flowchart LR
    C[Client] -->|"POST /orders"| H["HTTP service<br/>node:http hoặc net/http, cổng 0"]
    H -->|"1 transaction: orders, order_history, outbox"| PG[("Postgres<br/>orders, outbox, stock,<br/>payments, processed_events")]
    PG -->|"SELECT FOR UPDATE SKIP LOCKED"| R["Relay<br/>runRelay"]
    R -->|"publish acks=all,<br/>key là orderId"| K[["Kafka topic<br/>3 partition"]]
    K --> I["Consumer inventory<br/>group prefix-inventory"]
    K --> P["Consumer payment<br/>group prefix-payment"]
    I -->|"1 transaction: dedup,<br/>effect, event follow-up"| PG
    P -->|"1 transaction: dedup,<br/>effect, event follow-up"| PG
    H -->|"GET /metrics"| M["Registry riêng của instance<br/>consumer_events_total,<br/>consumer_group_lag"]
    M -.->|"đo lag mỗi lần scrape"| K
    PR[Prometheus] -.->|scrape| M
```

Một `OrderService` (TypeScript: `createOrderService`, Go: `NewService`) gồm một HTTP server trên cổng do hệ điều hành chọn, một producer Kafka (acks=all), một registry Prometheus riêng và các consumer do `startConsumer` chạy.
Relay là hàm `runRelay` (TypeScript) hoặc `RunRelay` (Go) chạy MỘT lượt: test gọi nó tường minh, demo gọi nó trong một vòng lặp nền.
Mọi event, kể cả event follow-up do consumer sinh ra, đi qua cùng bảng outbox và cùng một relay.

## Hợp đồng HTTP

`POST /orders` nhận JSON và hợp đồng này giống hệt ở hai ngôn ngữ.

| Trường     | Kiểu và luật                                                              |
| ---------- | ------------------------------------------------------------------------- |
| `amount`   | Số hữu hạn lớn hơn 0 (bắt buộc)                                           |
| `sku`      | Chuỗi không rỗng, tối đa 64 ký tự tính theo code point (bắt buộc)         |
| `quantity` | Số nguyên từ 1 tới 1000000, mặc định 1                                    |
| `id`       | Tùy chọn, 1 tới 64 ký tự trong `A-Za-z0-9_-`, thiếu thì sinh `ord-<uuid>` |

| Status | Khi nào                                                                                      |
| ------ | -------------------------------------------------------------------------------------------- |
| 201    | Body `{ "id": "...", "status": "placed" }`, order và event đã nằm trong database             |
| 400    | Body không phải JSON, không phải object, hoặc một trường sai luật: body `{ "error": "..." }` |
| 409    | Order với `id` này đã tồn tại, không ghi thêm gì                                             |
| 413    | Body lớn hơn 64 KiB                                                                          |
| 404    | Đường dẫn khác `/orders` và `/metrics`                                                       |
| 405    | `/orders` với method khác POST, kèm header `Allow: POST`                                     |

Request sai luôn trả 400 TRƯỚC khi mở transaction, nên không có dòng nào được ghi (test `invalid_post_body_returns_400_and_inserts_nothing` đếm `orders`, `outbox` và `order_history`).
`POST` lặp lại với cùng `id` trả 409 chứ không phải 200: use case không làm idempotency key cho HTTP (xem phần giới hạn).

## Mô hình dữ liệu

Mỗi bảng thuộc một "service" logic, nhưng chúng nằm chung một database để demo gọn.

| Bảng               | Vai trò                                                                                              |
| ------------------ | ---------------------------------------------------------------------------------------------------- |
| `orders`           | Order và trạng thái hiện tại (`placed`, `paid`, `payment_failed`, `cancelled`)                       |
| `order_history`    | Dấu vết mỗi lần đổi trạng thái, test đọc nó để kiểm tra thứ tự các trạng thái                        |
| `outbox`           | Event chờ publish, `event_id` do Postgres sinh một lần khi ghi dòng, `published_at` null là chưa gửi |
| `stock`            | Tồn kho theo `sku`, ràng buộc `quantity >= 0` là hàng rào cuối: không bao giờ bán âm                 |
| `reservations`     | Một dòng cho mỗi order: `reserved`, `released` hoặc `rejected`                                       |
| `payments`         | `order_id` là khóa chính và `event_id` unique: một order không thể có hai khoản thanh toán           |
| `processed_events` | Bảng dedup (inbox), khóa là `(consumer, event_id)`                                                   |

Khóa dedup gồm cả tên consumer vì `inventory` và `payment` là hai consumer độc lập cùng đọc một event (research: khóa dedup phải gồm consumer identity và message identity).
`event_id` được sinh một lần bằng `gen_random_uuid()` khi INSERT dòng outbox, nên mọi lần relay publish lại đều mang cùng id.
Đó là điều kiện để dedup hoạt động: id phải ổn định qua mọi lần redeliver, không phải id do broker hay thời điểm nhận sinh ra.

## Envelope event

Value của message Kafka là JSON, key là order id.
Cùng key thì cùng partition, nên mọi event của một order nằm trong một partition và giữ thứ tự (Debezium Outbox Event Router dùng `aggregateid` làm key theo cùng lý do).

```json
{
  "eventId": "9f0c...",
  "type": "OrderPlaced",
  "orderId": "ord-...",
  "data": { "amount": 25.5, "sku": "sku-1", "quantity": 2 }
}
```

| Event               | Ai phát                           | Ai xử lý                                   |
| ------------------- | --------------------------------- | ------------------------------------------ |
| `OrderPlaced`       | `POST /orders` (qua outbox)       | `inventory` giữ hàng, `payment` thanh toán |
| `InventoryReserved` | `inventory` sau khi giữ hàng      | Không ai (dấu vết)                         |
| `InventoryRejected` | `inventory` khi không đủ hàng     | Không ai (dấu vết)                         |
| `PaymentCompleted`  | `payment` khi charge thành công   | Không ai (dấu vết)                         |
| `PaymentFailed`     | `payment` khi amount vượt hạn mức | `inventory` hoàn kho (compensation)        |
| `InventoryReleased` | `inventory` sau khi hoàn kho      | Không ai (dấu vết)                         |

## Saga: choreography, không có orchestrator

Saga ở đây là choreography: mỗi local transaction ghi effect cùng một event follow-up vào outbox, và event đó kích hoạt bước kế tiếp ở service khác.
Chuỗi cho đường lỗi là `OrderPlaced`, rồi `payment` phát `PaymentFailed`, rồi `inventory` nhận `PaymentFailed`, hoàn kho và phát `InventoryReleased`.
Không có thành phần nào giữ "trạng thái saga": trạng thái của order nằm trong bảng `orders` và `order_history`, và mỗi bước tự quyết định dựa trên event nó nhận.

Lý do chọn choreography thay vì orchestrator như lab 08-06 (lý do thiết kế, không phải tuyên bố của nguồn nào):

- Chỉ có hai participant và một đường bù, nên không có luồng phức tạp cần một chỗ để đọc.
- Orchestrator trong lab 08-06 chạy trong bộ nhớ và chưa ghi tiến độ bền vững để resume sau khi chết.
  Ở đây mỗi bước đã là một transaction có outbox, nên tiến độ nằm sẵn trong database và event.
- Compensation (hoàn kho) là một event handler bình thường, nên nó tự thừa hưởng dedup, retry và redelivery của hạ tầng consumer.

Đổi lại, luồng bị rải ra các handler: muốn biết "saga đang ở bước nào" phải đọc `order_history`.
Khi số bước tăng lên (thêm vận chuyển, hóa đơn, nhiều đường bù), orchestrator dễ đọc và dễ thay đổi hơn.

Thứ tự `OrderPlaced` trước `PaymentFailed` trong partition đảm bảo `inventory` luôn giữ hàng TRƯỚC khi nhận lệnh hoàn kho của cùng order: cả hai event cùng key nên cùng partition, và `PaymentFailed` chỉ được ghi sau khi `payment` đã đọc `OrderPlaced`.
Hoàn kho còn có guard thứ hai: câu `UPDATE reservations SET status = 'released' WHERE status = 'reserved'` chỉ trúng một lần, nên một `PaymentFailed` khác `eventId` cho cùng order không hoàn kho thêm (test gửi cả hai kiểu giao lại).
Trạng thái order cũng có guard tương tự (`UPDATE orders ... WHERE status = ...`), nên một event lặp không kéo order lùi về trạng thái cũ.

```mermaid
stateDiagram-v2
    [*] --> placed: POST /orders
    placed --> paid: payment charged
    placed --> payment_failed: amount vượt hạn mức
    payment_failed --> cancelled: kho đã hoàn
    placed --> cancelled: không đủ hàng
    paid --> cancelled: không đủ hàng sau khi đã thanh toán
    paid --> [*]
    cancelled --> [*]
```

## Dedup trong transaction

Mỗi event được xử lý trong MỘT transaction Postgres gồm ba việc theo thứ tự: chèn dòng `(consumer, event_id)` vào `processed_events`, áp dụng effect, ghi event follow-up vào outbox.

```sql
insert into processed_events (consumer, event_id) values ($1, $2) on conflict do nothing
-- 1 dòng: lần đầu, chạy effect. 0 dòng: đã xử lý, bỏ qua và commit.
```

Vì ba việc cùng commit hoặc cùng rollback:

- Lỗi chen giữa dòng dedup và effect rollback cả hai, nên lần giao lại sau đó áp dụng được (test cố ý ném lỗi ngay sau khi chèn dòng dedup và kiểm tra cả dòng dedup lẫn effect đều biến mất).
- Hai bản của cùng một event chạy đồng thời không áp dụng effect hai lần: unique constraint làm trọng tài, bản thứ hai chờ bản đầu commit rồi gặp conflict.
- Event follow-up chỉ tồn tại nếu effect commit, nên bản trùng không sinh thêm event (test chaos đếm đúng một `InventoryReserved` và một `PaymentCompleted` trên topic).

Đây là biến thể mạnh của dedup mà lab 08-01 nêu trong phần giới hạn: marker và effect nằm trong cùng database.
Cửa sổ "đang xử lý" của lab 08-01 (key đã chiếm nhưng effect chưa xong, bản trùng bị ack nhầm rồi effect đầu lỗi nên effect mất) KHÔNG áp dụng ở đây.
Lý do: không có trạng thái trung gian nhìn thấy được từ ngoài transaction.
Dòng dedup chưa commit thì không ai thấy, và nếu effect lỗi thì dòng dedup cũng rollback nên bản giao lại không bị coi là trùng.

## Offset: commit SAU effect

Consumer tắt auto commit và chỉ commit offset sau khi transaction của effect đã commit.
Crash giữa hai bước cho một bản trùng, không bao giờ làm mất event: đó là at-least-once, và dedup biến nó thành effectively-once.

```mermaid
sequenceDiagram
    participant K as Kafka
    participant C1 as Consumer inventory 1
    participant DB as Postgres
    participant C2 as Consumer inventory 2
    K-->>C1: OrderPlaced (eventId E)
    C1->>DB: 1 transaction: dedup E, trừ kho, InventoryReserved
    DB-->>C1: commit thành công
    Note over C1: crash TRƯỚC khi commit offset
    Note over K: offset của group vẫn chưa nhúc nhích, lag là 1
    K-->>C2: OrderPlaced (eventId E) giao lại
    C2->>DB: chèn dedup E, đã có nên 0 dòng
    DB-->>C2: bỏ qua, không trừ kho lần nữa
    C2->>K: commit offset
    Note over K: lag về 0, effect đúng một lần
```

Test `consumer_crash_after_effect_before_offset_commit_redelivers_and_is_deduped` mô phỏng crash bằng một hook `crashBeforeCommit` (consumer dừng xử lý và không commit gì, giống `consumeOneThenCrash` của lab 06-kafka/lab-03).
Khác với kill -9 thật ở chỗ `stop()` gửi LeaveGroup nên consumer thay thế vào group ngay, còn crash thật phải chờ `session.timeout.ms`.
Offset mà group lưu giống hệt nhau trong cả hai trường hợp, và đó là thứ test kiểm chứng.

Chọn API client:

- TypeScript: `consumer.run({ eachMessage })` với `autoCommit: false`, gọi `commitOffsets` (offset message cộng 1, vì Kafka lưu "offset kế tiếp cần đọc") sau khi handler xong.
  Handler ném lỗi thì wrapper log lỗi, seek về message đó và giao lại, nên consumer chờ 200 ms rồi ném lại để không thành vòng lặp nóng.
- Go: `kafka.Reader` với `GroupID`, `FetchMessage` rồi `CommitMessages` (như lab 06-kafka/lab-03).
  `FetchMessage` không commit và `CommitMessages` với `CommitInterval` 0 commit đồng bộ.
  Chọn Reader thay vì `ConsumerGroup` (lab 06-kafka/lab-02) vì chỉ cần một vòng lặp tuần tự và Reader tự xử lý rebalance, còn assignment của từng member không cần ở đây.
  Handler lỗi thì vòng lặp thử lại đúng message đó sau 200 ms.

Poison event (value không phải JSON, thiếu `eventId`, `type`, `orderId` hay `data`, hoặc `OrderPlaced` thiếu `sku`, `quantity`, `amount` hợp lệ) được log, đếm bằng `consumer_events_total{result="poison"}` rồi commit offset để bỏ qua.
Quyết định này nhằm không để một message hỏng chặn partition mãi mãi.
Use case không có DLQ topic (YAGNI): nội dung hỏng chỉ nằm trong log (80 ký tự đầu) nên không replay được.
Loại event lạ (`type` không nằm trong danh sách trên) không phải poison: consumer đếm là `ignored` và đi tiếp, để thêm loại event mới không làm hỏng consumer cũ.

## Đường lỗi

Đường lỗi 1: relay chết giữa publish và mark, event bị publish lại, consumer dedupe.

```mermaid
sequenceDiagram
    participant A as Relay A
    participant DB as Postgres
    participant K as Kafka
    participant C as Consumer inventory và payment
    participant B as Relay B
    A->>DB: BEGIN, SELECT outbox FOR UPDATE SKIP LOCKED
    A->>K: publish OrderPlaced (eventId E, acks=all)
    K-->>A: ack
    K-->>C: giao E lần 1
    C->>DB: 1 transaction: dedup E, effect, event follow-up
    Note over A: SIGKILL, chưa UPDATE published_at, chưa COMMIT
    Note over DB: connection của A đứt, rollback, nhả lock
    B->>DB: SELECT outbox FOR UPDATE SKIP LOCKED
    DB-->>B: dòng của E, vẫn chưa publish
    B->>K: publish lại OrderPlaced (cùng eventId E)
    K-->>C: giao E lần 2
    C->>DB: chèn dedup E, đã có nên 0 dòng
    Note over C: bỏ qua, effect và event follow-up đúng một lần
    B->>DB: UPDATE published_at, COMMIT
```

Đường lỗi 2: thanh toán lỗi và compensation.

```mermaid
sequenceDiagram
    participant S as HTTP service
    participant DB as Postgres
    participant K as Kafka
    participant P as Consumer payment
    participant I as Consumer inventory
    S->>DB: 1 transaction: orders, outbox OrderPlaced
    K-->>I: OrderPlaced
    I->>DB: dedup, trừ kho, reservation reserved, outbox InventoryReserved
    K-->>P: OrderPlaced
    P->>DB: dedup, payment failed, order payment_failed, outbox PaymentFailed
    K-->>I: PaymentFailed
    I->>DB: dedup, reservation released, cộng kho, order cancelled, outbox InventoryReleased
    Note over DB: order_history là placed, payment_failed, cancelled
    K-->>I: PaymentFailed giao lại (cùng eventId hoặc eventId khác)
    I->>DB: dedup chặn bản cùng id, guard reserved chặn bản khác id
    Note over I: kho không được cộng lần hai
```

Đường lỗi 3 (consumer crash trước khi commit offset) nằm ở phần "Offset" ở trên.

## Consumer lag

`consumerLag(groupId, topic)` (Go: `ConsumerLag(ctx, groupId, topic) (int64, error)`) trả tổng theo partition của `log end offset - offset đã commit của group`.
Research ghi lag của một partition là `LOG-END-OFFSET` trừ `CURRENT-OFFSET`, và `Reader.Lag()` của kafka-go luôn trả -1 khi dùng `GroupID`, nên Go phải dùng admin API.

| Việc                         | TypeScript (`@confluentinc/kafka-javascript` 1.10.1)                         | Go (kafka-go v0.4.51)                                            |
| ---------------------------- | ---------------------------------------------------------------------------- | ---------------------------------------------------------------- |
| Log end offset và offset đầu | `admin.fetchTopicOffsets(topic)`: `{ partition, offset, high, low }` (chuỗi) | `Client.ListOffsets` với `LastOffsetOf` và `FirstOffsetOf`       |
| Offset đã commit             | `admin.fetchOffsets({ groupId, topics: [{ topic, partitions }] })`           | `Client.OffsetFetch` với `OffsetFetchRequest{ GroupID, Topics }` |
| Partition chưa có commit     | `offset` là `"-1001"` (đo)                                                   | `CommittedOffset` là `-1` (đo)                                   |

Quy tắc tính: partition chưa có offset đã commit tính từ offset đầu (`low`), vì consumer mới của group đọc từ `earliest`.
Offset đã commit nhỏ hơn `low` (retention đã xóa đoạn đầu của log) cũng tính từ `low`.
Message đang được xử lý mà chưa commit vẫn tính là lag: lag đo "việc chưa xong", không phải "việc chưa nhận".
Vì thế test `consumer_lag_reaches_zero_after_catching_up` khẳng định lag đúng bằng N trước khi consumer chạy (group chưa commit gì), rồi `eventually` lag bằng 0.

Hai chi tiết đo được của TypeScript: gọi `fetchOffsets` với danh sách tên topic (không chỉ định partition) trả mảng rỗng cho group chưa có commit, nên code truyền tường minh danh sách partition lấy từ `fetchTopicOffsets`.
Ngay sau khi tạo topic, `fetchTopicOffsets` và `ListOffsets` có thể báo lỗi leader tạm thời dù metadata đã có leader, nên cả hai ngôn ngữ thử lại tới 10 giây.

### Gauge trên /metrics và cảnh báo

Endpoint `GET /metrics` của mỗi `OrderService` có registry riêng (không dùng registry mặc định toàn cục) với hai metric:

- `consumer_events_total{consumer, result}`: số message mỗi consumer đã xử lý, `result` là `applied`, `duplicate`, `ignored` hoặc `poison`.
  Mọi series được khởi tạo bằng 0.
- `consumer_group_lag{group, topic}`: lag của từng group, đo MỖI LẦN scrape bằng `consumerLag` (không cache).
  Nếu Kafka không trả lời thì scrape trả 500 sau tối đa 10 giây (test `metrics_endpoint_returns_500_when_kafka_is_unreachable`), nên Prometheus thấy `up = 0` thay vì một giá trị lag cũ hay một body rỗng.

Cảnh báo nên dựa vào gì (research, nguồn thứ cấp, chưa xác minh với tài liệu chính thức): báo động khi lag tăng liên tục lâu hơn thời gian rebalance hoặc batch bình thường, hoặc khi committed offset đứng yên trong khi lag lớn hơn 0.
Một ngưỡng cố định ("lag lớn hơn 1000") dễ báo sai khi traffic có đỉnh: Burrow của LinkedIn đánh giá lag theo cửa sổ trượt thay vì ngưỡng cố định.
Use case này không cài Burrow và không kiểm chứng các quy tắc cảnh báo trên bằng test, nó chỉ cung cấp con số lag chính xác để các quy tắc đó dựa vào.
Consumer Java xuất `records-lag-max` (lag lớn nhất của mọi partition trong cửa sổ), `records-lag` và `records-lag-avg` qua JMX, còn các gauge ở đây đo từ phía admin nên không cần consumer nào đang chạy.

## Giao diện

TypeScript (`ts/lab.ts`, `ts/kafka.ts`, `ts/events.ts`):

```ts
createOrderService({ pool, topic, groupPrefix, batchSize? }): Promise<OrderService>;
service.url; service.metricsUrl;                       // cổng do hệ điều hành chọn
service.runRelay(): Promise<number>;                   // một lượt relay, trả số event đã publish
service.startConsumer("inventory" | "payment", { paymentLimit?, afterDedupInsert?, crashBeforeCommit? }): Promise<RunningConsumer>;
service.groupId(name): string;
service.close(): Promise<void>;                        // dừng consumer, đóng HTTP server và producer (không đóng pool)
runRelay(pool, publish, { batchSize }): Promise<number>;
inventoryHandler(pool, { afterDedupInsert? }); paymentHandler(pool, { paymentLimit?, afterDedupInsert? });
placeOrder(pool, { id, amount, sku, quantity }): Promise<void>;
consumerLag(groupId: string, topic: string): Promise<number>;
readTopic(topic): Promise<TopicRecord[]>;              // đọc cả log bằng group tạm, rồi xóa group
```

Go (`go/lab.go`, `go/kafka.go`, `go/events.go`): `NewService(ctx, Config{Pool, Topic, GroupPrefix, BatchSize})` với `URL()`, `MetricsURL()`, `RunRelay(ctx)`, `StartConsumer(name, ConsumerOptions{PaymentLimit, AfterDedupInsert, CrashBeforeCommit})`, `GroupID(name)`, `Close(ctx)`, cùng `RunRelay`, `InventoryHandler`, `PaymentHandler`, `PlaceOrder`, `ConsumerLag(ctx, groupId, topic) (int64, error)` và `ReadTopic`.
Tên giống nhau ở hai ngôn ngữ (Go viết hoa chữ đầu).

Khác biệt cố ý giữa hai bên:

- TypeScript đọc topic trong `readTopic` bằng một group tạm (rồi xóa), Go đọc từng partition trực tiếp bằng `Reader` không có `GroupID` nên không để lại group nào.
- Hook mô phỏng lỗi (`afterDedupInsert`, `crashBeforeCommit`) chỉ phục vụ test, demo không dùng chúng, và luồng thật cũng không dùng.
- Go trả `*InvalidOrderError` và `ErrDuplicateOrder`, TypeScript ném `InvalidOrderError` và `DuplicateOrderError`.

## Chạy

```bash
make up
export DATABASE_URL=postgres://handbook:handbook@127.0.0.1:5432/handbook   # đổi cổng nếu stack Postgres của bạn publish cổng khác
pnpm vitest run 09-production/usecase-02-order-events
go test -race ./09-production/usecase-02-order-events/...
make lab-ts LAB=09-production/usecase-02-order-events
make lab-go LAB=09-production/usecase-02-order-events
```

Demo in sáu kịch bản: order hợp lệ (reserve, charge, `paid`), thanh toán lỗi và saga hoàn kho, hết hàng, event bị giao trùng, lag trên `/metrics`, và danh sách event trên topic.
Cả hai demo chạy relay trong một vòng lặp nền và dọn schema, topic và group của chúng ở cuối.
Nếu Postgres của bạn không ở cổng 5432 (ví dụ `POSTGRES_PORT=5439 make up`), đặt `DATABASE_URL` tương ứng trước khi chạy.

## Kết quả mong đợi

Test (cùng ý nghĩa ở TS và Go, Go dùng CamelCase).
Năm test đầu là năm hành vi cốt lõi của use case.

| Test                                                                                | Chứng minh                                                                                                                                                                                                           |
| ----------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `post_orders_emits_exactly_one_order_placed_event`                                  | POST thật trả 201, relay publish đúng một `OrderPlaced` (key là order id, payload và `eventId` đúng), lần relay thứ hai publish 0                                                                                    |
| `chaos_relay_killed_between_publish_and_mark_still_yields_single_effect_downstream` | Relay là process con bị SIGKILL sau publish, relay mới publish lại, event nằm trên topic ít nhất hai lần nhưng effect, event follow-up và order chỉ một lần                                                          |
| `duplicate_event_does_not_double_charge`                                            | Cùng `eventId` tới ba lần (hai lần cùng partition, một lần key khác): một payment, một reservation, kho trừ một lần. Lỗi giữa dedup và effect rollback cả hai                                                        |
| `payment_failure_triggers_inventory_release_via_saga`                               | Thanh toán lỗi: reservation `released`, order `cancelled`, kho về ban đầu, dấu vết `placed, payment_failed, cancelled`, giao lại `PaymentFailed` không hoàn hai lần                                                  |
| `consumer_lag_reaches_zero_after_catching_up`                                       | Lag đúng bằng N khi group chưa commit gì, về 0 sau khi consumer chạy, gauge `/metrics` khớp                                                                                                                          |
| `invalid_post_body_returns_400_and_inserts_nothing`                                 | Body sai trả 400 và không ghi gì, id trùng trả 409, đường dẫn và method lạ trả 404 và 405                                                                                                                            |
| `relay_with_an_empty_outbox_returns_zero_without_publishing`                        | Outbox rỗng: relay trả 0, không gọi publish, topic rỗng                                                                                                                                                              |
| `consumer_crash_after_effect_before_offset_commit_redelivers_and_is_deduped`        | Crash sau effect, trước commit offset: lag còn 1, consumer thay thế nhận lại, dedup bỏ qua, effect một lần                                                                                                           |
| `poison_event_is_skipped_counted_and_does_not_block_the_partition`                  | Hai event hỏng đứng trước event hợp lệ trong cùng partition: được đếm là `poison`, bị bỏ qua và event hợp lệ vẫn được xử lý                                                                                          |
| `out_of_stock_order_is_rejected_and_cancelled_when_payment_ran_first`               | Kho 1, order cần 3, `payment` xử lý trước: reservation `rejected`, kho không đổi, `InventoryRejected` một lần, order `placed, paid, cancelled`, payment vẫn `charged` (giới hạn đã biết), giao lại không hủy hai lần |
| `out_of_stock_order_is_rejected_and_cancelled_when_inventory_ran_first`             | Như trên với `inventory` xử lý trước: order `placed, cancelled`                                                                                                                                                      |
| `metrics_endpoint_returns_500_when_kafka_is_unreachable`                            | Broker không với tới: `/metrics` trả 500 kèm thông báo lỗi trong thời gian có hạn, không phải 200 với body rỗng                                                                                                      |
| `sku_length_counts_code_points_not_utf16_units`                                     | 64 ký tự ngoài BMP được nhận, 65 bị từ chối, giống nhau ở hai ngôn ngữ                                                                                                                                               |
| `parse_event_treats_order_placed_without_valid_amount_as_poison`                    | `OrderPlaced` thiếu, null hoặc sai kiểu `amount` là poison (Go: `TestParseEventTreatsOrderPlacedWithoutValidAmountAsPoison`)                                                                                         |

Chaos test (kèm các bước bảo vệ như chương 08: chaos test của các chương này giết một process, không bao giờ dừng container của broker): relay chạy trong một process con thật (TypeScript: `node --import tsx ts/chaos-worker.ts`, Go: chạy lại chính test binary ở chế độ helper process), publish lên Kafka với acks=all, ghi marker vào một bảng của chính schema (không dùng Redis) rồi treo trước khi đánh dấu.
Process cha chờ marker bằng `eventually`, kiểm tra child thoát sớm để báo lỗi ngay kèm stderr của child, gửi SIGKILL, reap child và có thời hạn tự thoát 60 giây để không bao giờ thành process mồ côi.
Use case không dừng container nào của hạ tầng.

Thời gian đo trên máy phát triển (một lần, không phải cam kết): bộ test TypeScript khoảng 26 giây, bộ test Go khoảng 15 giây.

## Kết quả đo

Các con số dưới đây do test, demo và các lần thử khi viết use case đo, không lấy từ tài liệu.

| Chủ đề                                       | Kết quả                                                                                                                        |
| -------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------ |
| `fetchOffsets` của group chưa commit (TS)    | Truyền tường minh partition thì mỗi partition trả `offset: "-1001"`, truyền tên topic thì trả mảng rỗng                        |
| `OffsetFetch` của group chưa commit (Go)     | `CommittedOffset = -1` cho mọi partition, không có lỗi                                                                         |
| Lag khi consumer chưa chạy                   | Đúng bằng số event đã publish (test khẳng định `lag == N`), sau khi consumer bắt kịp thì bằng 0                                |
| Chaos: số bản của event trên topic           | Ít nhất 2 (một từ child, một từ relay mới), mỗi consumer có `applied = 1` và `duplicate >= 1`                                  |
| Child hỏng lúc khởi động (drill)             | Test fail sau khoảng 3 giây (TS) hoặc 1 giây (Go) với stderr của child, không đợi hết timeout 40 giây                          |
| Số event trên topic của một order thành công | `OrderPlaced`, `InventoryReserved`, `PaymentCompleted` (thứ tự giữa hai event sau không xác định vì hai consumer chạy độc lập) |

## Giới hạn thật của use case

- At-least-once từ đầu tới cuối, effectively-once nhờ dedup.
  Relay có thể publish một event nhiều lần và Kafka có thể giao một message nhiều lần, nên cùng một event tới consumer nhiều lần là bình thường.
  Tính đúng đắn đến từ dòng dedup trong transaction Postgres, không phải từ broker.
- Exactly-once của Kafka (idempotent producer, transaction, `read_committed`) chỉ bao phủ chuỗi đọc, xử lý, ghi trong Kafka.
  Nó không bao phủ ghi vào database hay API bên ngoài, nên không giúp ở đây: effect của use case nằm ở Postgres.
  Cách tổng quát cho đích ngoài Kafka là lưu offset cùng chỗ với output hoặc làm idempotent theo key, và use case chọn cách thứ hai (dedup theo `eventId`).
  Lab cũng không dùng transaction của Kafka: kafka-go v0.4.51 không có API transaction trong `Writer` (research, chưa xác minh tuyệt đối).
- Dedup trong transaction chỉ bảo vệ effect nằm trong cùng database.
  Với cổng thanh toán thật (HTTP ra ngoài) effect không nằm trong transaction, nên lời gọi phải mang idempotency key ổn định (research: header `Idempotency-Key` của Stripe, lưu kết quả của request đầu theo key) và cần thêm trạng thái "đang xử lý" như lab 08-01.
  Use case giả lập cổng thanh toán bằng luật amount vượt hạn mức, nên phần này chưa được làm.
- Thứ tự chỉ có trong một key.
  Event của một order nằm trong một partition và đúng thứ tự, nhưng giữa các order không có thứ tự toàn cục.
  Nhiều relay chạy song song với `SKIP LOCKED` có thể đảo thứ tự publish (lab 08-05 đo điều này), nên use case chạy một relay.
  Một relay là điều kiện cần, chưa đủ: hai transaction commit đảo thứ tự id vẫn có thể làm id nhỏ được publish sau id lớn (chương 08).
  Thứ tự mà use case này cần đến từ chỗ khác: cùng key thì cùng partition, và event follow-up (ví dụ `PaymentFailed`) chỉ được ghi sau khi event trước đó đã được publish và consumer đã xử lý nó, nên nó luôn nằm sau trong partition (quan hệ nhân quả).
  Muốn nhiều relay thì phải claim theo aggregate (chưa xác minh bằng nguồn riêng, suy luận thiết kế) hoặc chấp nhận event của cùng order tới đảo thứ tự và để consumer chịu được.
- Transaction và row lock của relay mở suốt thời gian publish (xem lab 08-05).
  Relay chậm hoặc batch lớn giữ lock lâu, đổi lại không có trạng thái "đã nhận nhưng chưa gửi" nào cần dọn.
- Compensation cũng có thể lỗi.
  Ở đây compensation là một event handler: nếu transaction hoàn kho lỗi thì offset không được commit và nó được thử lại mãi (backoff 200 ms), nên một lỗi vĩnh viễn (ví dụ database hỏng) chặn partition đó và làm lag tăng.
  Đó chính là tín hiệu lag dùng để cảnh báo, nhưng use case không có ngưỡng thử lại tối đa hay đường escalate cho người vận hành.
  Chương 08, lab 06 mô tả cách báo compensation lỗi và vì sao compensation phải idempotent.
- Hết hàng chưa được xử lý trọn vẹn.
  `inventory` ghi `rejected`, hủy order và phát `InventoryRejected`, nhưng `payment` chạy song song nên vẫn charge cho order bị hủy, và use case không có bước hoàn tiền.
  Có hai thứ tự xen kẽ và cả hai đều kết thúc ở `cancelled`: nếu `inventory` chạy trước thì order đi `placed, cancelled` rồi `payment` vẫn ghi payment `charged` (guard trạng thái không đổi được order đã hủy), còn nếu `payment` chạy trước thì order đi `placed, paid, cancelled` vì `inventory` hủy được từ cả `placed` lẫn `paid`.
  Hậu quả giống nhau: một payment `charged` và một `PaymentCompleted` cho order đã hủy, không ai hoàn tiền.
  Muốn đóng chỗ này thì `payment` phải chờ `InventoryReserved` (chuỗi tuần tự) hoặc có compensation hoàn tiền (bài tập 3).
- Số lượng tồn kho là một bộ đếm theo `sku`, không có đặt chỗ hết hạn: reservation `reserved` không bao giờ tự hết hạn.
- `POST /orders` không có idempotency key: client gửi lại cùng một order không có `id` sẽ tạo order thứ hai.
  Client muốn an toàn khi gửi lại phải tự đặt `id` và chấp nhận 409 ở lần gửi lại.
- Gauge lag mở một admin client mỗi lần scrape (TypeScript) hoặc một client kafka-go mới (Go), nên scrape chậm hơn một chút và không phù hợp với chu kỳ scrape rất ngắn.
  Production nên dùng một exporter chuyên dụng hoặc giữ admin client lâu dài.
- Hook `afterDedupInsert` và `crashBeforeCommit` mô phỏng lỗi trong cùng process, không phải kill process consumer thật.
  Chỉ relay được kill bằng SIGKILL thật (chaos test).
- Poison event không có DLQ topic: nội dung hỏng chỉ nằm trong log, nên không replay được sau khi sửa.
- Các bảng của ba "service" nằm chung một database để demo gọn.
  Trong hệ thống thật mỗi service có database riêng, và khi đó dòng dedup phải nằm trong database của chính consumer (điều use case đã làm với `processed_events`).

## Bài tập mở rộng

1. Thêm `quantity` tối đa theo `sku` vào hợp đồng HTTP và trả 422 khi vượt, rồi viết test cho cả hai ngôn ngữ.
2. Thêm một DLQ topic cho poison event: producer gửi giá trị hỏng cùng offset, partition và lý do, rồi viết test replay sau khi sửa parser.
3. Thêm compensation hoàn tiền: `payment` nhận `InventoryRejected` và hoàn tiền nếu order đã `paid`, với dedup và guard trạng thái như `inventory`.
4. Đổi saga thành orchestrator (một consumer `order-saga` giữ trạng thái và gửi lệnh), rồi so sánh số event và độ dễ đọc với bản choreography.
5. Thêm ngưỡng thử lại tối đa cho handler lỗi, sau đó chuyển event vào DLQ và đếm bằng một metric riêng.
6. Chạy hai relay song song với `SKIP LOCKED` và đo thứ tự event của cùng một order trên topic, rồi thử claim theo aggregate để giữ thứ tự.
7. Thay gauge lag bằng một exporter có sẵn admin client lâu dài, và đặt quy tắc cảnh báo "lag tăng liên tục" thay cho ngưỡng cố định.

## Nguồn tham khảo

Phiên bản đã kiểm chứng ngày 2026-10-06: `@confluentinc/kafka-javascript` 1.10.1, `kafka-go` v0.4.51, Apache Kafka 4.3.1, Postgres 18.6, `@prometheus-io/client` 0.16.1, `client_golang` v1.24.1.

- Transactional outbox: relay kiểu polling publisher hoặc transaction log tailing, at-least-once nên consumer phải idempotent.
  Nguồn: https://microservices.io/patterns/data/transactional-outbox.html
- Debezium Outbox Event Router: cột `id` vào header để phát hiện trùng, `aggregateid` làm message key để giữ thứ tự theo partition.
  Nguồn: https://debezium.io/documentation/reference/stable/transformations/outbox-event-router.html
- Idempotent consumer: dedup key ổn định, ghi marker và effect cùng một transaction (Inbox), unique constraint làm trọng tài.
  Nguồn: https://learn.microsoft.com/en-us/azure/architecture/patterns/idempotent-consumer
  Nguồn: https://microservices.io/patterns/communication-style/idempotent-consumer.html
- Saga: choreography và orchestration, compensating transaction.
  Nguồn: https://microservices.io/patterns/data/saga.html
- Compensating transaction: compensation cũng có thể lỗi nên phải idempotent và retry được.
  Nguồn: https://learn.microsoft.com/en-us/azure/architecture/patterns/compensating-transaction
- Kafka delivery semantics: exactly-once chỉ trong phạm vi Kafka-to-Kafka, side effect ngoài Kafka cần idempotency.
  Nguồn: https://docs.confluent.io/kafka/design/delivery-semantics.html
- Phạm vi thật của exactly-once, transaction, `read_committed`, `sendOffsetsToTransaction`.
  Nguồn: https://www.confluent.io/blog/exactly-once-semantics-are-possible-heres-how-apache-kafka-does-it/
- Idempotency key ở mức API: Stripe lưu kết quả của request đầu theo key.
  Nguồn: https://docs.stripe.com/api/idempotent_requests
- Consumer lag: `LOG-END-OFFSET` trừ `CURRENT-OFFSET` của `kafka-consumer-groups.sh --describe`.
  Nguồn: https://kafka.apache.org/43/operations/basic-kafka-operations/
- Metric lag của consumer Java (`records-lag-max`, `records-lag`, `records-lag-avg`).
  Nguồn: https://kafka.apache.org/41/operations/monitoring/
- Burrow: đánh giá lag theo cửa sổ trượt thay vì ngưỡng cố định (cách cảnh báo là nguồn thứ cấp, chưa xác minh).
  Nguồn: https://github.com/linkedin/Burrow
- kafka-go: `Reader.Lag()` luôn trả -1 khi dùng `GroupID`, `Writer` không có API transaction hay idempotence.
  Nguồn: https://raw.githubusercontent.com/segmentio/kafka-go/v0.4.51/README.md
- Chaos testing: định nghĩa steady state, đưa vào biến cố thực tế, giữ blast radius nhỏ.
  Nguồn: https://principlesofchaos.org/
