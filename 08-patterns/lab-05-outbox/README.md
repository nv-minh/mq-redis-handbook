# Lab 05 - Transactional outbox: Postgres và RabbitMQ

## Mục tiêu

Giải bài toán "ghi DB và gửi message phải nguyên tử" mà không cần two-phase commit, và chứng minh bằng test:

- `placeOrder` ghi `orders` và `outbox` trong một transaction: hai dòng cùng `xmin`, cùng `created_at`, và nếu một câu lỗi thì không dòng nào tồn tại.
- `runRelay` chọn dòng chưa publish với `FOR UPDATE SKIP LOCKED`, publish lên RabbitMQ quorum queue (có publisher confirm), rồi đặt `published_at` và commit.
- Chạy relay lần hai trả 0: không publish lại dòng đã đánh dấu.
- Chaos: relay là process con bị `SIGKILL` sau khi publish và trước khi đánh dấu.
  Relay mới publish lại cùng event, consumer dedupe theo event id nên effect chỉ áp dụng một lần, và không dòng nào bị mất.
- Các điểm Review Focus: outbox rỗng trả 0, `batchSize` nhỏ hơn backlog chỉ xử lý `batchSize` dòng mỗi lượt, publish lỗi ở một dòng thì cả batch rollback, và hai relay chạy song song làm mất thứ tự chèn (đo được, xem bên dưới).

Lab cần Postgres và RabbitMQ chạy bằng `make up`, cộng Redis cho marker của test chaos.
Lab đọc `DATABASE_URL`, `AMQP_URL` và `REDIS_URL` (mặc định lần lượt là Postgres cổng 5432, RabbitMQ cổng 5672 và Redis cổng 6379 của `make up`).
Mỗi test tạo một Postgres schema riêng đặt tên bằng `uniqueName` (pool có `search_path` trỏ vào schema đó) và xóa bằng `DROP SCHEMA ... CASCADE` khi kết thúc, kể cả khi test fail.
Queue của RabbitMQ và marker key của Redis cũng có tên duy nhất và bị xóa.
Test chaos không dừng container nào, chỉ kill một process con của chính test.
Lý thuyết của cả chương nằm ở [theory.md](../theory.md) của chủ đề 08.

## Kiến trúc

```mermaid
flowchart LR
    A[placeOrder] -->|"một transaction: INSERT orders + INSERT outbox"| PG[("Postgres")]
    PG -->|"SELECT ... WHERE published_at IS NULL<br/>ORDER BY id LIMIT n FOR UPDATE SKIP LOCKED"| R[Relay]
    R -->|"publish + publisher confirm"| Q[("RabbitMQ quorum queue")]
    R -->|"UPDATE published_at, COMMIT"| PG
    Q --> C["Consumer<br/>dedupe theo event id"]
```

Order và event nằm cùng một transaction nên "message được gửi nếu và chỉ nếu transaction DB commit".
Relay là bên duy nhất nói chuyện với broker.

Đường lỗi (chaos): relay chết giữa publish và đánh dấu.

```mermaid
sequenceDiagram
    participant T as Test (process cha)
    participant C as Relay A (process con)
    participant PG as Postgres
    participant MQ as RabbitMQ
    participant K as Consumer (dedupe theo id)
    participant B as Relay B (trong process cha)
    T->>PG: placeOrder o-1, o-2 (hai dòng outbox)
    C->>PG: BEGIN, SELECT ... FOR UPDATE SKIP LOCKED (khóa dòng 1 và 2)
    C->>MQ: publish event 1 (có confirm)
    MQ-->>C: confirm
    MQ->>K: giao event 1 (lần 1)
    K->>K: áp dụng effect, ghi nhận id 1
    C->>T: SET marker (Redis), rồi treo trước UPDATE
    T->>C: SIGKILL
    Note over PG: connection chết, Postgres ROLLBACK và nhả lock, dòng 1 và 2 vẫn chưa publish
    B->>PG: BEGIN, SELECT ... FOR UPDATE SKIP LOCKED (lấy dòng 1 và 2)
    B->>MQ: publish event 1 rồi event 2
    MQ->>K: giao event 1 (lần 2)
    K->>K: id 1 đã thấy, bỏ qua effect
    MQ->>K: giao event 2 (lần 1)
    B->>PG: UPDATE published_at, COMMIT
    Note over PG: không dòng nào bị mất, event 1 được giao hai lần
```

## Giao diện

TypeScript (`ts/lab.ts`):

```ts
placeOrder(pool, { id, amount }): Promise<void>;
runRelay(pool, publish: (e: OutboxEvent) => Promise<void>, { batchSize }): Promise<number>;
// OutboxEvent = { id: string; aggregateId: string; type: string; payload: unknown }
```

Go (`go/lab.go`): `PlaceOrder(ctx, pool, Order{ID, Amount})` và `RunRelay(ctx, pool, publish, batchSize) (int, error)` với `OutboxEvent{ID int64, AggregateID, Type, Payload json.RawMessage}`.
Tên field giống nhau ở hai ngôn ngữ.
`id` là string ở TypeScript vì `pg` trả `bigint` dưới dạng string, và là `int64` ở Go.
Trên dây (thuộc tính `messageId` của AMQP) cả hai đều là chuỗi thập phân.

Các hàm phụ trợ dùng chung cho test và demo: `createSchema`, `dropSchema`, `openPool`, `createTables` (Go viết hoa chữ đầu), `declareQueue`, `amqpPublisher` (Go: `NewPublisher`), `startConsumer`.

Schema:

```sql
create table orders (id text primary key, amount numeric not null, created_at timestamptz not null default now());
create table outbox (
  id bigserial primary key, aggregate_id text not null, type text not null, payload jsonb not null,
  created_at timestamptz not null default now(), published_at timestamptz
);
create index outbox_unpublished_idx on outbox (id) where published_at is null;
```

Partial index chỉ chứa dòng chưa publish, nên nó nhỏ và không phình theo lịch sử event đã gửi.

## Các điểm quan trọng

Thiết kế transaction của relay.
Trong MỘT transaction: chọn tối đa `batchSize` dòng chưa publish theo id với `FOR UPDATE SKIP LOCKED`, gọi `publish` cho từng dòng, đặt `published_at` cho cả batch rồi commit.
Nếu process chết sau publish và trước commit, Postgres rollback transaction và nhả lock, nên dòng quay lại trạng thái chưa publish và lần sau được publish lại.
Đó là at-least-once theo thiết kế, không phải bug: consumer phải dedupe theo event id (cột `id` của outbox).

Publish lỗi ở một dòng.
Cả batch nằm trong một transaction nên khi `publish` ném lỗi, relay ROLLBACK và ném lại lỗi đó.
Không dòng nào của batch được đánh dấu, kể cả những dòng đã publish thành công trước dòng lỗi, và chúng sẽ được publish lại ở lượt sau.
Test `publish_failure_rolls_back_the_whole_batch_and_propagates_the_error` ghi lại đúng điều này: event đầu xuất hiện hai lần trong danh sách gọi `publish`.
Đây là hành vi đã chọn, không phải sai sót: duplicate được chấp nhận, mất message thì không.

Đánh đổi của thiết kế này (suy luận từ code, không phải tuyên bố của nguồn nào): transaction và các row lock mở suốt thời gian publish.
Một lượt publish chậm giữ một connection Postgres và khóa các dòng của batch lâu, và `batchSize` lớn làm cửa sổ này dài hơn.
Hai thiết kế thay thế (đều là suy luận thiết kế, không nằm trong tài liệu nghiên cứu của handbook):

- Claim column: transaction ngắn đánh dấu dòng là "đã nhận" (ví dụ cột `claimed_by` và `claimed_at`) rồi commit, publish ngoài transaction, rồi đánh dấu `published_at`.
  Không giữ lock lâu, nhưng phải xử lý claim bị bỏ rơi khi relay chết giữa chừng (cần timeout để nhận lại), nên phức tạp hơn.
- CDC (transaction log tailing): đọc thẳng transaction log của DB thay vì truy vấn bảng định kỳ.
  Microservices.io nêu đây là kiểu relay thứ hai bên cạnh polling publisher, và Debezium có Outbox Event Router: cột `id` được đặt vào header để consumer phát hiện trùng, `aggregateid` làm message key để giữ thứ tự theo partition, và chỉ INSERT được xử lý.
  Lab này không dùng CDC.

Thứ tự.
Một relay duy nhất publish theo thứ tự id, và thứ tự id trùng thứ tự chèn khi các transaction commit theo đúng thứ tự id: test `relay_publishes_each_row_and_marks_it_published` kiểm tra consumer nhận đúng thứ tự đó trong trường hợp này.
Giới hạn: `bigserial` cấp id lúc INSERT nhưng dòng chỉ hiện ra lúc COMMIT, nên nếu hai transaction commit đảo thứ tự thì ngay cả một relay vẫn có thể publish id nhỏ sau id lớn.
Lab không đo trường hợp này, đó là suy luận từ cách `bigserial` và transaction hoạt động.
Với nhiều relay chạy song song, research ghi "thứ tự bị mất trừ khi chỉ có một relay hoặc claim theo aggregate" là chưa xác minh bằng nguồn riêng.
Lab này đo nó bằng test `several_relays_with_skip_locked_do_not_preserve_insertion_order` và kết quả xác nhận suy luận đó cho thiết kế này.
Relay A khóa dòng 1 và 2 rồi đứng chờ ở event đầu, relay B chạy trọn vẹn trong lúc đó: `SKIP LOCKED` bỏ qua dòng 1 và 2 nên B publish dòng 3 và 4 trước.
Thứ tự publish quan sát được là 3, 4, 1, 2.
Test dùng cổng (gate) chứ không dùng thời gian nên kết quả này xác định, không phải may rủi.
Kết luận chỉ áp dụng cho thiết kế này: muốn giữ thứ tự theo entity thì dùng một relay, hoặc partition theo `aggregate_id` (claim theo aggregate), hoặc để broker giữ thứ tự theo key như Kafka.

Event id và dedupe.
Event id đi vào thuộc tính `messageId` của AMQP, đúng ý Debezium là đặt `id` vào header để consumer phát hiện trùng.
Consumer của test dedupe bằng một `Set` trong bộ nhớ: đủ để chứng minh pattern.
Production cần dedupe bền vững (xem lab 01 với Redis `SET NX EX`, hoặc bảng inbox trong cùng transaction với effect).

Publisher confirm.
Hàm publish của lab chờ `basic.ack` của broker cho từng message trước khi sang message kế tiếp.
Với message persistent vào quorum queue, confirm nghĩa là broker đã nhận và ghi.
Confirm không chứng minh message đã vào queue nếu message không route được (publish qua default exchange tới queue không tồn tại vẫn được confirm), nên queue phải được khai báo trước khi relay chạy.

Test rollback ép lỗi bằng một check constraint thêm vào bảng `outbox` của schema riêng của test.
Câu INSERT order chạy xong trước, rồi câu INSERT outbox bị từ chối, nên test chứng minh transaction thực sự hoàn tác phần đã ghi.

Test chaos chờ bằng `eventually` chứ không giả định lock đã nhả ngay.
Khi relay con chết, connection đóng, Postgres rollback và nhả lock; trong lúc lock còn giữ, `SKIP LOCKED` trả 0 dòng nên `runRelay` trả 0 mà không báo lỗi.
Đo một lần (không phải đảm bảo): trong 5 lần chạy TypeScript, lượt `runRelay` đầu tiên sau `SIGKILL` đã thấy lock được nhả.

## Giới hạn thật của pattern

- At-least-once, không phải exactly-once: event có thể được publish nhiều lần, consumer phải idempotent.
- Outbox chuyển độ trễ vào polling: event chỉ đi khi relay chạy, và lab không có vòng lặp polling định kỳ (caller gọi `runRelay`).
- Giữ transaction mở khi publish (đã nêu ở trên) giới hạn throughput và số relay có ích.
- Nhiều relay không giữ thứ tự chèn (đã đo ở trên), và một relay chỉ giữ thứ tự khi commit theo đúng thứ tự id.
- Bảng `outbox` lớn dần: lab không có job dọn dòng đã publish.
  Partial index giữ truy vấn của relay nhanh, nhưng bảng vẫn cần được dọn hoặc partition.
- Consumer in-memory của test không sống qua restart: dedupe production phải bền vững.
- Test chaos chỉ giết relay, không giết RabbitMQ hay Postgres, cũng không cắt mạng: xem phần chaos trong theory.

## Cách chạy process con trong test chaos

TypeScript: test `spawn(process.execPath, ["--import", "tsx", ts/chaos-worker.ts], { cwd: <repo root> })`.
Process con chạy `runRelay` thật với callback publish (publish có confirm, ghi marker vào Redis, rồi treo), test chờ marker bằng `eventually` (30 giây, ngắn hơn timeout 90 giây của test để child lỗi lúc khởi động cho thông báo dễ đọc) rồi `SIGKILL`.

Go: test chạy lại chính test binary (`os.Args[0]` với `-test.run=^TestHelperProcess$` và biến môi trường `LAB05_HELPER=1`).
`TestHelperProcess` tự bỏ qua khi không có biến môi trường đó.

Chống process mồ côi: `afterEach` (TypeScript) hoặc `t.Cleanup` (Go) luôn `SIGKILL` và chờ child thoát kể cả khi test fail, và child có thời hạn cứng 60 giây tự thoát.
Stderr của child được ghi lại và in ra khi test fail, và nếu child thoát trước khi báo marker (ví dụ lỗi lúc khởi động) thì test fail ngay thay vì chờ hết thời gian chờ marker.
Thứ tự dọn có chủ đích: child chết trước (nhả lock), rồi pool đóng, rồi mới `DROP SCHEMA`.
Kiểm tra sau khi chạy: `pgrep -fl "chaos-worker|TestHelperProcess"` không in gì.

## Chạy

```bash
make up
export DATABASE_URL=postgres://handbook:handbook@127.0.0.1:5432/handbook   # đổi cổng nếu POSTGRES_PORT khác 5432
pnpm vitest run 08-patterns/lab-05-outbox/ts
go test -race ./08-patterns/lab-05-outbox/go/...
make lab-ts LAB=08-patterns/lab-05-outbox
make lab-go LAB=08-patterns/lab-05-outbox
```

## Kết quả mong đợi

Test (cùng tên ở TS và Go, Go dùng CamelCase):

| Test                                                                           | Chứng minh                                                                                                                 |
| ------------------------------------------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------- |
| `order_and_outbox_row_commit_atomically`                                       | Sau `placeOrder` có đúng một order và một dòng outbox, cùng `xmin` và `created_at`                                         |
| `rollback_leaves_no_outbox_row`                                                | Lỗi ở câu INSERT outbox (sau INSERT order): không có order, không có dòng outbox, pool vẫn dùng được                       |
| `relay_publishes_each_row_and_marks_it_published`                              | Quorum queue có confirm: consumer nhận mỗi event một lần đúng thứ tự, `published_at` được đặt, lần chạy thứ hai trả 0      |
| `empty_outbox_relay_returns_zero_without_publishing`                           | Outbox rỗng: trả 0, không gọi `publish`, không lỗi                                                                         |
| `batch_size_limits_the_rows_published_per_call`                                | Backlog 5 với `batchSize` 2 cho các lượt 2, 2, 1, 0 theo thứ tự id                                                         |
| `publish_failure_rolls_back_the_whole_batch_and_propagates_the_error`          | Publish lỗi ở dòng thứ hai: lỗi được ném ra, không dòng nào được đánh dấu, lượt sau publish lại cả ba                      |
| `several_relays_with_skip_locked_do_not_preserve_insertion_order`              | Hai relay song song publish theo thứ tự 3, 4, 1, 2: không giữ thứ tự chèn                                                  |
| `relay_rejects_a_non_positive_batch_size`                                      | `batchSize` bằng 0, âm hoặc không nguyên bị từ chối                                                                        |
| `chaos_relay_crash_after_publish_before_mark_republishes_and_consumer_dedupes` | Relay là process con bị `SIGKILL` sau publish: event được giao ít nhất hai lần, effect áp dụng một lần, không dòng nào mất |

Demo in ba bước: đặt 3 order, mô phỏng relay chết sau publish event đầu (callback publish rồi ném lỗi, nên transaction rollback), rồi chạy lại relay với `batchSize` 2.
Demo không chạy process con: nó mô phỏng crash bằng lỗi trong callback.
Kết quả: event 1 được giao hai lần nhưng effect chỉ áp dụng một lần cho mỗi event.

## Bài tập mở rộng

1. Thêm vòng lặp polling định kỳ cho relay, với backoff khi outbox rỗng, và dừng sạch bằng `AbortSignal` hoặc `context`.
2. Cài claim column và so sánh: transaction ngắn hơn, nhưng cần xử lý claim bị bỏ rơi.
3. Cho hai relay chạy song song trên các `aggregate_id` khác nhau (ví dụ chia theo hash của `aggregate_id`) và kiểm tra thứ tự theo từng aggregate được giữ.
4. Thêm bảng inbox cho consumer: ghi dedupe marker và effect trong cùng một transaction.
5. Thêm job dọn dòng đã publish quá một ngày và đo kích thước bảng.

## Nguồn tham khảo

Phiên bản: PostgreSQL 18.6, RabbitMQ 4.3.6, `pg` 8.23.1, `pgx` v5.11.0, amqplib 2.2.0, amqp091-go v1.15.0 (kiểm tra ngày 2026-10-06).

- Transactional outbox: ghi message vào bảng outbox trong cùng transaction với thay đổi business, relay đọc và publish (polling publisher hoặc transaction log tailing), at-least-once nên consumer phải idempotent.
  Nguồn: https://microservices.io/patterns/data/transactional-outbox.html
- Debezium Outbox Event Router: `id` vào header để phát hiện trùng, `aggregateid` làm message key, chỉ xử lý INSERT.
  Nguồn: https://debezium.io/documentation/reference/stable/transformations/outbox-event-router.html
- Publisher confirms của RabbitMQ: confirm của message persistent vào durable queue nghĩa là đã ghi đĩa, và message không route được vẫn được confirm.
  Nguồn: https://www.rabbitmq.com/docs/confirms
- Idempotent consumer và inbox: dedupe theo id ổn định, ghi marker và effect trong cùng transaction.
  Nguồn: https://learn.microsoft.com/en-us/azure/architecture/patterns/idempotent-consumer
- Chaos testing: kill relay giữa publish và đánh dấu, giữ blast radius nhỏ.
  Nguồn: https://principlesofchaos.org/
