# Ghi chú nghiên cứu: patterns và production (chương 01, 08, 09)

## Phiên bản

Kiểm tra ngày 2026-10-06 qua npm registry, Go module proxy và GitHub releases.

| Thành phần | Phiên bản | Ngày phát hành | Ghi chú |
|---|---|---|---|
| bullmq (npm) | 6.3.11 | 2026-10-01 | Khớp phiên bản yêu cầu; engines node >= 14.17.0 |
| asynq (Go) | v0.26.0 | 2026-02-03 | Khớp; go.mod yêu cầu go 1.24.0, phụ thuộc go-redis v9.14.1 |
| ws (npm) | 8.22.0 | chưa ghi | engines node >= 10 |
| github.com/coder/websocket | v1.8.15 | 2026-06-15 | go 1.23 |
| ioredis (npm) | 6.0.0 | 2026-07-31 | engines node >= 20 |
| redis (node-redis, npm) | 6.3.0 | 2026-09-30 | BullMQ 6 cũng chấp nhận (peer `redis >= 5.0.0`) |
| github.com/redis/go-redis/v9 | v9.23.0 | 2026-10-05 | |
| golang.org/x/sync | v0.23.0 | 2026-08-31 | chứa `singleflight` |
| Redis server | 8.10.2 (nhánh 8.x mới nhất trên GitHub releases) | 2026-09-17 | `DELEX ... IFEQ` và `SET ... IFEQ` từ 8.4.0; `XNACK` từ 8.8.0 |

Thay đổi đáng chú ý:
- BullMQ 6.0.0 (2026-07-30): bỏ legacy repeatable jobs (dùng Job Schedulers), bỏ `debounce` (dùng deduplication), bỏ `Job#discard()` (dùng `UnrecoverableError`), bỏ trạng thái `paused` khỏi JobType, `ioredis` thành optional peer dependency nên phải tự `npm i ioredis`, thêm backend PostgreSQL.
- Lab TS với BullMQ 6 phải cài `ioredis` rõ ràng và không dùng API repeatable cũ; các bài hướng dẫn BullMQ cũ trên mạng (QueueScheduler, `repeat` option, `debounce`) là outdated.
- asynq v0.26.0: thêm Headers cho task, `UpdateTaskPayload` cho inspector; asynq ít phát hành (bản trước là 0.25.1 vào 2024-12) nên cần cân nhắc khi chọn cho dự án mới.
- Redis 8.4 thêm `DELEX key IFEQ value` và `SET ... IFEQ`, cho phép release lock không cần Lua; Redis docs vẫn đưa script Lua cho phiên bản cũ hơn.
- Redis `XNACK` chỉ có từ 8.8.0, `XAUTOCLAIM` từ 6.2.0; Redis Pub/Sub sharded từ 7.0.
- coder/websocket: do Coder duy trì từ 2024, trước đó là `nhooyr.io/websocket`; import path hiện tại là `github.com/coder/websocket`.
- Trang `SET` của Redis ghi mẫu `SET NX EX` làm lock là "discouraged in favor of Redlock" - chú ý mâu thuẫn quan điểm với Kleppmann, xem mục distributed lock.
Nguồn: https://registry.npmjs.org/bullmq, https://proxy.golang.org/github.com/hibiken/asynq/@latest, https://proxy.golang.org/github.com/coder/websocket/@latest, https://github.com/taskforcesh/bullmq/releases/tag/v6.0.0, https://github.com/hibiken/asynq/blob/v0.26.0/CHANGELOG.md

## Khái niệm bắt buộc

### Chương 01

**Vì sao dùng message queue (load leveling, decoupling)**
Queue đặt giữa task và service đóng vai trò buffer, nên service xử lý theo nhịp của nó và producer không bị chậm khi service tạm không sẵn sàng.
Nếu tốc độ trung bình của producer lớn hơn consumer thì queue tăng dần và latency tăng, nên phải theo dõi queue depth, scale consumer trong giới hạn an toàn hoặc shed work ở producer.
Autoscale consumer mà không giới hạn tốc độ downstream chỉ chuyển quá tải sang dependency phía sau.
Nguồn: https://learn.microsoft.com/en-us/azure/architecture/patterns/queue-based-load-leveling

**Queue (competing consumers) khác pub/sub**
Trong competing consumers, mỗi message chỉ một consumer nhận; trong publisher-subscriber, mọi subscriber nhận mọi message.
Nguồn: https://learn.microsoft.com/en-us/azure/architecture/patterns/competing-consumers

**Queue vs pub/sub vs log**
Redis Pub/Sub là at-most-once: message đẩy đi một lần, nếu subscriber lỗi hoặc rớt kết nối thì message mất vĩnh viễn, không có history.
Redis docs khuyên dùng Streams khi cần đảm bảo mạnh hơn, vì message trong stream được persist và hỗ trợ cả at-most-once lẫn at-least-once.
Nguồn: https://redis.io/docs/latest/develop/pubsub/
Kafka tổng quát hoá queue và pub/sub bằng consumer group: mỗi partition chỉ được một consumer trong một group đọc tại một thời điểm, nhiều group cùng đọc một topic cho ra hành vi pub/sub.
Log là lưu trữ append-only có offset, consumer tự giữ vị trí đọc và có thể đọc lại, khác với queue xoá message sau khi ack (khẳng định này suy ra từ mô hình offset của Kafka design doc).
Nguồn: https://kafka.apache.org/41/design/design/

**Delivery semantics**
At-most-once: message có thể mất nhưng không bao giờ redeliver.
At-least-once: message không mất nhưng có thể bị giao nhiều lần; đây là mặc định của Kafka và là mức mà hầu hết queue cung cấp.
Exactly-once (định nghĩa lý tưởng): mỗi message được giao đúng một lần, không mất, không đọc hai lần dù có lỗi.
Nguồn: https://docs.confluent.io/kafka/design/delivery-semantics.html
Redis Streams cho at-least-once: message nằm trong Pending Entries List cho tới khi XACK, consumer chết trước XACK thì message vẫn trong PEL và consumer khác có thể XCLAIM/XAUTOCLAIM.
Nguồn: https://redis.io/docs/latest/develop/data-types/streams/

**Exactly-once thật sự nghĩa là gì, và effectively-once cần gì**
Không thể có exactly-once delivery end to end qua mạng không tin cậy: producer không phân biệt được "message chưa tới", "ack bị mất" và "consumer chết sau khi xử lý", nên phải chọn gửi lại (có thể trùng) hoặc không gửi lại (có thể mất).
Cách hệ thống thực tế "giả lập" exactly-once là at-least-once delivery cộng với xử lý idempotent hoặc dedup, còn gọi là effectively-once hoặc exactly-once processing.
Nguồn: https://bravenewgeek.com/you-cannot-have-exactly-once-delivery/
Phạm vi exactly-once của Kafka là Kafka-to-Kafka: idempotent producer (từ 0.11.0.0, dedup bằng producer ID và sequence number) và transactions ghi nhiều partition atomically.
Khi ghi ra hệ thống ngoài, Kafka khuyên lưu offset cùng chỗ với output (cùng một transaction) thay vì two-phase commit.
Nguồn: https://kafka.apache.org/41/design/design/ và https://docs.confluent.io/kafka/design/delivery-semantics.html
Kết luận cho handbook: effectively-once cần (1) at-least-once delivery (ack sau khi xử lý xong), (2) consumer idempotent hoặc dedup bằng key ổn định sinh ở phía producer, (3) cập nhật dedup record và side effect trong cùng một transaction nếu có thể.

**Ordering**
Với competing consumers, thứ tự nhận message không được đảm bảo và không nhất thiết phản ánh thứ tự tạo; nên thiết kế consumer idempotent để loại bỏ phụ thuộc thứ tự.
Muốn strict ordering phải dùng cơ chế như message sessions (Azure Service Bus), tức là gom message của cùng một key về một consumer.
Nguồn: https://learn.microsoft.com/en-us/azure/architecture/patterns/competing-consumers
Kafka giữ thứ tự theo partition, không giữ thứ tự giữa các partition; vì vậy key hoá message (cùng key vào cùng partition) là cách giữ thứ tự theo entity.
Nguồn: https://kafka.apache.org/41/design/design/
Queue có nhiều consumer song song cũng không bảo toàn thứ tự gốc trong mọi điều kiện.
Nguồn: https://learn.microsoft.com/en-us/azure/architecture/patterns/queue-based-load-leveling

**Backpressure và bounded queue**
Kafka dùng mô hình pull: consumer tự kéo dữ liệu theo nhịp của mình; push-based khó xử lý consumer đa dạng vì broker quyết định tốc độ.
Nguồn: https://kafka.apache.org/41/design/design/
RabbitMQ dùng prefetch (QoS) để giới hạn số message chưa ack đang bay tới một consumer; docs nêu giá trị 100-300 thường cho throughput tối ưu, 0 là không giới hạn.
Nguồn: https://www.rabbitmq.com/docs/confirms
Redis Streams giới hạn kích thước bằng XADD MAXLEN (có dạng xấp xỉ `~`) hoặc XTRIM; đây là bounded stream, cần nhớ rằng trim có thể xoá message chưa được xử lý.
Nguồn: https://redis.io/docs/latest/develop/data-types/streams/
Queue không giới hạn chỉ che giấu quá tải; khi producer nhanh hơn consumer thì phải shed work hoặc giới hạn ở producer (xem load leveling ở trên).

**Acknowledgements**
RabbitMQ có hai chế độ: automatic (coi là đã giao ngay khi gửi, mất message nếu consumer rớt trước khi xử lý) và manual (`basic.ack`, `basic.nack`, `basic.reject`).
Delivery chưa ack sẽ tự động requeue khi channel đóng, và message redeliver mang cờ `redeliver = true`; `requeue=false` đẩy message tới Dead Letter Exchange hoặc bỏ.
Publisher confirms là cơ chế độc lập với consumer ack; với message persistent trên durable queue, confirm nghĩa là đã ghi đĩa.
Nguồn: https://www.rabbitmq.com/docs/confirms
Ghi chú: lệnh `XNACK` (nhả message về lại group không cần ack) có trang riêng, ghi "since 8.8.0", nên chỉ dùng trong lab khi server là Redis 8.8 trở lên.
Nguồn: https://redis.io/docs/latest/commands/xnack/

### Chương 08

**Idempotent consumer (khái niệm và dedup key)**
Hầu hết broker (Service Bus, Event Hubs, Kafka, RabbitMQ) cung cấp at-least-once; exactly-once delivery trên hệ thống phân tán là không thực tế, và ngay cả broker có exactly-once cũng chỉ đảm bảo các thao tác nó kiểm soát, không kiểm soát side effect ở hệ thống ngoài.
At-least-once cộng consumer bỏ qua trùng lặp cho ra effectively-once processing.
Dedup key phải ổn định qua mọi lần redeliver: dùng message ID do producer gán hoặc business idempotency key; không dùng ID transport do broker sinh lại, không dùng timestamp nhận, không dùng correlation ID dùng chung.
Khi nhiều consumer độc lập cùng đọc một channel (pub/sub), key dedup phải gồm cả consumer identity và message identity.
Nguồn: https://learn.microsoft.com/en-us/azure/architecture/patterns/idempotent-consumer

**Idempotent consumer: Inbox và atomicity**
Cách chuẩn: ghi dedup marker và side effect trong cùng một DB transaction (Inbox pattern, bạn đồng hành phía consumer của Transactional Outbox); nếu ghi marker ở bước riêng thì crash giữa hai bước làm side effect đã áp dụng nhưng marker chưa có, và lần redeliver sau sẽ xử lý lại.
Dùng unique constraint của DB làm trọng tài cho concurrent duplicates; trên cache phải dùng atomic set-if-absent thay vì check rồi set riêng.
Nguồn: https://learn.microsoft.com/en-us/azure/architecture/patterns/idempotent-consumer và https://microservices.io/patterns/communication-style/idempotent-consumer.html

**Idempotent consumer với Redis `SET key value NX EX`**
`SET key value NX EX seconds` chỉ set khi key chưa tồn tại, trả `OK` nếu set được và nil nếu key đã có; mọi TTL cũ bị bỏ khi SET thành công ghi đè (trừ khi dùng KEEPTTL).
Nguồn: https://redis.io/docs/latest/commands/set/
Hai pha cho side effect không nằm trong transaction (gọi API ngoài): ghi key trạng thái in-progress, thực hiện effect, rồi cập nhật thành completed kèm outcome; redeliver gặp in-progress phải reconcile hoặc đưa ra can thiệp thủ công.
Nguồn: https://learn.microsoft.com/en-us/azure/architecture/patterns/idempotent-consumer
Hệ quả khi effect thất bại: nếu chiếm key bằng SET NX trước rồi effect lỗi, key đã tồn tại sẽ khiến lần retry bị coi là trùng và bỏ qua effect (mất việc); vì vậy hoặc xoá key khi effect lỗi, hoặc dùng trạng thái in-progress với TTL ngắn rồi đổi thành completed với TTL dài.
Đây là suy luận từ hai pha ở trên, handbook nên có lab minh hoạ.
TTL của dedup key phải lớn hơn cửa sổ mà broker còn có thể redeliver (max delivery attempts, visibility/lock timeout, message TTL) và cả message do operator replay từ DLQ; xoá sớm sẽ mở lại cửa sổ trùng lặp.
Nguồn: https://learn.microsoft.com/en-us/azure/architecture/patterns/idempotent-consumer
Ví dụ API-level: Stripe lưu status code và body của request đầu tiên theo idempotency key (kể cả lỗi 500), key có thể bị xoá sau tối thiểu 24 giờ, và so sánh tham số để báo lỗi nếu cùng key mà tham số khác.
Nguồn: https://docs.stripe.com/api/idempotent_requests
Ưu tiên operation idempotent tự nhiên (upsert theo business key, ghi giá trị tuyệt đối thay vì tăng) trước khi dùng dedup.
Nguồn: https://learn.microsoft.com/en-us/azure/architecture/patterns/idempotent-consumer

**Retry với exponential backoff và jitter (AWS)**
Công thức Full Jitter: `sleep = random_between(0, min(cap, base * 2 ** attempt))`.
Equal Jitter: `temp = min(cap, base * 2 ** attempt); sleep = temp/2 + random_between(0, temp/2)`; Decorrelated Jitter: `sleep = min(cap, random_between(base, sleep * 3))` (code simulator của AWS dùng cận dưới `base`; trang blog viết rút gọn).
Nguồn code simulator: https://github.com/aws-samples/aws-arch-backoff-simulator
Phân tích của AWS: tổng công việc của hệ thống khi N client tranh chấp tăng theo N bình phương; jitter giảm đáng kể công việc nhưng không đổi bản chất đó; Full Jitter tốn ít công việc hơn các biến thể còn lại, và không jitter là tệ nhất; kết luận "jittered backoff is huge, and it should be considered a standard approach for remote clients".
Nguồn: https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/

**Dead letter queue và replay**
DLQ cô lập message không xử lý được để debug; SQS dùng redrive policy với `maxReceiveCount` (số lần nhận trước khi chuyển sang DLQ), nên đặt đủ cao để cho phép retry, và cảnh báo (alarm) khi có message vào DLQ.
Retention của DLQ nên dài hơn queue gốc vì timestamp enqueue gốc không đổi khi message chuyển vào DLQ (với standard queue).
SQS hỗ trợ redrive để đưa message từ DLQ trở lại source queue; không dùng DLQ với FIFO queue nếu cần giữ thứ tự tuyệt đối.
Nguồn: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-dead-letter-queues.html
Poison message (malformed, tài nguyên không có) phải bị chặn không quay lại queue mãi mãi; lưu chi tiết để phân tích, và monitor độ sâu DLQ rồi resubmit khi đã sửa nguyên nhân.
Nguồn: https://learn.microsoft.com/en-us/azure/architecture/patterns/competing-consumers và https://learn.microsoft.com/en-us/azure/architecture/patterns/queue-based-load-leveling
Replay từ DLQ phải đi qua consumer idempotent vì có thể xảy ra rất lâu sau cửa sổ redelivery thông thường.
Nguồn: https://learn.microsoft.com/en-us/azure/architecture/patterns/idempotent-consumer

**Competing consumers trên Redis Streams**
Consumer group: mỗi message trong stream chỉ giao cho một consumer của group; `XREADGROUP ... >` nhận message mới chưa giao; sau xử lý `XACK` để rút khỏi Pending Entries List.
`XPENDING` cho thấy message id, consumer, idle time và delivery count (dùng để phát hiện poison message); `XAUTOCLAIM` (Redis 6.2+) chuyển quyền sở hữu message idle quá ngưỡng sang consumer khác để phục hồi khi consumer chết.
Nguồn: https://redis.io/docs/latest/develop/data-types/streams/
Redis Streams không có DLQ sẵn (suy ra từ danh sách lệnh trong docs): tự triển khai bằng cách đọc delivery count từ XPENDING, quá ngưỡng thì XADD sang stream `*:dlq` rồi XACK.

**Transactional outbox**
Vấn đề: cập nhật DB và gửi message lên broker phải nguyên tử mà 2PC thường không khả dụng; giải pháp: ghi message vào bảng outbox trong cùng transaction với thay đổi business, một relay riêng đọc và publish.
Hai kiểu relay: polling publisher (truy vấn định kỳ bảng outbox) và transaction log tailing (CDC).
Relay có thể publish một message nhiều lần nếu crash trước khi ghi nhận hoàn tất, nên outbox cho at-least-once và consumer phải idempotent (ví dụ theo dõi ID message đã xử lý).
Message được gửi nếu và chỉ nếu transaction DB commit.
Nguồn: https://microservices.io/patterns/data/transactional-outbox.html
Debezium Outbox Event Router: cột `id` được đặt vào header để consumer phát hiện trùng, `aggregateid` làm message key để đảm bảo thứ tự theo partition, `aggregatetype` dùng để route topic; chỉ xử lý INSERT và bỏ qua DELETE vì bảng outbox đóng vai trò queue.
Nguồn: https://debezium.io/documentation/reference/stable/transformations/outbox-event-router.html
Ordering của outbox: thứ tự publish theo thứ tự service gửi (microservices.io); với polling relay chạy nhiều instance song song thì thứ tự bị mất trừ khi chỉ có một relay hoặc claim theo aggregate (chưa xác minh bằng nguồn riêng, là suy luận thiết kế).

**Saga**
Saga là chuỗi local transaction; mỗi bước cập nhật DB và publish message hoặc event để kích hoạt bước tiếp theo.
Choreography: mỗi local transaction publish domain event kích hoạt local transaction ở service khác; orchestration: một orchestrator chỉ định participant chạy local transaction nào.
Compensating transaction hoàn tác các bước đã xong khi một bước lỗi; khác với ACID rollback; saga thiếu Isolation nên phải có countermeasure cho anomaly do saga chạy đồng thời.
Nguồn: https://microservices.io/patterns/data/saga.html
Khi compensation thất bại: compensating transaction cũng là eventually consistent và có thể lỗi; hệ thống phải ghi tiến độ để resume từ điểm lỗi, các bước compensation phải là idempotent command để lặp lại được, và đôi khi chỉ can thiệp thủ công mới phục hồi được, lúc đó cần alert kèm chi tiết lỗi.
Nên retry bước lỗi như transient fault trước, chỉ compensate khi retry cạn hoặc lỗi non-transient; xác định các bước "point of no return" không thể hoàn tác và đặt chúng sau các bước validate quan trọng.
Nguồn: https://learn.microsoft.com/en-us/azure/architecture/patterns/compensating-transaction

**Chaos testing consumer và broker**
Nguyên tắc: định nghĩa steady state bằng output đo được, đưa vào biến cố thực tế (hỏng phần cứng, lỗi phần mềm, đột biến traffic), tự động hoá thí nghiệm, giữ blast radius nhỏ.
Nguồn: https://principlesofchaos.org/
Với consumer và broker, các thí nghiệm cụ thể cho lab: kill consumer giữa lúc xử lý (kiểm tra redelivery và idempotency), restart Redis (kiểm tra AOF/persistence và reconnect), thêm latency hoặc cắt kết nối bằng Toxiproxy, duplicate và reorder message.
Nguồn Toxiproxy: https://github.com/Shopify/toxiproxy (chỉ dùng cho công cụ, chi tiết tính năng chưa xác minh trong phiên này).

### Chương 09

**Job queue TS: BullMQ - retry, backoff**
`attempts` lớn hơn 1 bật retry tự động; processor ném exception thì worker chuyển job vào failed set (khi hết attempts).
Exponential backoff dùng công thức `2^(attempts-1) * delay` (delay 1000 ms thì lần thứ ba chờ 2 giây); cả fixed và exponential đều hỗ trợ tuỳ chọn jitter.
Custom backoff strategy trả về mili giây cho lần retry kế tiếp; trả 0 đẩy job về cuối waiting list, trả -1 không retry và đánh dấu failed.
Nguồn: https://docs.bullmq.io/guide/retrying-failing-jobs

**BullMQ - retention mặc định và failed set**
Mặc định BullMQ giữ completed và failed job vô thời hạn trong các set trong Redis (tức `removeOnComplete` và `removeOnFail` mặc định là không xoá), nên production phải cấu hình, nếu không Redis phình dần.
Tuỳ chọn: `true` (xoá ngay), số (giữ tối đa N job), hoặc object `{ age, count }` (age tính bằng giây).
Failed job nằm trong "failed" set; việc auto-removal chạy lazy, chỉ kích hoạt khi có job mới completed hoặc failed.
Nếu dùng job ID tuỳ chọn để idempotent thì job đã bị xoá sẽ không còn chặn được duplicate.
Nguồn: https://docs.bullmq.io/guide/queues/auto-removal-of-jobs

**BullMQ - delayed job, stalled job, graceful close**
Delayed job: `queue.add(name, data, { delay: ms })`, job nằm trong delayed set tới khi hết delay; thời điểm xử lý không chính xác tuyệt đối, phụ thuộc mức bận của worker.
Nguồn: https://docs.bullmq.io/guide/jobs/delayed
Stalled job là job active mà worker không còn báo được cho queue (crash, vòng lặp vô hạn); mặc định `maxStalledCount = 1` và `stalledInterval = 30s`; vượt giới hạn thì job fail vĩnh viễn với lỗi "job stalled more than allowable limit".
Nguồn: https://docs.bullmq.io/guide/jobs/stalled
`worker.close()` ngừng nhận job mới và chờ job đang chạy xong; hàm không có timeout nội bộ nên job phải kết thúc trong thời gian hợp lý; nếu worker chết không đóng đúng cách thì job dở dang thành stalled và worker khác nhặt lại (từ BullMQ 2.0 không cần QueueScheduler).
Nguồn: https://docs.bullmq.io/guide/workers/graceful-shutdown

**BullMQ - metrics và scheduler**
Bật metrics ở Worker bằng `metrics: { maxDataPoints: MetricsTime.ONE_WEEK * 2 }` (cấu hình thống nhất trên mọi worker); số liệu completed/failed gộp theo phút, hai tuần dùng khoảng 120KB RAM mỗi queue; đọc bằng `queue.getMetrics('completed', 0, ...)`; có export Prometheus.
Nguồn: https://docs.bullmq.io/guide/metrics
Job recurring dùng Job Schedulers: `upsertJobScheduler(id, { every | pattern }, template)`, upsert idempotent theo scheduler ID, scheduler chỉ tạo job mới khi job trước bắt đầu xử lý.
Nguồn: https://docs.bullmq.io/guide/job-schedulers

**BullMQ v6 (thay đổi đáng chú ý, ảnh hưởng code lab)**
v6.0.0 (2026-07-30) có breaking changes: bỏ legacy repeatable jobs (dùng Job Schedulers), bỏ `debounce` (dùng deduplication), bỏ `Job#discard()` (dùng `UnrecoverableError`), bỏ trạng thái `paused` khỏi JobType.
`ioredis` không còn là dependency trực tiếp mà là optional peer dependency (`>=5.0.0`, cũng hỗ trợ `redis >=5.0.0` và `pg`), nên phải tự cài `ioredis`.
Tuỳ chọn `connection` ở Queue/Worker vẫn tồn tại trong type definitions của 6.3.11 (kiểm tra bằng gói npm đã tải về).
Nguồn: https://github.com/taskforcesh/bullmq/releases/tag/v6.0.0 và https://registry.npmjs.org/bullmq

**Job queue Go: asynq**
Mặc định retry tối đa 25 lần (`defaultMaxRetry = 25`), ghi đè theo task bằng `asynq.MaxRetry(n)`; khi hết retry task chuyển vào archive (dead queue) để kiểm tra và thao tác thủ công qua CLI hoặc Web UI.
`asynq.SkipRetry` trả từ handler đưa task vào archive ngay không tốn retry; `IsFailure` cho phép coi một số lỗi không phải thất bại.
Nguồn: https://github.com/hibiken/asynq/wiki/Task-Retry và https://github.com/hibiken/asynq/blob/v0.26.0/client.go
`DefaultRetryDelayFunc` (v0.26.0): `n^4 + 15 + rand(0..29)*(n+1)` giây (công thức lấy từ Sidekiq), có jitter nhưng không phải full jitter; ghi đè bằng `Config.RetryDelayFunc`.
Nguồn: https://github.com/hibiken/asynq/blob/v0.26.0/server.go
Archive có giới hạn trong source: tối đa 10000 task và task archived bị xoá vĩnh viễn sau 90 ngày (`maxArchiveSize`, `archivedExpirationInDays`); cần xử lý hoặc retry task archived trước thời hạn này.
Nguồn: https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go
Timeout mặc định của một task là 30 phút nếu không đặt `Timeout` hoặc `Deadline`; `Retention` giữ task đã xử lý thành công trong khoảng thời gian chỉ định.
Nguồn: https://github.com/hibiken/asynq/blob/v0.26.0/client.go
Scheduler: `asynq.NewScheduler` với `Register(cronspec, task, opts...)`, `Run()`, `Shutdown()`; wiki cảnh báo phải đảm bảo chỉ một scheduler chạy cho mỗi lịch, nếu không sẽ có task trùng; để quản lý lịch động dùng `PeriodicTaskManager`.
Nguồn: https://github.com/hibiken/asynq/wiki/Periodic-Tasks
Graceful shutdown: `Server.Shutdown()` chờ worker đang chạy trong `Config.ShutdownTimeout` (mặc định 8 giây); quá hạn thì task được đẩy ngược lại Redis để xử lý lại.
`srv.Run(mux)` chặn, tự chờ tín hiệu của OS (`waitForSignals`) rồi gọi `Shutdown()`; dùng `Start`/`Shutdown` nếu muốn tự điều khiển.
Nguồn: https://github.com/hibiken/asynq/blob/v0.26.0/server.go
Metrics: package `github.com/hibiken/asynq/x/metrics` là Prometheus collector, các metric có prefix `asynq` như `asynq_tasks_enqueued_total`, `asynq_queue_size`, `asynq_queue_latency_seconds`, `asynq_tasks_processed_total`, `asynq_tasks_failed_total`, `asynq_queue_paused_total`.
Nguồn: https://github.com/hibiken/asynq/blob/v0.26.0/x/metrics/metrics.go
Changelog v0.26.0 (2026-02-03): thêm Headers cho task, `UpdateTaskPayload` cho inspector, cờ `--username` cho CLI; module yêu cầu Go 1.24.0.
Nguồn: https://github.com/hibiken/asynq/blob/v0.26.0/CHANGELOG.md

**Distributed lock: single-node SET NX PX, token, release an toàn**
Acquire: `SET resource_name my_random_value NX PX 30000`; value phải duy nhất giữa mọi client và mọi lần lock (docs dùng 20 byte từ /dev/urandom).
Release: dùng script Lua chỉ xoá key khi value đúng bằng token của mình (`if redis.call("get",KEYS[1]) == ARGV[1] then return redis.call("del",KEYS[1]) else return 0 end`); từ Redis 8.4 có `DELEX key IFEQ value` làm đúng việc đó.
Chỉ dùng `DEL` là không an toàn vì client có thể xoá lock của client khác sau khi lock của mình đã hết hạn.
Lock validity time vừa là thời gian auto-release vừa là thời gian client có để làm xong việc; sau đó mutual exclusion không còn được đảm bảo.
Nguồn: https://redis.io/docs/latest/develop/clients/patterns/distributed-locks/
Lưu ý: trang lệnh SET của redis.io ghi mẫu `SET NX EX` là "discouraged in favor of Redlock", trong khi cùng trang distributed-locks lại nói single-instance là phương án khả dụng khi chấp nhận race thỉnh thoảng xảy ra; handbook theo khuyến nghị của Kleppmann bên dưới.
Nguồn: https://redis.io/docs/latest/commands/set/
Failover: replica dùng thay master làm hỏng mutual exclusion vì replication bất đồng bộ (client A lấy lock, master chết trước khi ghi sang replica, replica lên master, client B lấy được lock cho cùng resource).
Nguồn: https://redis.io/docs/latest/develop/clients/patterns/distributed-locks/

**Redlock và tranh luận Kleppmann - antirez**
Redlock: N=5 master độc lập, acquire song song với timeout ngắn, lock hợp lệ nếu lấy được đa số (ít nhất 3) trong thời gian nhỏ hơn validity time; validity còn lại là validity ban đầu trừ thời gian đã dùng.
Redlock giả định đồng hồ cục bộ chạy cùng tốc độ xấp xỉ (clock drift nhỏ); chính docs Redis nói: nếu quan tâm correctness thì phải implement fencing tokens, và Redis không dùng monotonic clock cho TTL nên đổi giờ hệ thống có thể làm hai process cùng giữ lock.
Nguồn: https://redis.io/docs/latest/develop/clients/patterns/distributed-locks/
Kleppmann (2016-02-08): chia lock thành hai mục đích: efficiency (lỗi chỉ tốn chi phí hoặc phiền toái nhỏ) và correctness (lỗi gây hỏng dữ liệu); process pause (GC), network delay, clock jump có thể làm client tiếp tục ghi sau khi lock hết hạn; giải pháp là fencing token tăng đơn điệu kèm mỗi write để storage từ chối token cũ; Redlock không sinh được fencing token và cần mô hình đồng bộ (bounded delay, pause, clock error).
Khuyến nghị của Kleppmann: lock chỉ cho efficiency thì dùng single-node Redis và ghi rõ là lock xấp xỉ; lock cho correctness thì không dùng Redlock, dùng hệ consensus như ZooKeeper cùng fencing token.
Nguồn: https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html
antirez (phản hồi): ứng dụng không cần lock nếu có cơ chế khác tránh race khi mutual exclusion bị vi phạm (ví dụ token ngẫu nhiên lớn cộng check-and-set); Redlock cần chỉ "semi synchronous" (đo thời gian tương đối với sai số giới hạn), đồng ý rằng monotonic clock API sẽ tốt hơn; Redlock kiểm tra thời gian trước và sau khi lấy đa số nên pause trong lúc acquire không phá safety, nhưng pause sau khi acquire thì vẫn là vấn đề mà phản hồi này không giải quyết bằng fencing.
Nguồn: http://antirez.com/news/101
Khuyến nghị cho handbook: (1) lock cho efficiency (tránh làm trùng việc, cron một instance): single-node `SET NX PX` với token và release bằng Lua hoặc `DELEX IFEQ`, chấp nhận hiếm khi có hai holder; (2) lock cho correctness: không dựa vào Redis lock đơn thuần; đưa fencing token hoặc optimistic concurrency (cột version, unique constraint, check-and-set) xuống chính resource được bảo vệ, hoặc dùng hệ consensus (ZooKeeper, etcd); (3) không dạy Redlock như giải pháp correctness; nếu nhắc tới thì chỉ như phần tham khảo kèm tranh luận.

**Rate limiter (fixed window, sliding window, token bucket)**
Redis docs (tutorial cập nhật 2026-03-20) so sánh: fixed window (STRING, một key, xấp xỉ, cho phép burst gấp 2 ở ranh giới), sliding window log (SORTED SET, O(n) mỗi client, chính xác), sliding window counter (hai STRING, gần chính xác), token bucket (HASH hai field, cho burst có kiểm soát), leaky bucket (policing hoặc shaping); mặc định nên chọn sliding window counter, dùng token bucket khi cần cho phép burst.
Mọi thuật toán là read - decide - write nên phải chạy nguyên tử bằng Lua `EVAL`: tách thành nhiều lệnh gây race TOCTOU làm vượt limit; `MULTI/EXEC` không rẽ nhánh theo giá trị đã đọc, `WATCH` thì abort và retry nhiều khi contention cao.
Fixed window: `INCR` rồi `EXPIRE` khi count == 1 phải nằm trong cùng script, nếu không crash giữa hai lệnh để lại key không TTL chặn client vĩnh viễn.
Sliding window log: `ZREMRANGEBYSCORE` bỏ entry cũ, `ZCARD` đếm, `ZADD` thêm với member duy nhất (`timestamp:random`); trong Redis Cluster script nhiều key phải cùng slot (dùng hash tag `{...}`).
Khi bị chặn trả HTTP 429 kèm `Retry-After`; các header `X-RateLimit-*` chỉ là quy ước phổ biến, không có chuẩn duy nhất.
Nguồn: https://redis.io/tutorials/howtos/ratelimiting/
Công thức sliding window counter của Cloudflare: `rate = previous_count * (window - elapsed) / window + current_count`; thử nghiệm 400 triệu request cho thấy 0.003% quyết định sai và chênh lệch trung bình 6% giữa rate thực và rate xấp xỉ; chỉ cần hai số cho mỗi counter.
Nguồn: https://blog.cloudflare.com/counting-things-a-lot-of-different-things/
Stripe dùng token bucket với Redis tập trung ("take tokens on each request, slowly drip more tokens"), cùng ba loại khác: concurrent requests limiter, fleet usage load shedder, worker utilization load shedder; khuyến nghị limiter nên fail open (lỗi hệ thống không chặn traffic hợp lệ).
Nguồn: https://stripe.com/blog/rate-limiters

**Cache-aside**
Đọc: kiểm tra cache, nếu miss thì đọc data store rồi nạp vào cache và trả về; ghi: cập nhật data store trước rồi invalidate (xoá) key trong cache.
Thứ tự quan trọng: nếu xoá cache trước khi cập nhật store thì một client có thể đọc bản cũ từ store và nạp lại vào cache, gây dữ liệu stale.
Cache-aside không đảm bảo nhất quán giữa cache và store; TTL không nên quá ngắn (liên tục miss) cũng không quá dài (stale); không cache giá trị null theo ví dụ của Microsoft.
Nguồn: https://learn.microsoft.com/en-us/azure/architecture/patterns/cache-aside

**Cache stampede: singleflight (Go)**
`golang.org/x/sync/singleflight`: `Group.Do(key, fn)` bảo đảm tại một thời điểm chỉ có một lần thực thi `fn` cho mỗi key; lời gọi trùng chờ lời gọi gốc xong rồi nhận cùng kết quả (kể cả lỗi), cờ `shared` cho biết kết quả được chia sẻ.
`DoChan` trả về channel để kết hợp `select` với context timeout; `Forget(key)` làm các lần `Do` sau gọi lại `fn` thay vì chờ lần đang chạy.
Phiên bản hiện tại của module `golang.org/x/sync`: v0.23.0 (2026-08-31).
Nguồn: https://pkg.go.dev/golang.org/x/sync/singleflight
Singleflight chỉ dedup trong một process; nhiều instance vẫn có thể cùng load lại một key, nên cần kết hợp TTL jitter hoặc early expiration (suy luận thiết kế, không phải tuyên bố của docs).

**Cache stampede: in-flight promise map (TS)**
Pattern tương đương singleflight trong TS là một `Map<string, Promise<T>>`: request đầu tạo promise load và lưu vào map, request trùng `await` cùng promise, và xoá entry trong `finally`.
Không có nguồn chính thức cho pattern này trong phiên nghiên cứu (chưa xác minh bằng docs); đây là cách cài đặt tự thiết kế tương ứng với định nghĩa của singleflight ở trên, nên cần lab kiểm chứng (đếm số lần gọi loader dưới tải đồng thời).

**Cache stampede: probabilistic early expiration (XFetch)**
Paper "Optimal Probabilistic Cache Stampede Prevention" (Vattani, Chierichetti, Lowenstein, VLDB 2015, vol 8 no 8, tr. 886-897): mỗi reader độc lập quyết định tính lại sớm với xác suất tăng dần khi gần hết hạn; điều kiện tính lại: `time() - delta * beta * ln(rand()) >= expiry`, với `delta` là thời gian tính lại lần trước và `beta` thường là 1.
Phân phối mũ được paper chứng minh là tối ưu (tối thiểu stampede và tính lại thừa) - nội dung này lấy từ kết quả tìm kiếm tóm tắt, công thức và tên paper khớp nhiều nguồn nhưng chưa đọc trực tiếp bản PDF trong phiên này (PDF không trích xuất được text).
Nguồn: https://cseweb.ucsd.edu/~avattani/papers/cache_stampede.pdf và https://en.wikipedia.org/wiki/Cache_stampede

**Order/payment event-driven: outbox + Kafka + idempotent consumer + consumer lag**
Luồng chuẩn: service ghi order và bản ghi outbox trong cùng DB transaction, relay (polling hoặc Debezium CDC) publish lên Kafka với `aggregateid` làm key để giữ thứ tự theo order, consumer idempotent theo `id` của event (xem phần outbox và idempotent consumer ở chương 08).
Kafka consumer phía payment: exactly-once của Kafka chỉ trong phạm vi Kafka-to-Kafka, còn side effect ra ngoài (gọi cổng thanh toán) cần idempotency key gửi kèm request, ví dụ header `Idempotency-Key` của Stripe, để retry không trừ tiền hai lần.
Nguồn: https://microservices.io/patterns/data/transactional-outbox.html, https://debezium.io/documentation/reference/stable/transformations/outbox-event-router.html, https://docs.confluent.io/kafka/design/delivery-semantics.html, https://docs.stripe.com/api/idempotent_requests
Consumer lag: consumer Java của Kafka xuất metric JMX `records-lag-max` (lag lớn nhất theo số record của mọi partition trong cửa sổ), `records-lag` (lag mới nhất của partition) và `records-lag-avg` dưới `kafka.consumer:type=consumer-fetch-manager-metrics`; đó là offset lag, chênh lệch giữa vị trí consumer và offset cuối của broker.
Nguồn: https://kafka.apache.org/41/operations/monitoring/
Cách cảnh báo (nguồn thứ cấp, chưa xác minh với tài liệu chính thức): báo động khi lag tăng liên tục lâu hơn thời gian rebalance hoặc batch bình thường, hoặc khi committed offset đứng yên trong khi lag lớn hơn 0; Burrow của LinkedIn đánh giá lag theo cửa sổ trượt thay vì ngưỡng cố định.
Nguồn: https://github.com/linkedin/Burrow

**Realtime: WebSocket fan-out nhiều instance qua Redis Pub/Sub**
Mỗi instance giữ danh sách kết nối cục bộ; khi có message, publish lên Redis channel và mọi instance subscribe sẽ đẩy tới các socket của nó.
Redis Pub/Sub là at-most-once: subscriber rớt kết nối thì mất message, nên chỉ phù hợp thông báo realtime chấp nhận mất; cần replay khi reconnect thì dùng Streams (hoặc cho client tải lại trạng thái).
Pub/Sub không gắn với keyspace/database number (publish ở db 10 vẫn được subscriber ở db 1 nghe), nên nên prefix tên channel theo môi trường.
Trong Redis Cluster từ 7.0 có sharded Pub/Sub (`SSUBSCRIBE`, `SPUBLISH`) để message chỉ lan trong shard thay vì toàn cluster.
Nguồn: https://redis.io/docs/latest/develop/pubsub/
Client Redis ở trạng thái subscribe cần connection riêng (client RESP2 đang subscribe chỉ được chạy một số lệnh như SUBSCRIBE, PSUBSCRIBE, PING; RESP3 thì không bị giới hạn).
Nguồn: https://redis.io/docs/latest/develop/pubsub/

**Realtime: leaderboard bằng sorted set**
Sorted set sắp xếp theo score, cùng score thì sắp xếp theo thứ tự từ điển của member; Redis docs nêu leaderboard là use case điển hình; ZADD, ZINCRBY, ZRANK có độ phức tạp O(log N), `ZRANGE` là O(log(N)+M) với M là số kết quả trả về.
`ZREVRANGE` bị đánh dấu Deprecated trong docs, dùng `ZRANGE ... REV` thay thế.
Nguồn: https://redis.io/docs/latest/develop/data-types/sorted-sets/ và https://redis.io/docs/latest/commands/zrevrange/

**Realtime: thư viện `ws` (TS) và `coder/websocket` (Go)**
`ws` (npm 8.22.0, engines node >= 10): permessage-deflate mặc định tắt ở server vì tốn hiệu năng và bộ nhớ, chỉ bật khi thật cần và phải test tải thực tế; muốn phát hiện kết nối chết (rút dây mạng) phải dùng ping từ server, theo dõi `pong` và `terminate()` kết nối không phản hồi (mẫu heartbeat trong README).
Nguồn: https://github.com/websockets/ws/blob/master/README.md
`github.com/coder/websocket` (v1.8.15, 2026-06-15, go 1.23): API tối giản dùng `context.Context`, không dependency ngoài, hỗ trợ concurrent writes, ping/pong API, `CloseRead` cho kết nối chỉ ghi; do Coder duy trì từ 2024 (nhận lại từ nhooyr).
Mặc định giới hạn đọc một message là 32768 byte (chỉnh bằng `SetReadLimit`); `Accept` kiểm tra Origin, cấu hình bằng `OriginPatterns` thay vì `InsecureSkipVerify`.
README ghi "Ping pong heartbeat helper" và "Graceful shutdown helpers" còn nằm trong roadmap, tức là phải tự viết heartbeat và shutdown.
Ví dụ chat của repo dùng buffered channel cho mỗi subscriber (16 message) và `closeSlow` ngắt subscriber chậm: đây là mẫu backpressure cho fan-out.
Nguồn: https://github.com/coder/websocket/blob/v1.8.15/README.md, https://github.com/coder/websocket/blob/v1.8.15/read.go, https://github.com/coder/websocket/blob/v1.8.15/accept.go, https://github.com/coder/websocket/blob/v1.8.15/internal/examples/chat/chat.go

## Lỗi thường gặp ở production

**1. Outbox relay crash giữa publish và đánh dấu đã gửi**
Triệu chứng: sau khi relay restart, cùng một event xuất hiện hai lần trên broker; consumer không idempotent sẽ tạo bản ghi hoặc tính tiền hai lần.
Nguyên nhân: relay publish xong nhưng crash trước khi ghi nhận hoàn tất nên lần chạy sau publish lại; đây là at-least-once theo thiết kế, không phải bug.
Cách sửa: chấp nhận duplicate ở phía publish, gán event ID ổn định (cột `id` của outbox, đặt vào header) và làm consumer idempotent theo ID đó (Inbox); với Kafka thì bật idempotent producer để giảm duplicate do retry của producer nhưng không thay thế được idempotent consumer.
Nguồn: https://microservices.io/patterns/data/transactional-outbox.html, https://debezium.io/documentation/reference/stable/transformations/outbox-event-router.html, https://learn.microsoft.com/en-us/azure/architecture/patterns/idempotent-consumer

**2. Duplicate effect (trừ tiền hai lần, gửi email hai lần)**
Triệu chứng: message được ack một lần nhưng effect xảy ra nhiều lần; xảy ra khi consumer crash sau khi ghi DB và trước khi ack, khi lock hoặc visibility timeout hết hạn, hoặc khi hai consumer nhận cùng bản sao cùng lúc.
Cách sửa: dedup key ổn định, ghi marker và effect trong cùng transaction, dùng unique constraint hoặc atomic set-if-absent thay vì check rồi set, TTL của marker lớn hơn cửa sổ redeliver và cửa sổ replay từ DLQ, truyền idempotency key xuống các service gọi tiếp.
Với Redis `SET NX EX` làm dedup: nếu chiếm key rồi effect lỗi mà không nhả key, retry sẽ bị bỏ qua nhầm (mất effect); dùng trạng thái in-progress rồi completed hoặc xoá key khi lỗi (suy luận từ hai pha của Microsoft).
Nguồn: https://learn.microsoft.com/en-us/azure/architecture/patterns/idempotent-consumer, https://redis.io/docs/latest/commands/set/

**3. Retry storm (thundering herd khi retry)**
Triệu chứng: downstream vừa chậm lại thì tải tăng vọt thành từng đợt đồng bộ, dịch vụ không hồi phục; tổng công việc tăng theo N bình phương khi N client cùng tranh chấp.
Cách sửa: exponential backoff có cap kèm jitter, ưu tiên full jitter `sleep = random(0, min(cap, base * 2^attempt))`; giới hạn số attempts; trong BullMQ bật tuỳ chọn jitter của backoff; lưu ý `DefaultRetryDelayFunc` của asynq đã có phần ngẫu nhiên (`rand(30)*(n+1)`) nhưng không phải full jitter; autoscale consumer mà không giới hạn tổng tốc độ tới downstream chỉ chuyển quá tải sang downstream.
Nguồn: https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/, https://docs.bullmq.io/guide/retrying-failing-jobs, https://github.com/hibiken/asynq/blob/v0.26.0/server.go, https://learn.microsoft.com/en-us/azure/architecture/patterns/queue-based-load-leveling

**4. Poison message chặn queue**
Triệu chứng: một message luôn làm consumer lỗi, được trả lại queue và xử lý lại vô hạn, ăn hết tài nguyên hoặc (với queue có thứ tự) chặn các message phía sau.
Cách sửa: giới hạn số lần giao (SQS `maxReceiveCount`, BullMQ `attempts`, asynq `MaxRetry` mặc định 25) rồi chuyển vào DLQ, failed set hoặc archive; dùng `UnrecoverableError` (BullMQ) hoặc `asynq.SkipRetry` cho lỗi không thể retry; alarm khi DLQ có message; đặt retention của DLQ dài hơn queue gốc; không dùng DLQ với FIFO queue nếu phải giữ thứ tự tuyệt đối; với Redis Streams đọc delivery count từ `XPENDING` để tự chuyển sang stream DLQ.
Nguồn: https://learn.microsoft.com/en-us/azure/architecture/patterns/competing-consumers, https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-dead-letter-queues.html, https://github.com/hibiken/asynq/wiki/Task-Retry, https://docs.bullmq.io/guide/retrying-failing-jobs, https://github.com/taskforcesh/bullmq/releases/tag/v6.0.0, https://redis.io/docs/latest/develop/data-types/streams/

**5. Retention mặc định làm Redis đầy (BullMQ) và archive hết hạn (asynq)**
Triệu chứng: bộ nhớ Redis tăng đều vì completed và failed job được giữ vô thời hạn theo mặc định của BullMQ; với asynq, task trong archive bị xoá sau 90 ngày và archive giới hạn 10000 task nên bằng chứng lỗi biến mất.
Cách sửa: đặt `removeOnComplete` và `removeOnFail` (số lượng hoặc `age`) cho BullMQ, nhớ rằng xoá job làm mất khả năng chặn duplicate theo job ID; theo dõi và xử lý archive của asynq định kỳ.
Nguồn: https://docs.bullmq.io/guide/queues/auto-removal-of-jobs, https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go

**6. Worker bị kill giữa chừng (stalled job, mất job khi shutdown)**
Triệu chứng: job bị chạy lại hoặc fail với "job stalled more than allowable limit"; vòng lặp CPU dài hơn 30 giây khiến worker không gia hạn lock.
Cách sửa: gọi `worker.close()` khi nhận SIGTERM (hàm chờ job đang chạy, không có timeout nên cần giới hạn từ bên ngoài, ví dụ terminationGracePeriod); tránh tác vụ CPU dài trong event loop hoặc dùng sandboxed processor; asynq: để `Server.Shutdown()` hoặc `Run` xử lý, đặt `ShutdownTimeout` đủ lớn (mặc định 8 giây), task quá hạn sẽ được đưa lại Redis nên handler vẫn phải idempotent.
Nguồn: https://docs.bullmq.io/guide/workers/graceful-shutdown, https://docs.bullmq.io/guide/jobs/stalled, https://github.com/hibiken/asynq/blob/v0.26.0/server.go

**7. Lock hết hạn khi đang giữ lock**
Triệu chứng: hai process cùng sửa một tài nguyên; xảy ra khi client bị GC pause, network delay hoặc xử lý lâu hơn TTL, rồi vẫn ghi sau khi lock đã hết hạn và client khác đã lấy lock; thêm nữa là release bằng `DEL` xoá nhầm lock của client khác.
Cách sửa: release bằng token (Lua hoặc `DELEX IFEQ`); đặt TTL lớn hơn thời gian xử lý dự kiến và gia hạn có kiểm tra token; với lock cần correctness thì dùng fencing token hoặc optimistic concurrency ở chính resource (version column, unique constraint), hoặc hệ consensus; biết rằng Redis không dùng monotonic clock cho TTL, đổi giờ hệ thống có thể làm hai process cùng giữ lock; failover replica làm mất mutual exclusion vì replication bất đồng bộ.
Nguồn: https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html, https://redis.io/docs/latest/develop/clients/patterns/distributed-locks/

**8. Cache stampede**
Triệu chứng: một key nóng hết hạn, hàng trăm request cùng miss và cùng đọc DB, DB quá tải; sau đó nạp lại cache nhiều lần.
Cách sửa: gộp request trùng bằng singleflight (Go) hoặc map promise đang chạy (TS) trong một process; dùng xác suất tính lại sớm (XFetch: `time() - delta * beta * ln(rand()) >= expiry`) hoặc TTL có jitter để nhiều instance không hết hạn cùng lúc; giữ thứ tự cache-aside đúng (cập nhật store rồi mới xoá cache); dùng `DoChan` cùng context timeout để waiter không chờ vô hạn một loader treo (suy luận từ semantics của `Do`).
Nguồn: https://pkg.go.dev/golang.org/x/sync/singleflight, https://cseweb.ucsd.edu/~avattani/papers/cache_stampede.pdf, https://learn.microsoft.com/en-us/azure/architecture/patterns/cache-aside

**9. Rate limiter có race hoặc không có TTL**
Triệu chứng: vượt limit dưới tải đồng thời; key counter không có TTL nên client bị chặn vĩnh viễn nếu crash giữa `INCR` và `EXPIRE`; limiter lỗi làm chặn cả traffic hợp lệ.
Cách sửa: toàn bộ read - decide - write trong một Lua script; trong Redis Cluster mọi key của script phải cùng slot (hash tag); cho limiter fail open khi Redis lỗi (khuyến nghị của Stripe); fixed window cho burst gấp 2 ở ranh giới nên dùng sliding window counter hoặc token bucket khi cần.
Nguồn: https://redis.io/tutorials/howtos/ratelimiting/, https://stripe.com/blog/rate-limiters

**10. Realtime: kết nối chết và subscriber chậm**
Triệu chứng: server giữ hàng nghìn socket đã chết (rút dây mạng, NAT timeout); một client chậm làm tràn bộ nhớ vì hàng đợi gửi không giới hạn; message Pub/Sub mất khi instance rớt kết nối Redis.
Cách sửa: heartbeat ping/pong và `terminate()` (ws), tự cài heartbeat với coder/websocket vì chưa có helper; buffer có giới hạn cho mỗi subscriber và ngắt subscriber chậm (mẫu `closeSlow`); đặt giới hạn kích thước message (coder/websocket mặc định 32768 byte); tránh bật permessage-deflate nếu không cần; chấp nhận Pub/Sub at-most-once hoặc dùng Streams nếu cần replay.
Nguồn: https://github.com/websockets/ws/blob/master/README.md, https://github.com/coder/websocket/blob/v1.8.15/README.md, https://github.com/coder/websocket/blob/v1.8.15/internal/examples/chat/chat.go, https://redis.io/docs/latest/develop/pubsub/

**11. Scheduler chạy trùng (asynq và BullMQ)**
Triệu chứng: job cron được enqueue nhiều lần khi chạy nhiều replica.
Cách sửa: asynq Scheduler chỉ chạy một instance cho mỗi lịch (wiki cảnh báo duplicate tasks) hoặc dùng `PeriodicTaskManager`; BullMQ dùng `upsertJobScheduler` với scheduler ID cố định (upsert idempotent) thay vì repeatable jobs đã bị gỡ ở v6.
Nguồn: https://github.com/hibiken/asynq/wiki/Periodic-Tasks, https://docs.bullmq.io/guide/job-schedulers

## Nguồn

http://antirez.com/news/101 (truy cập 2026-10-06)
https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/ (truy cập 2026-10-06)
https://blog.cloudflare.com/counting-things-a-lot-of-different-things/ (truy cập 2026-10-06)
https://bravenewgeek.com/you-cannot-have-exactly-once-delivery/ (truy cập 2026-10-06)
https://cseweb.ucsd.edu/~avattani/papers/cache_stampede.pdf (truy cập 2026-10-06)
https://debezium.io/documentation/reference/stable/transformations/outbox-event-router.html (truy cập 2026-10-06)
https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-dead-letter-queues.html (truy cập 2026-10-06)
https://docs.bullmq.io/guide/job-schedulers (truy cập 2026-10-06)
https://docs.bullmq.io/guide/jobs/delayed (truy cập 2026-10-06)
https://docs.bullmq.io/guide/jobs/stalled (truy cập 2026-10-06)
https://docs.bullmq.io/guide/metrics (truy cập 2026-10-06)
https://docs.bullmq.io/guide/queues/auto-removal-of-jobs (truy cập 2026-10-06)
https://docs.bullmq.io/guide/retrying-failing-jobs (truy cập 2026-10-06)
https://docs.bullmq.io/guide/workers/graceful-shutdown (truy cập 2026-10-06)
https://docs.confluent.io/kafka/design/delivery-semantics.html (truy cập 2026-10-06)
https://docs.stripe.com/api/idempotent_requests (truy cập 2026-10-06)
https://en.wikipedia.org/wiki/Cache_stampede (truy cập 2026-10-06)
https://github.com/Shopify/toxiproxy (truy cập 2026-10-06)
https://github.com/aws-samples/aws-arch-backoff-simulator (truy cập 2026-10-06)
https://github.com/coder/websocket/blob/v1.8.15/README.md (truy cập 2026-10-06)
https://github.com/coder/websocket/blob/v1.8.15/accept.go (truy cập 2026-10-06)
https://github.com/coder/websocket/blob/v1.8.15/internal/examples/chat/chat.go (truy cập 2026-10-06)
https://github.com/coder/websocket/blob/v1.8.15/read.go (truy cập 2026-10-06)
https://github.com/hibiken/asynq/blob/v0.26.0/CHANGELOG.md (truy cập 2026-10-06)
https://github.com/hibiken/asynq/blob/v0.26.0/client.go (truy cập 2026-10-06)
https://github.com/hibiken/asynq/blob/v0.26.0/internal/rdb/rdb.go (truy cập 2026-10-06)
https://github.com/hibiken/asynq/blob/v0.26.0/server.go (truy cập 2026-10-06)
https://github.com/hibiken/asynq/blob/v0.26.0/x/metrics/metrics.go (truy cập 2026-10-06)
https://github.com/hibiken/asynq/wiki/Periodic-Tasks (truy cập 2026-10-06)
https://github.com/hibiken/asynq/wiki/Task-Retry (truy cập 2026-10-06)
https://github.com/linkedin/Burrow (truy cập 2026-10-06)
https://github.com/taskforcesh/bullmq/releases/tag/v6.0.0 (truy cập 2026-10-06)
https://github.com/websockets/ws/blob/master/README.md (truy cập 2026-10-06)
https://kafka.apache.org/41/design/design/ (truy cập 2026-10-06)
https://kafka.apache.org/41/operations/monitoring/ (truy cập 2026-10-06)
https://learn.microsoft.com/en-us/azure/architecture/patterns/cache-aside (truy cập 2026-10-06)
https://learn.microsoft.com/en-us/azure/architecture/patterns/compensating-transaction (truy cập 2026-10-06)
https://learn.microsoft.com/en-us/azure/architecture/patterns/competing-consumers (truy cập 2026-10-06)
https://learn.microsoft.com/en-us/azure/architecture/patterns/idempotent-consumer (truy cập 2026-10-06)
https://learn.microsoft.com/en-us/azure/architecture/patterns/queue-based-load-leveling (truy cập 2026-10-06)
https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html (truy cập 2026-10-06)
https://microservices.io/patterns/communication-style/idempotent-consumer.html (truy cập 2026-10-06)
https://microservices.io/patterns/data/saga.html (truy cập 2026-10-06)
https://microservices.io/patterns/data/transactional-outbox.html (truy cập 2026-10-06)
https://pkg.go.dev/golang.org/x/sync/singleflight (truy cập 2026-10-06)
https://principlesofchaos.org/ (truy cập 2026-10-06)
https://proxy.golang.org/github.com/coder/websocket/@latest (truy cập 2026-10-06)
https://proxy.golang.org/github.com/hibiken/asynq/@latest (truy cập 2026-10-06)
https://redis.io/docs/latest/commands/set/ (truy cập 2026-10-06)
https://redis.io/docs/latest/commands/xnack/ (truy cập 2026-10-06)
https://redis.io/docs/latest/commands/zrevrange/ (truy cập 2026-10-06)
https://redis.io/docs/latest/develop/clients/patterns/distributed-locks/ (truy cập 2026-10-06)
https://redis.io/docs/latest/develop/data-types/sorted-sets/ (truy cập 2026-10-06)
https://redis.io/docs/latest/develop/data-types/streams/ (truy cập 2026-10-06)
https://redis.io/docs/latest/develop/pubsub/ (truy cập 2026-10-06)
https://redis.io/tutorials/howtos/ratelimiting/ (truy cập 2026-10-06)
https://registry.npmjs.org/bullmq (truy cập 2026-10-06)
https://stripe.com/blog/rate-limiters (truy cập 2026-10-06)
https://www.rabbitmq.com/docs/confirms (truy cập 2026-10-06)
