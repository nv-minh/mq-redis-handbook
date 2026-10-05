# Kafka - ghi chú nghiên cứu cho chương 06

## Phiên bản

Kafka mới nhất đã xác minh ngày 2026-10-06 là 4.3.1, phát hành 2026-06-25, có docker image `apache/kafka:4.3.1` và `apache/kafka-native:4.3.1`.
Nguồn: https://kafka.apache.org/community/downloads/
Các nhánh còn được hỗ trợ trên trang downloads là 4.3.1, 4.2.2 (phát hành 2026-09-29) và 4.1.2, các bản 4.0.x đã vào danh sách archived.
Nguồn: https://kafka.apache.org/community/downloads/
Image `apache/kafka:4.3.1` trong repo là bản mới nhất, không lỗi thời.
Bản 4.3.1 sửa khoảng 15 lỗi so với 4.3.0, đáng chú ý nhất là rò rỉ bộ nhớ native RocksDB của Kafka Streams (KAFKA-20616), không ảnh hưởng labs vì labs không dùng Streams.
Nguồn: https://kafka.apache.org/43/getting-started/upgrade/
Client TypeScript `@confluentinc/kafka-javascript` 1.10.1 là bản `latest` trên npm, phát hành 2026-09-10, tham chiếu librdkafka 2.15.1, yêu cầu Node.js >= 18.
Nguồn: https://registry.npmjs.org/@confluentinc/kafka-javascript
Nguồn: https://github.com/confluentinc/confluent-kafka-javascript/blob/master/CHANGELOG.md
Bản 1.10.1 sửa việc offset chỉ được "store" sau khi `eachMessage` xử lý xong (trước đó lưu sớm hơn) và sửa lỗi trùng một message khi seek lùi để xử lý lại.
Nguồn: https://github.com/confluentinc/confluent-kafka-javascript/blob/master/CHANGELOG.md
Ghi chú cho lab: bản 1.11.0 chưa phát hành (mục "unreleased" trong CHANGELOG), nên 1.10.1 là bản ổn định hiện tại.
Client Go `github.com/segmentio/kafka-go` v0.4.51 là tag mới nhất trên Go module proxy, ngày 2026-04-23.
Nguồn: https://proxy.golang.org/github.com/segmentio/kafka-go/@latest
Các thay đổi 4.x liên quan lab:
- Kafka 4.0 chỉ hỗ trợ KRaft, ZooKeeper mode đã bị gỡ, và điều này vẫn đúng ở 4.3 ("Apache Kafka 4.3 only supports KRaft mode - ZooKeeper mode has been removed").
- Kafka 4.0 loại bỏ các protocol API version cũ, yêu cầu broker và Java client từ 2.1 trở lên, và khuyến nghị đọc KIP-896 cho client không thuộc Apache Kafka.
- KIP-848 (consumer rebalance protocol thế hệ mới) là GA từ Kafka 4.0, tự bật trên server khi finalize nâng cấp lên 4.0.
- KIP-890 (transaction server-side defense) nằm trong 4.0, producer 4.0 bump epoch ở mỗi transaction.
- 4.0 gỡ `DefaultPartitioner` và `UniformStickyPartitioner` khỏi Java client, partitioner mặc định nay nằm trong producer (xem phần partitioner bên dưới).
- 4.0 đưa group coordinator mới vào, KIP-966 phần 1 thêm Eligible Leader Replicas (ELR).
- 4.2 coi Queues for Kafka (share groups, KIP-932) là production-ready, nằm ngoài phạm vi chương này.
- 4.3 đánh dấu cấu hình `group.coordinator.rebalance.protocols` là deprecated, sẽ bỏ ở Kafka 5.0 (mọi protocol luôn bật, điều khiển bởi `group.version` qua `kafka-features.sh`).
- Trong `--bootstrap-server` của các tool CLI, chỉ chấp nhận danh sách phân tách bằng dấu phẩy.
Nguồn: https://kafka.apache.org/43/getting-started/upgrade/
Nguồn: https://kafka.apache.org/42/getting-started/upgrade/

## Khái niệm bắt buộc

### Kafka single-node KRaft trong Docker (kiểm tra bắt buộc)

Kết luận: image `apache/kafka:4.3.1` chạy được một broker duy nhất ở combined mode (`process.roles=broker,controller`) mà không cần ZooKeeper.
Nếu không truyền cấu hình nào, image dùng cấu hình KRaft mặc định cho một node combined, đủ để `docker run -p 9092:9092 apache/kafka:4.3.1` hoạt động từ host.
Nguồn: https://kafka.apache.org/43/getting-started/docker/
Nguồn: https://github.com/apache/kafka/blob/4.3/docker/examples/README.md
Cấu hình mặc định trong tarball đặt `listeners=PLAINTEXT://:9092,CONTROLLER://:9093`, `advertised.listeners=PLAINTEXT://localhost:9092,CONTROLLER://localhost:9093`, `controller.quorum.bootstrap.servers=localhost:9093` và đặt sẵn `offsets.topic.replication.factor=1`, `transaction.state.log.replication.factor=1`, `transaction.state.log.min.isr=1`.
Nguồn: https://github.com/apache/kafka/blob/4.3/config/server.properties
Khi tự cấu hình bằng env var (ví dụ docker compose) thì phải khai báo đủ các thuộc tính KRaft bắt buộc, và quy tắc đặt tên env var là: thay `.` bằng `_`, thay `_` bằng `__`, thay `-` bằng `___`, thêm tiền tố `KAFKA_`.
Nguồn: https://github.com/apache/kafka/blob/4.3/docker/examples/README.md
Compose mẫu chính thức cho single node (plaintext) dùng các biến sau, đây là mẫu nên chép cho lab:
- `KAFKA_NODE_ID=1`, `KAFKA_PROCESS_ROLES=broker,controller`, `KAFKA_CONTROLLER_QUORUM_VOTERS=1@broker:29093`, `KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER`, `CLUSTER_ID` (một UUID base64).
- `KAFKA_LISTENERS=CONTROLLER://:29093,PLAINTEXT_HOST://:9092,PLAINTEXT://:19092` và `KAFKA_ADVERTISED_LISTENERS=PLAINTEXT_HOST://localhost:9092,PLAINTEXT://broker:19092`, tức client trên host dùng `localhost:9092`, client trong mạng compose dùng `broker:19092`.
- `KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT` và `KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT`.
- Internal topic phải hạ về 1 replica vì mặc định cần 3 broker: `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1`, `KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1`, `KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1`.
- Tùy chọn: `KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0` (bỏ độ trễ 3 giây khi group mới lập, giúp test nhanh) và `KAFKA_SHARE_COORDINATOR_STATE_TOPIC_REPLICATION_FACTOR=1` cùng `..._MIN_ISR=1` (chỉ cần nếu dùng share group).
Nguồn: https://github.com/apache/kafka/blob/4.3/docker/examples/docker-compose-files/single-node/plaintext/docker-compose.yml
Docker Compose mẫu này lưu ý `advertised.listeners` quyết định địa chỉ mà broker trả về trong metadata, nên nếu advertised sai thì client kết nối bootstrap được nhưng producer và consumer treo hoặc lỗi kết nối ở bước sau (xem mục lỗi production).
Nếu một broker có `min.insync.replicas` lớn hơn số replica có sẵn thì produce với `acks=all` sẽ bị từ chối, nên topic tạo ở single-node phải dùng replication factor 1 và `min.insync.replicas=1` (chưa xác minh bằng chạy thật trong lab, suy ra từ định nghĩa `min.insync.replicas` ở mục replication).

### Log, topic, partition, thứ tự (ordering)

Kafka lưu dữ liệu như một log: event được append vào một partition của topic, và sau khi consumer đọc thì event không bị xóa, chỉ bị loại bỏ khi hết thời gian retention cấu hình theo topic.
Nguồn: https://kafka.apache.org/43/getting-started/introduction/
Topic được chia thành nhiều partition, mỗi event mới được append vào đúng một partition của topic.
Các event có cùng key được ghi vào cùng một partition, và consumer của một topic-partition luôn đọc các event của partition đó đúng theo thứ tự đã ghi.
Nguồn: https://kafka.apache.org/43/getting-started/introduction/
Hệ quả cần dạy: thứ tự chỉ được đảm bảo trong một partition, không đảm bảo giữa các partition khác nhau của cùng topic (suy ra trực tiếp từ hai câu trên, chương cần nhấn mạnh để người học không kỳ vọng global order).
Nguồn: https://kafka.apache.org/43/getting-started/introduction/
Nhân bản (replication) thực hiện ở mức topic-partition, cấu hình production phổ biến là replication factor 3.
Nguồn: https://kafka.apache.org/43/getting-started/introduction/
Tất cả replica của một partition có cùng log với cùng offset, và consumer tự kiểm soát position (offset) của mình trong log đó.
Nguồn: https://kafka.apache.org/43/design/design/

### Replication, ISR, acks, min.insync.replicas

Kafka không dùng majority vote để chọn quorum mà duy trì động tập in-sync replicas (ISR) gồm các replica đã bắt kịp leader, và chỉ thành viên ISR mới đủ điều kiện được bầu làm leader.
Một write chưa được coi là committed cho tới khi mọi replica đang trong ISR đã nhận write đó, và với f+1 replica thì topic chịu được f lỗi mà không mất message đã committed.
Nguồn: https://kafka.apache.org/43/design/design/
Producer chọn chờ ack từ 0, 1 hay all (-1) replica, nhưng "all" nghĩa là tất cả replica đang trong ISR, không phải toàn bộ replica được gán.
Ví dụ topic có 2 replica mà 1 replica rớt khỏi ISR thì ghi với `acks=all` vẫn thành công và có thể mất nếu replica còn lại cũng hỏng.
Nguồn: https://kafka.apache.org/43/design/design/
`min.insync.replicas` (mặc định 1) chỉ có tác dụng khi producer dùng `acks=all`; nếu ISR nhỏ hơn ngưỡng này thì partition từ chối ghi.
Cấu hình khuyến nghị trong tài liệu chính thức: replication factor 3, `min.insync.replicas=2`, `acks=all`, đổi lại partition mất khả năng ghi khi ISR dưới 2.
Nguồn: https://kafka.apache.org/43/configuration/topic-configs/
Nguồn: https://kafka.apache.org/43/design/design/
Khi tính năng Eligible Leader Replicas (ELR, KIP-966 phần 1) bật thì ngữ nghĩa của `min.insync.replicas` thay đổi, xem mục ELR của tài liệu (chưa xác minh chi tiết, ngoài phạm vi lab).
Nguồn: https://kafka.apache.org/43/configuration/topic-configs/
`unclean.leader.election.enable` mặc định `false`, nghĩa là nếu toàn bộ ISR chết thì partition không khả dụng cho tới khi một replica ISR quay lại, đổi availability lấy consistency.
Nguồn: https://kafka.apache.org/43/configuration/topic-configs/
Nguồn: https://kafka.apache.org/43/design/design/
`replica.lag.time.max.ms` mặc định 30000: follower không gửi fetch hoặc không bắt kịp log end offset của leader trong thời gian này sẽ bị loại khỏi ISR.
Nguồn: https://kafka.apache.org/43/configuration/broker-configs/
Producer 4.x mặc định `acks=all` và `enable.idempotence=true` (idempotence tự tắt nếu người dùng đặt cấu hình xung đột mà không bật tường minh).
Nguồn: https://kafka.apache.org/43/configuration/producer-configs/

### Producer: key, partitioner, idempotence, batching (kiểm tra bắt buộc về partitioner)

Java client 4.x: khi `partitioner.class` không đặt thì dùng logic mặc định tích hợp trong producer: có key thì chọn partition theo hash của key, không có key thì chọn sticky partition và đổi partition khi đã ghi ít nhất `batch.size` byte.
Nguồn: https://kafka.apache.org/43/configuration/producer-configs/
Bản 4.0 đã gỡ các lớp `DefaultPartitioner` và `UniformStickyPartitioner` khỏi Java client, logic mặc định không còn là một lớp có thể tham chiếu (khớp với ghi chú nâng cấp 4.0).
Nguồn: https://kafka.apache.org/43/getting-started/upgrade/
Java client có `partitioner.adaptive.partitioning.enable` (mặc định `true`, dồn nhiều record hơn tới partition của broker nhanh hơn) và `partitioner.ignore.keys` (mặc định `false`).
Nguồn: https://kafka.apache.org/43/configuration/producer-configs/
Hash của Java client là murmur2; tài liệu librdkafka xác nhận `murmur2_random` "functionally equivalent to the default partitioner in the Java Producer".
Nguồn: https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md
Kết luận cho librdkafka thuần: partitioner mặc định là `consistent_random` (CRC32 của key, key rỗng hoặc NULL được chọn partition ngẫu nhiên), KHÔNG tương thích hash với Java client.
Nguồn: https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md
Kết luận cho `@confluentinc/kafka-javascript` 1.10.1 (API KafkaJS): wrapper tự đặt `partitioner = 'murmur2_random'` (xem `lib/kafkajs/_producer.js` dòng 175) và từ chối tùy chọn `createPartitioner`; muốn đổi thì truyền `rdKafka: { partitioner: ... }` hoặc khai báo ngoài khối `kafkaJS`.
Do đó cùng key luôn vào cùng partition (miễn số partition không đổi), key null thì chọn ngẫu nhiên giữa các partition ở mức từng message, và có thêm hành vi sticky cho key null theo `sticky.partitioning.linger.ms` (mặc định 10 ms nên các message gửi sát nhau có thể dồn vào cùng một partition).
Nguồn: https://raw.githubusercontent.com/confluentinc/confluent-kafka-javascript/v1.10.1/lib/kafkajs/_producer.js
Nguồn: https://github.com/confluentinc/confluent-kafka-javascript/blob/master/MIGRATION.md
Nguồn: https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md
Hệ quả cho assert "null key spreads" trong lab TypeScript: với key null chỉ nên assert rằng các message phân tán qua nhiều hơn một partition khi gửi đủ nhiều message và có độ trễ giữa các lần gửi hoặc gửi từng batch riêng, không assert phân phối đều, vì sticky partitioning cố ý dồn message gửi sát nhau vào một partition.
Đây là suy luận từ tài liệu `sticky.partitioning.linger.ms`, chưa xác minh bằng chạy thật.
Nguồn: https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md
Kết luận cho `segmentio/kafka-go` v0.4.51: `Writer.Balancer` mặc định là round-robin ("The default is to use a round-robin distribution"), nên mặc định message có key cũng KHÔNG đảm bảo cùng key cùng partition.
Muốn "same key same partition" phải đặt tường minh `Balancer: &kafka.Hash{}` (FNV-1a, key nil thì round-robin, tương thích Sarama), hoặc `kafka.Murmur2Balancer{}` (tương thích Java và librdkafka `murmur2_random`, key nil thì chọn ngẫu nhiên), hoặc `kafka.CRC32Balancer{}` (tương đương librdkafka `consistent_random`).
Cả ba chỉ cho cùng partition khi số partition không đổi; `Murmur2Balancer` và `CRC32Balancer` chọn ngẫu nhiên cho key nil nên đáp ứng "null key spreads", còn `Hash` và `RoundRobin` luân phiên (spread đều theo thứ tự).
Nguồn: https://raw.githubusercontent.com/segmentio/kafka-go/v0.4.51/balancer.go
Nguồn: https://raw.githubusercontent.com/segmentio/kafka-go/v0.4.51/writer.go
Nếu lab trộn producer TypeScript (murmur2) và producer Go thì phải dùng `Murmur2Balancer` ở Go để cùng key rơi vào cùng partition giữa hai ngôn ngữ; dùng `Hash` (FNV-1a) sẽ cho partition khác (suy ra từ mã nguồn hai thuật toán khác nhau, chưa chạy kiểm thử chéo).
Nguồn: https://raw.githubusercontent.com/segmentio/kafka-go/v0.4.51/balancer.go
Idempotent producer: broker gán producer ID và khử trùng bằng sequence number gửi kèm mỗi message, đảm bảo việc gửi lại (retry) không tạo bản ghi trùng trong log của partition.
Nguồn: https://kafka.apache.org/43/design/design/
Điều kiện của idempotence: `max.in.flight.requests.per.connection <= 5`, `retries > 0`, `acks=all`; với các điều kiện đó thứ tự được giữ.
Nếu tắt idempotence mà vẫn `max.in.flight > 1` và có retry thì có nguy cơ đảo thứ tự record trong cùng partition.
Nguồn: https://kafka.apache.org/43/configuration/producer-configs/
Idempotence chỉ phủ phạm vi một producer session và một partition (producer ID mới sau khi restart nên không khử trùng qua restart), muốn xuyên session phải có `transactional.id` (xem mục transaction).
Nguồn: https://kafka.apache.org/43/configuration/producer-configs/
Batching Java client: `linger.ms` mặc định đổi từ 0 sang 5 ở Kafka 4.0, `batch.size` mặc định 16384 byte, `delivery.timeout.ms` bao trùm retry (khuyến nghị để mặc định `retries` và chỉnh `delivery.timeout.ms`).
Nguồn: https://kafka.apache.org/43/configuration/producer-configs/
Batching librdkafka: `linger.ms` (alias `queue.buffering.max.ms`) mặc định 5, `batch.num.messages` mặc định 10000, `acks` mặc định -1 (all), nhưng `enable.idempotence` mặc định `false` (khác Java client), `max.in.flight.requests.per.connection` được đặt về 5 khi bật idempotence.
Nguồn: https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md
Với kafka-javascript: `idempotent` mặc định `false`, `acks`, `compression`, `timeout` đặt ở cấu hình producer chứ không theo từng lần `send()`, và `sendBatch` chỉ là wrapper của `send` vì batching do librdkafka lo.
Nguồn: https://github.com/confluentinc/confluent-kafka-javascript/blob/master/MIGRATION.md
Với kafka-go: `Writer.RequiredAcks` mặc định `RequireNone` (0, fire-and-forget) khi dùng struct `kafka.Writer` trực tiếp; đặt `RequireAll` (-1) để chờ toàn bộ ISR.
`BatchSize` mặc định 100 message, `BatchTimeout` mặc định 1 giây (một lần `WriteMessages` đơn lẻ có thể bị trễ tới 1 giây), `MaxAttempts` mặc định 10, `Async` mặc định `false`.
`kafka.NewWriter` và `WriterConfig` đã deprecated, và khi dùng chúng thì `RequiredAcks` 0 được hiểu là `RequireAll`.
Nguồn: https://raw.githubusercontent.com/segmentio/kafka-go/v0.4.51/writer.go
kafka-go không có API idempotent producer hay transaction trong `Writer` (chưa xác minh tuyệt đối, tôi không thấy tùy chọn nào liên quan trong `writer.go` và README ở v0.4.51); lab Go về exactly-once nên dùng cách khử trùng phía consumer thay vì transaction.
Nguồn: https://raw.githubusercontent.com/segmentio/kafka-go/v0.4.51/writer.go
Nguồn: https://raw.githubusercontent.com/segmentio/kafka-go/v0.4.51/README.md

### Consumer group, offset, commit

Consumer group chia việc đọc các partition của topic cho các thành viên: mỗi partition tại một thời điểm chỉ được gán cho một consumer trong group, nên thêm consumer quá số partition thì consumer thừa ngồi không (idle).
Nguồn: https://developer.confluent.io/courses/architecture/consumer-group-protocol/
Group coordinator (một broker, chọn bằng hash của `group.id` theo số partition của `__consumer_offsets`) theo dõi thành viên và lưu offset đã commit vào topic nội bộ `__consumer_offsets`.
Nguồn: https://developer.confluent.io/courses/architecture/consumer-group-protocol/
Offset là định danh duy nhất của record trong một partition; "position" là offset của record kế tiếp sẽ được trả về (lớn hơn offset cao nhất đã thấy một đơn vị), còn "committed position" là offset cuối cùng đã lưu an toàn và là điểm consumer quay lại sau khi crash.
Offset không nhất thiết liên tục (topic compaction hoặc có transaction tạo ra khoảng trống).
Nguồn: https://kafka.apache.org/43/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html
Điều này có nghĩa là offset commit lên broker phải là offset của message đã xử lý cộng 1: tài liệu kafka-javascript nêu rõ "Note how we are committing offset + 1" vì Kafka lưu "next offset to read".
Nguồn: https://github.com/confluentinc/confluent-kafka-javascript/blob/master/INTRODUCTION.md
Auto commit (Java): `enable.auto.commit=true` mặc định, chu kỳ `auto.commit.interval.ms=5000`; offset được coi là đã consumed ngay khi `poll()` trả về, nên crash giữa chừng có thể làm mất xử lý.
Tài liệu Java nêu auto commit vẫn cho at-least-once nếu ứng dụng xử lý hết dữ liệu mỗi lần `poll()` trước khi gọi `poll()` lần sau.
Nguồn: https://kafka.apache.org/43/configuration/consumer-configs/
Nguồn: https://kafka.apache.org/43/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html
Commit thủ công sau khi xử lý (`commitSync` sau khi insert DB) cho at-least-once: nếu crash sau khi xử lý nhưng trước khi commit thì tiến trình thay thế đọc lại từ offset đã commit và lặp lại bước xử lý cuối.
Nguồn: https://kafka.apache.org/43/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html
Commit trước khi xử lý cho at-most-once: nếu crash sau khi lưu position nhưng trước khi xử lý xong thì message bị bỏ lỡ; tài liệu design mô tả đúng ba thứ tự này (lưu position rồi xử lý là at-most-once, xử lý rồi lưu position là at-least-once).
Nguồn: https://kafka.apache.org/43/design/design/
Mặc định của Kafka là at-least-once; at-most-once đạt được bằng cách tắt retry ở producer và commit offset trước khi xử lý batch.
Nguồn: https://kafka.apache.org/43/design/design/
`auto.offset.reset` (mặc định `latest` ở Java và librdkafka) chỉ áp dụng khi group chưa có offset đã commit hoặc offset hiện tại không còn trên server; nếu có offset đã commit thì consumer luôn tiếp tục từ đó.
Tài liệu cũng cảnh báo tăng số partition khi đặt `latest` có thể làm mất message vì producer ghi vào partition mới trước khi consumer reset offset cho partition đó.
Nguồn: https://kafka.apache.org/43/configuration/consumer-configs/
`@confluentinc/kafka-javascript` 1.10.1 (API KafkaJS): `autoCommit` mặc định `true`, commit mỗi 5000 ms offset của message mà `eachMessage` đã chạy xong; `fromBeginning` đặt ở cấp consumer (không phải từng `subscribe`) và ánh xạ sang `auto.offset.reset=earliest`, nếu không đặt thì dùng mặc định librdkafka là `latest`.
Nguồn: https://github.com/confluentinc/confluent-kafka-javascript/blob/master/INTRODUCTION.md
Nguồn: https://raw.githubusercontent.com/confluentinc/confluent-kafka-javascript/v1.10.1/lib/kafkajs/_consumer.js
Khi dùng commit thủ công trong kafka-javascript phải tự commit trong `rebalance_cb` khi gặp `ERR__REVOKE_PARTITIONS` vì thư viện không tự commit trong trường hợp đó.
Nguồn: https://github.com/confluentinc/confluent-kafka-javascript/blob/master/INTRODUCTION.md
Seek về đầu trong kafka-javascript: `consumer.seek({ topic, partition, offset: '-2' })` (`-2` là offset sớm nhất), gọi được lúc đang connected, và nếu auto commit bật thì seek cũng commit offset đó; `seek` có thể gọi trước khi partition được gán, khi đó seek được giữ lại và thực hiện sau rebalance.
Với `fromBeginning: true` chỉ có hiệu lực khi group chưa có offset đã commit, nên lab nên dùng `groupId` mới cho mỗi lần chạy hoặc seek tường minh.
Nguồn: https://github.com/confluentinc/confluent-kafka-javascript/blob/master/INTRODUCTION.md
Nguồn: https://raw.githubusercontent.com/confluentinc/confluent-kafka-javascript/v1.10.1/lib/kafkajs/_consumer.js
Tạo topic N partition trong kafka-javascript: `admin.createTopics({ topics: [{ topic, numPartitions: N, replicationFactor: 1 }] })`; nếu bỏ `numPartitions` hoặc `replicationFactor` thì gửi -1 để broker dùng mặc định; chưa hỗ trợ `validateOnly`, `waitForLeaders`, `replicaAssignment`.
Vì `waitForLeaders` chưa hỗ trợ, lab nên đợi metadata có đủ partition trước khi produce (chưa xác minh bằng chạy thật).
Nguồn: https://raw.githubusercontent.com/confluentinc/confluent-kafka-javascript/v1.10.1/lib/kafkajs/_admin.js
Nguồn: https://github.com/confluentinc/confluent-kafka-javascript/blob/master/MIGRATION.md
kafka-go reader group: dùng `kafka.NewReader(kafka.ReaderConfig{GroupID: ...})`; `ReadMessage` tự commit offset, còn muốn commit sau khi xử lý thì dùng `FetchMessage` rồi `CommitMessages`; `CommitInterval` mặc định 0 nghĩa là commit đồng bộ.
Nguồn: https://raw.githubusercontent.com/segmentio/kafka-go/v0.4.51/README.md
Nguồn: https://raw.githubusercontent.com/segmentio/kafka-go/v0.4.51/reader.go
`ReaderConfig.StartOffset` mặc định `FirstOffset` (tức `kafka.FirstOffset` = -2) khi group chưa có offset, nên khác với Java và librdkafka (`latest`), nên consumer group kafka-go mới tạo sẽ đọc từ đầu topic.
Khi dùng `GroupID`, `SetOffset` trả lỗi, `Offset()` và `Lag()` luôn trả -1, `ReadLag` trả lỗi; đọc từ offset cụ thể thì dùng reader không có `GroupID` (chỉ định `Partition`) rồi `SetOffset`.
Nguồn: https://raw.githubusercontent.com/segmentio/kafka-go/v0.4.51/reader.go
Nguồn: https://raw.githubusercontent.com/segmentio/kafka-go/v0.4.51/README.md
Tạo topic N partition trong kafka-go: kết nối tới controller (`conn.Controller()` rồi `kafka.Dial` tới host:port đó) và gọi `CreateTopics(kafka.TopicConfig{Topic, NumPartitions: N, ReplicationFactor: 1})`; cần làm vậy khi `auto.create.topics.enable=false`.
Nguồn: https://raw.githubusercontent.com/segmentio/kafka-go/v0.4.51/README.md

### Rebalance: nguyên nhân, thời gian, eager và cooperative, KIP-848 (kiểm tra bắt buộc)

Group coordinator kích hoạt rebalance khi một trong các sự kiện sau xảy ra: số partition của topic đã subscribe thay đổi, topic đã subscribe được tạo hoặc xóa, một thành viên hiện có tắt hoặc lỗi, hoặc có thành viên mới vào group.
Nguồn: https://kafka.apache.org/43/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html
"Thành viên lỗi" gồm hai cơ chế phát hiện riêng: không có heartbeat trong `session.timeout.ms`, và không gọi `poll()` trong `max.poll.interval.ms` (khi đó client chủ động rời group và `commitSync` có thể ném `CommitFailedException`).
Nguồn: https://kafka.apache.org/43/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html
Thay đổi subscription cũng kích hoạt rebalance, và `disconnect()` của consumer trong kafka-javascript gây một rebalance cuối.
Nguồn: https://github.com/confluentinc/confluent-kafka-javascript/blob/master/INTRODUCTION.md
Thời gian với mặc định, giao thức classic: Java client `session.timeout.ms=45000`, `heartbeat.interval.ms=3000`, `max.poll.interval.ms=300000`.
Nguồn: https://kafka.apache.org/43/configuration/consumer-configs/
Default 45 giây của Java đến từ KIP-735 (tăng từ 10 giây lên 45 giây để cho phép kết nối lại và retry sau một request timeout); trong lúc một thành viên lỗi, cả group phải tạm dừng để rebalance, và lỗi giả có thể gây hai rebalance liên tiếp.
Nguồn: https://cwiki.apache.org/confluence/display/KAFKA/KIP-735%3A+Increase+default+consumer+session+timeout
Hệ quả: nếu consumer bị kill đột ngột (SIGKILL, mất mạng) không gửi LeaveGroup thì coordinator chỉ phát hiện sau tối đa khoảng `session.timeout.ms` rồi mới rebalance, tức khoảng 45 giây với Java, trong thời gian đó partition của consumer chết không được xử lý.
Đây là suy luận từ định nghĩa `session.timeout.ms`, chưa xác minh bằng đo thực tế.
Nguồn: https://kafka.apache.org/43/configuration/consumer-configs/
Mặc định của hai client dùng trong lab khác Java: wrapper KafkaJS của kafka-javascript đặt `session.timeout.ms=30000` khi giao thức là classic (xem `_consumer.js` dòng 546; ghi chú: librdkafka thuần mặc định 45000), `heartbeatInterval` 3000, `rebalanceTimeout`/`max.poll.interval.ms` 300000.
Nguồn: https://raw.githubusercontent.com/confluentinc/confluent-kafka-javascript/v1.10.1/lib/kafkajs/_consumer.js
Nguồn: https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md
kafka-go: `SessionTimeout` mặc định 30 giây, `HeartbeatInterval` 3 giây, `RebalanceTimeout` 30 giây, `JoinGroupBackoff` 5 giây.
Nguồn: https://raw.githubusercontent.com/segmentio/kafka-go/v0.4.51/reader.go
Nguồn: https://raw.githubusercontent.com/segmentio/kafka-go/v0.4.51/consumergroup.go
Nhóm mới lập có độ trễ ban đầu: broker `group.initial.rebalance.delay.ms` mặc định 3000 ms (coordinator chờ thêm consumer vào trước lần rebalance đầu), nên consumer đầu tiên của một group mới mất khoảng 3 giây mới bắt đầu nhận partition trừ khi đặt về 0 như trong compose mẫu.
Nguồn: https://kafka.apache.org/43/configuration/broker-configs/
Chưa xác minh bằng đo thực tế: thời gian rebalance tổng (từ lúc phát hiện lỗi tới lúc consumer còn lại nhận partition) còn phụ thuộc thời điểm heartbeat kế tiếp của các thành viên, vì heartbeat là kênh báo "rebalance đang diễn ra" (KIP-735 nêu rõ heartbeat phục vụ phát hiện rebalance).
Nguồn: https://cwiki.apache.org/confluence/display/KAFKA/KIP-735%3A+Increase+default+consumer+session+timeout
Eager (stop-the-world): khi rebalance bắt đầu mọi thành viên thu hồi toàn bộ partition đang giữ rồi chờ assignment mới, để đảm bảo mỗi partition chỉ thuộc đúng một consumer tại mọi thời điểm.
Cooperative (KIP-429): chỉ thu hồi các partition thật sự phải đổi chủ và chạy thêm một vòng rebalance cho phần đó; assignor phải "sticky" vì nếu assignment mới khác hoàn toàn thì cooperative suy biến thành eager.
Nguồn: https://cwiki.apache.org/confluence/display/KAFKA/KIP-429%3A+Kafka+Consumer+Incremental+Rebalance+Protocol
Java client mặc định `partition.assignment.strategy=[RangeAssignor, CooperativeStickyAssignor]`: dùng RangeAssignor (eager) và cho phép nâng cấp lên CooperativeStickyAssignor chỉ với một rolling bounce.
Nguồn: https://kafka.apache.org/43/configuration/consumer-configs/
Cooperative và eager không được trộn trong cùng danh sách strategy của librdkafka; các strategy librdkafka hỗ trợ là `range`, `roundrobin`, `cooperative-sticky`, mặc định `range,roundrobin` (eager).
Nguồn: https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md
kafka-javascript: `partitionAssigners` mặc định `[roundRobin]`, hỗ trợ range, roundRobin, cooperativeSticky, không hỗ trợ assignor tùy biến; có `consumer.rebalanceProtocol()` trả "NONE", "COOPERATIVE" hoặc "EAGER".
Nguồn: https://github.com/confluentinc/confluent-kafka-javascript/blob/master/MIGRATION.md
Nguồn: https://raw.githubusercontent.com/confluentinc/confluent-kafka-javascript/v1.10.1/lib/kafkajs/_consumer.js
kafka-go chỉ có ba GroupBalancer ở phía client là `RangeGroupBalancer`, `RoundRobinGroupBalancer`, `RackAffinityGroupBalancer` (mặc định Range rồi RoundRobin), tất cả đều là giao thức classic eager; không có cooperative-sticky và không có giao thức consumer KIP-848 trong v0.4.51.
Nguồn: https://raw.githubusercontent.com/segmentio/kafka-go/v0.4.51/groupbalancer.go
Nguồn: https://raw.githubusercontent.com/segmentio/kafka-go/v0.4.51/reader.go
KIP-848 (giao thức consumer mới) GA từ Kafka 4.0: rebalance hoàn toàn incremental, không còn global synchronization barrier, broker điều khiển heartbeat và session timeout, và assignment do server quyết định.
Nguồn: https://kafka.apache.org/43/operations/consumer-rebalance-protocol/
Phía broker 4.x giao thức này tự bật; phía Java client KHÔNG bật mặc định, phải đặt `group.protocol=consumer` (mặc định `classic`), và dự kiến chỉ đổi mặc định ở Kafka 5.0 theo KIP-1274.
Nguồn: https://kafka.apache.org/43/operations/consumer-rebalance-protocol/
Cấu hình do broker quản lý khi dùng giao thức consumer: `group.consumer.session.timeout.ms` mặc định 45000, `group.consumer.heartbeat.interval.ms` mặc định 5000, assignor server mặc định `uniform` (có `range`).
Nguồn: https://kafka.apache.org/43/configuration/broker-configs/
Nguồn: https://kafka.apache.org/43/operations/consumer-rebalance-protocol/
Khi giao thức consumer bật, `session.timeout.ms`, `heartbeat.interval.ms`, `partition.assignment.strategy` và `enforceRebalance` không còn dùng được ở client Java; client-side assignor tùy biến không được hỗ trợ.
Nguồn: https://kafka.apache.org/43/operations/consumer-rebalance-protocol/
librdkafka hỗ trợ KIP-848 production-ready từ 2.12.0 nhưng mặc định `group.protocol=classic`; với giao thức consumer thì dùng `group.remote.assignor` thay cho `partition.assignment.strategy` và không đặt `session.timeout.ms`.
Nguồn: https://github.com/confluentinc/librdkafka/blob/master/CHANGELOG.md
Nguồn: https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md
kafka-javascript 1.10.1 (librdkafka 2.15.1) có chứa mã xử lý `group.protocol` (báo lỗi nếu đặt `sessionTimeout`, `heartbeatInterval` hay `partitionAssigners` khi không phải classic), nên có thể bật KIP-848 qua cấu hình ngoài khối `kafkaJS`; chưa xác minh bằng chạy thật với broker 4.3.1 nên lab nên giữ mặc định classic.
Nguồn: https://raw.githubusercontent.com/confluentinc/confluent-kafka-javascript/v1.10.1/lib/kafkajs/_consumer.js
Static membership (KIP-345, `group.instance.id`) giảm rebalance do restart ngắn: thành viên static không bị loại ngay, partition chỉ được gán lại sau khi hết session timeout.
Nguồn: https://kafka.apache.org/43/configuration/consumer-configs/
Nguồn: https://cwiki.apache.org/confluence/display/KAFKA/KIP-345%3A+Introduce+static+membership+protocol+to+reduce+consumer+rebalances

### Retention và compaction

Chính sách `cleanup.policy` mặc định là `delete`; giá trị hợp lệ là `compact` và `delete`.
Nguồn: https://kafka.apache.org/43/configuration/topic-configs/
Với policy `delete`, `retention.ms` quy định thời gian tối đa giữ log trước khi bỏ các segment cũ, `retention.bytes` mặc định -1 (không giới hạn), broker mặc định giữ 168 giờ (`log.retention.hours`), kiểm tra mỗi 5 phút (`log.retention.check.interval.ms=300000`), segment tối đa 1 GiB (`log.segment.bytes`).
Retention xóa theo segment chứ không theo từng record, và segment đang ghi (active) chỉ bị đóng sau khi đủ lớn hoặc quá `segment.ms` (mặc định 7 ngày), nên dữ liệu có thể sống lâu hơn `retention.ms`; để test retention trong lab phải hạ `segment.ms` hoặc `segment.bytes` (suy ra từ định nghĩa `retention.ms` và `segment.ms`, chưa xác minh bằng chạy thật).
Nguồn: https://kafka.apache.org/43/configuration/topic-configs/
Nguồn: https://kafka.apache.org/43/configuration/broker-configs/
Log compaction bảo đảm Kafka luôn giữ ít nhất giá trị cuối cùng của mỗi key trong một partition, nên log chứa snapshot đầy đủ giá trị cuối của mọi key.
Nguồn: https://kafka.apache.org/43/design/design/
Record có key và payload null là tombstone (delete marker): nó xóa các record trước đó cùng key và chính nó bị dọn khỏi log sau `delete.retention.ms` (mặc định 1 ngày).
Nguồn: https://kafka.apache.org/43/design/design/
Nguồn: https://kafka.apache.org/43/configuration/topic-configs/
Compaction chạy nền bởi log cleaner (bật mặc định) bằng cách recopy các segment, segment đang ghi không bị compaction dù mọi record đã quá cũ, và log đủ điều kiện khi tỷ lệ dirty đạt `min.cleanable.dirty.ratio` (mặc định 0.5).
Nguồn: https://kafka.apache.org/43/design/design/
Nguồn: https://kafka.apache.org/43/configuration/broker-configs/
Offset của record không đổi sau compaction, nên offset có thể có khoảng trống (xem phần offset).
Nguồn: https://kafka.apache.org/43/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html
Offset đã commit của group cũng hết hạn: `offsets.retention.minutes` mặc định 10080 (7 ngày) tính từ lúc group trở nên rỗng (hoặc từ lần commit cuối với consumer dùng manual assignment), sau đó offset bị xóa và group bắt đầu lại theo `auto.offset.reset`.
Nguồn: https://kafka.apache.org/43/configuration/broker-configs/

### Transaction và exactly-once (kiểm tra bắt buộc: phạm vi thật sự)

Kafka có ba mức: at most once, at least once, exactly once; tài liệu chính thức cảnh báo nhiều hệ thống tuyên bố "exactly-once" gây hiểu nhầm vì không áp dụng khi producer hoặc consumer lỗi, có nhiều consumer process, hoặc dữ liệu đã ghi đĩa có thể mất.
Nguồn: https://kafka.apache.org/43/design/design/
Mặc định Kafka cho at-least-once; idempotent producer chỉ đảm bảo gửi lại không tạo bản ghi trùng trong log; transaction cho phép ghi nguyên tử (atomic) vào nhiều partition, hoặc tất cả cùng thành công hoặc không có gì.
Nguồn: https://kafka.apache.org/43/design/design/
Exactly-once trong Kafka bao phủ chuỗi đọc từ Kafka, xử lý, ghi lại vào Kafka: tài liệu nêu rõ "transactional producer and the consumer using read-committed isolation level can be used generally to provide exactly-once delivery when reading, processing and writing data on Kafka topics", và Kafka Streams hỗ trợ exactly-once.
Nguồn: https://kafka.apache.org/43/design/design/
Cơ chế là producer ghi output và cả offset của consumer (qua `sendOffsetsToTransaction`) trong cùng một transaction, nên nếu abort thì offset quay về giá trị cũ và dữ liệu output không hiển thị cho consumer dùng `read_committed`.
Chỉ producer có tính transactional, consumer và producer là hai thành phần riêng biệt.
Nguồn: https://kafka.apache.org/43/design/design/
Cấu hình bắt buộc: producer đặt `transactional.id` (kéo theo idempotence), consumer đặt `isolation.level=read_committed` và `enable.auto.commit=false`; nên dùng một producer cho mỗi consumer instance khi kết hợp với rebalancing.
Nguồn: https://kafka.apache.org/43/design/design/
Mặc định `isolation.level=read_uncommitted` vẫn trả cả record của transaction đã abort và chưa kết thúc; với `read_committed` consumer chỉ đọc tới last stable offset (LSO) và bị chặn bởi transaction đang mở, nên một transaction treo làm tăng lag của consumer `read_committed`.
Nguồn: https://kafka.apache.org/43/configuration/consumer-configs/
Khi transaction abort, ứng dụng phải tự đặt lại position của consumer (seek về offset đã commit) để xử lý lại các record của transaction đó, vì consumer không tự rewind.
Nguồn: https://kafka.apache.org/43/design/design/
`transaction.timeout.ms` của producer mặc định 60000 ms, nếu lớn hơn `transaction.max.timeout.ms` của broker (mặc định 900000) thì request thất bại; transaction quá hạn bị coordinator abort.
Nguồn: https://kafka.apache.org/43/configuration/producer-configs/
Nguồn: https://kafka.apache.org/43/configuration/broker-configs/
Exactly-once KHÔNG bao phủ: ghi vào hệ thống bên ngoài Kafka (database, HTTP API, email); tài liệu nêu "Exactly-once delivery for other destination systems generally requires cooperation with such systems", và cách tổng quát là lưu offset cùng chỗ với output hoặc làm idempotent theo key.
Nguồn: https://kafka.apache.org/43/design/design/
Cũng không bao phủ side effect trong code xử lý (gọi API, ghi file) vì khi rebalance hoặc abort, bước xử lý có thể chạy lại; Confluent nhắc rằng hệ thống chỉ rewind offset Kafka mà không rollback được state ở hệ thống ngoài thì kết quả sai nếu cập nhật không idempotent.
Nguồn: https://www.confluent.io/blog/exactly-once-semantics-are-possible-heres-how-apache-kafka-does-it/
Từ Kafka 4.0, KIP-890 (server-side defense) làm producer 4.0 bump epoch ở mỗi transaction để chống record trùng lọt sang transaction kế tiếp; đây là cải thiện độ bền của giao thức, không mở rộng phạm vi exactly-once ra ngoài Kafka.
Nguồn: https://kafka.apache.org/43/getting-started/upgrade/
Mặc định transaction cần cụm tối thiểu 3 broker, ở môi trường dev phải hạ `transaction.state.log.replication.factor` (và `transaction.state.log.min.isr`) như trong compose single-node.
Nguồn: https://kafka.apache.org/43/configuration/producer-configs/
Nguồn: https://kafka.apache.org/43/configuration/broker-configs/
kafka-javascript hỗ trợ transaction: `transactionalId` (tự bật `idempotent`), `producer.transaction()` rồi `send`, `sendOffsets` (truyền chính đối tượng consumer, bỏ `consumerGroupId`), `commit` hoặc `abort`; producer transactional không thể `send` ngoài một transaction; consumer có `readUncommitted` (mặc định `false`, tức đọc `read_committed`).
Nguồn: https://github.com/confluentinc/confluent-kafka-javascript/blob/master/MIGRATION.md
Nguồn: https://raw.githubusercontent.com/confluentinc/confluent-kafka-javascript/v1.10.1/lib/kafkajs/_producer.js
kafka-go v0.4.51: `Writer` không expose API transaction hay idempotence; `ReaderConfig.IsolationLevel` có tồn tại để chọn mức nhìn thấy record transactional, nên lab Go chỉ đọc được dữ liệu transactional chứ không tạo được (xem mục chưa xác minh trong phần kết).
Nguồn: https://raw.githubusercontent.com/segmentio/kafka-go/v0.4.51/reader.go
Nguồn: https://raw.githubusercontent.com/segmentio/kafka-go/v0.4.51/writer.go

### KRaft, không còn ZooKeeper (kiểm tra bắt buộc)

Kết luận: ZooKeeper mode đã bị gỡ từ Kafka 4.0, và vẫn đúng ở 4.3: ghi chú nâng cấp nói "Apache Kafka 4.0 only supports KRaft mode - ZooKeeper mode has been removed" và lặp lại cho 4.3.
Cluster ở ZooKeeper mode phải migrate sang KRaft trước khi nâng cấp lên 4.x; KRaft được coi là production-ready từ 3.3, và với cluster KRaft cũ hơn 3.3 thì nên lên 3.9.x trước.
Nguồn: https://kafka.apache.org/43/getting-started/upgrade/
Mỗi server KRaft đặt `process.roles` là `broker`, `controller` hoặc cả hai; server vừa broker vừa controller gọi là combined, đơn giản cho môi trường dev nhưng "not recommended in critical deployment environments", vì không thể roll hay scale controller tách khỏi broker.
Nguồn: https://kafka.apache.org/43/operations/kraft/
Controller tham gia metadata quorum, cần đa số controller sống để cluster khả dụng (3 controller chịu 1 lỗi, 5 controller chịu 2 lỗi), mọi node tìm active controller qua `controller.quorum.bootstrap.servers`.
Cấu hình `controller.quorum.voters` là cách cũ (static) và không nên đặt khi dùng dynamic quorum; compose mẫu chính thức của image 4.3 vẫn dùng `KAFKA_CONTROLLER_QUORUM_VOTERS`, còn `server.properties` mặc định dùng `controller.quorum.bootstrap.servers`.
Nguồn: https://kafka.apache.org/43/operations/kraft/
Nguồn: https://kafka.apache.org/43/configuration/broker-configs/
Nguồn: https://github.com/apache/kafka/blob/4.3/docker/examples/docker-compose-files/single-node/plaintext/docker-compose.yml
Image Docker cần `CLUSTER_ID` (mặc định image tự đặt nếu không truyền), tạo giá trị mới bằng `bin/kafka-storage.sh random-uuid`.
Nguồn: https://github.com/apache/kafka/blob/4.3/docker/resources/common-scripts/configureDefaults

### Consumer lag

Lag của một partition là `LOG-END-OFFSET` trừ `CURRENT-OFFSET` (offset đã commit của group); xem bằng `bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group <group>`.
Nguồn: https://kafka.apache.org/43/operations/basic-kafka-operations/
Đặt lại offset của group bằng `kafka-consumer-groups.sh --reset-offsets` với các kịch bản như `--to-earliest`, `--to-datetime`; mặc định chỉ hiển thị kế hoạch, thêm `--execute` mới thực thi, và các consumer của group phải ngừng hoạt động trước.
Nguồn: https://kafka.apache.org/43/operations/basic-kafka-operations/
Với kafka-go khi dùng `GroupID`, `Reader.Lag()` luôn trả -1 nên lab Go phải tính lag bằng công cụ CLI hoặc admin API thay vì `Lag()`.
Nguồn: https://raw.githubusercontent.com/segmentio/kafka-go/v0.4.51/README.md

## Lỗi thường gặp ở production

Mỗi mục dưới đây có triệu chứng, nguyên nhân và cách sửa; triệu chứng nào do tôi suy ra từ tài liệu mà chưa chạy thật thì được ghi "chưa xác minh".

### 1. advertised.listeners sai trong Docker

Triệu chứng: client kết nối được bootstrap `localhost:9092` rồi treo hoặc báo lỗi kết nối ở bước sau (chưa xác minh thông báo lỗi cụ thể, tùy client).
Nguyên nhân: broker trả địa chỉ trong `advertised.listeners` cho client, và client sau đó kết nối tới chính địa chỉ đó thay vì địa chỉ bootstrap; nếu địa chỉ là hostname nội bộ container thì host không với tới.
Cách sửa: đặt `KAFKA_ADVERTISED_LISTENERS` là địa chỉ mà client thực sự dùng được, dùng hai listener như compose mẫu (một cho host `localhost:9092`, một cho mạng compose `broker:19092`).
Nguồn: https://www.confluent.io/blog/kafka-listeners-explained/
Nguồn: https://github.com/apache/kafka/blob/4.3/docker/examples/docker-compose-files/single-node/plaintext/docker-compose.yml

### 2. Single node không hạ replication factor của internal topic

Triệu chứng: consumer group hoặc transaction không hoạt động trên một broker (chưa xác minh thông báo lỗi cụ thể, thường liên quan coordinator không khả dụng).
Nguyên nhân: `offsets.topic.replication.factor` và `transaction.state.log.replication.factor` mặc định 3, `transaction.state.log.min.isr` mặc định 2, và tài liệu nêu "Internal topic creation will fail until the cluster size meets this replication factor requirement".
Cách sửa: đặt cả ba về 1 (và `group.initial.rebalance.delay.ms=0` cho test nhanh), như compose mẫu của Apache Kafka.
Nguồn: https://kafka.apache.org/43/configuration/broker-configs/
Nguồn: https://github.com/apache/kafka/blob/4.3/docker/examples/docker-compose-files/single-node/plaintext/docker-compose.yml

### 3. Mất message do acks hoặc min.insync.replicas

Triệu chứng: producer báo thành công nhưng message biến mất sau khi leader chết.
Nguyên nhân: `acks=0/1`, hoặc `acks=all` nhưng ISR co lại còn 1 replica và `min.insync.replicas=1` (mặc định) nên write vẫn được chấp nhận; kafka-go `Writer` mặc định `RequireNone` (không chờ ack).
Cách sửa: replication factor 3, `min.insync.replicas=2`, `acks=all`, giữ `unclean.leader.election.enable=false`, và ở kafka-go đặt `RequiredAcks: kafka.RequireAll`; chấp nhận partition ngừng ghi khi ISR dưới 2.
Nguồn: https://kafka.apache.org/43/design/design/
Nguồn: https://kafka.apache.org/43/configuration/topic-configs/
Nguồn: https://raw.githubusercontent.com/segmentio/kafka-go/v0.4.51/writer.go

### 4. Cùng key rơi vào partition khác nhau giữa các dịch vụ

Triệu chứng: sự kiện cùng key bị xử lý song song hoặc sai thứ tự giữa dịch vụ TypeScript và dịch vụ Go, hoặc test "same key same partition" fail ở Go.
Nguyên nhân: mỗi client dùng hash khác nhau: Java và kafka-javascript (do wrapper đặt) dùng murmur2, librdkafka thuần mặc định CRC32 (`consistent_random`), kafka-go mặc định không hash mà round-robin, `kafka.Hash` dùng FNV-1a.
Cách sửa: thống nhất một thuật toán cho mọi producer (ở kafka-go dùng `kafka.Murmur2Balancer{}` để khớp Java/kafka-javascript) và ghi rõ trong hợp đồng của topic.
Nguồn: https://raw.githubusercontent.com/segmentio/kafka-go/v0.4.51/balancer.go
Nguồn: https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md
Nguồn: https://raw.githubusercontent.com/confluentinc/confluent-kafka-javascript/v1.10.1/lib/kafkajs/_producer.js

### 5. Tăng số partition làm vỡ quan hệ key-partition và có thể mất message

Triệu chứng: sau khi tăng partition, cùng key chuyển sang partition khác nên thứ tự theo key bị phá; với `auto.offset.reset=latest`, message vào partition mới có thể bị bỏ qua.
Nguyên nhân: partition được chọn bằng hash của key modulo số partition (xem `balancer.go`), và tài liệu nêu việc đổi số partition khi đặt `latest` có thể gây mất message vì producer ghi vào partition mới trước khi consumer reset offset.
Cách sửa: tính số partition dư từ đầu, không đổi số partition của topic dùng key để giữ thứ tự, hoặc tạo topic mới và migrate; nếu buộc phải tăng thì dùng `earliest` cho consumer.
Nguồn: https://kafka.apache.org/43/configuration/consumer-configs/
Nguồn: https://raw.githubusercontent.com/segmentio/kafka-go/v0.4.51/balancer.go

### 6. Auto commit gây mất hoặc lặp xử lý

Triệu chứng: crash giữa chừng thì message bị bỏ lỡ (commit trước khi xử lý) hoặc xử lý lặp (xử lý xong nhưng chưa commit).
Nguyên nhân: auto commit coi record là đã consumed ngay khi `poll()` trả về, và commit theo chu kỳ 5 giây; commit thủ công sau khi xử lý cho at-least-once, nên consumer phải idempotent.
Cách sửa: tắt auto commit khi xử lý có side effect, commit sau khi xử lý xong, và khử trùng theo khóa nghiệp vụ; với kafka-javascript nhớ commit offset + 1 và commit trong `rebalance_cb` khi bị thu hồi partition.
Nguồn: https://kafka.apache.org/43/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html
Nguồn: https://github.com/confluentinc/confluent-kafka-javascript/blob/master/INTRODUCTION.md

### 7. Xử lý quá lâu vượt max.poll.interval.ms, rebalance liên tục

Triệu chứng: consumer bị đá khỏi group, `CommitFailedException` khi commit, partition chuyển qua lại giữa các consumer, message bị xử lý lặp.
Nguyên nhân: không gọi `poll()` trong `max.poll.interval.ms` (mặc định 5 phút) thì client chủ động rời group; heartbeat vẫn gửi nhưng không có tiến triển được coi là livelock.
Cách sửa: giảm lượng việc mỗi lần poll (`max.poll.records`), tăng `max.poll.interval.ms`, hoặc tách xử lý nặng sang worker; với kafka-javascript thời gian xử lý `eachMessage` hoặc `eachBatch` không được vượt `rebalanceTimeout` (cũng là max poll interval), ở librdkafka nên đặt `enable.auto.offset.store=false` và store offset sau khi xử lý xong.
Nguồn: https://kafka.apache.org/43/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html
Nguồn: https://github.com/confluentinc/confluent-kafka-javascript/blob/master/MIGRATION.md
Nguồn: https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md

### 8. Rebalance storm khi deploy rolling hoặc consumer chập chờn

Triệu chứng: mỗi lần restart một instance, toàn group dừng đọc rồi mới chạy lại; lỗi giả gây hai rebalance liên tiếp.
Nguyên nhân: giao thức classic eager thu hồi mọi partition trong mỗi rebalance; `session.timeout.ms` quá ngắn khiến lỗi mạng thoáng qua bị coi là consumer chết.
Cách sửa: dùng cooperative-sticky (`CooperativeStickyAssignor` hoặc `cooperative-sticky` của librdkafka), static membership (`group.instance.id`) cho restart nhanh, giữ session timeout hợp lý (Java mặc định 45 giây vì KIP-735), hoặc chuyển sang giao thức KIP-848 (`group.protocol=consumer`) nếu client hỗ trợ; kafka-go chưa có cooperative hay KIP-848 nên chỉ giảm bằng cách giảm số lần restart.
Nguồn: https://cwiki.apache.org/confluence/display/KAFKA/KIP-429%3A+Kafka+Consumer+Incremental+Rebalance+Protocol
Nguồn: https://cwiki.apache.org/confluence/display/KAFKA/KIP-735%3A+Increase+default+consumer+session+timeout
Nguồn: https://kafka.apache.org/43/operations/consumer-rebalance-protocol/
Nguồn: https://raw.githubusercontent.com/segmentio/kafka-go/v0.4.51/groupbalancer.go

### 9. Retry không idempotent làm đảo thứ tự hoặc trùng

Triệu chứng: record trong cùng partition bị đảo thứ tự hoặc xuất hiện hai lần sau lỗi tạm thời.
Nguyên nhân: tắt idempotence mà `max.in.flight.requests.per.connection > 1` kèm retry; librdkafka và kafka-javascript mặc định `enable.idempotence=false` (khác Java client 4.x mặc định bật).
Cách sửa: bật idempotence (`idempotent: true` trong kafka-javascript) với `max.in.flight <= 5` và `acks=all`; kafka-go không có idempotent producer nên phải khử trùng ở consumer.
Nguồn: https://kafka.apache.org/43/configuration/producer-configs/
Nguồn: https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md
Nguồn: https://github.com/confluentinc/confluent-kafka-javascript/blob/master/MIGRATION.md

### 10. Consumer read_committed bị kẹt vì transaction treo

Triệu chứng: lag tăng dù producer vẫn ghi vào partition, consumer `read_committed` không đọc tiếp.
Nguyên nhân: consumer `read_committed` chỉ đọc tới last stable offset, mọi message sau một transaction đang mở bị giữ lại cho tới khi transaction đóng (commit, abort hoặc bị coordinator abort sau `transaction.timeout.ms`, mặc định 1 phút).
Cách sửa: giữ transaction ngắn, đặt `transaction.timeout.ms` hợp lý, giám sát transaction mở, và luôn abort trong khối xử lý lỗi.
Nguồn: https://kafka.apache.org/43/configuration/consumer-configs/
Nguồn: https://kafka.apache.org/43/configuration/producer-configs/

### 11. Tin rằng exactly-once bao phủ cả database hay API bên ngoài

Triệu chứng: bản ghi trong database hoặc email gửi đi bị lặp dù đã bật transaction.
Nguyên nhân: exactly-once chỉ đúng cho chuỗi đọc, xử lý, ghi trong Kafka; ghi ra hệ thống ngoài cần phối hợp với hệ thống đó.
Cách sửa: dùng khóa idempotent ở đích, hoặc lưu offset cùng transaction với output trong hệ thống đích (outbox hoặc bảng offset), như cách Kafka Connect làm với HDFS.
Nguồn: https://kafka.apache.org/43/design/design/

### 12. Offset đã commit hết hạn

Triệu chứng: group không chạy lâu (lớn hơn 7 ngày) khi khởi động lại thì đọc lại từ đầu hoặc bỏ qua message cũ tùy `auto.offset.reset`.
Nguyên nhân: `offsets.retention.minutes` mặc định 10080, đếm từ lúc group trở nên rỗng.
Cách sửa: tăng `offsets.retention.minutes` cho các group hay dừng lâu, đặt `auto.offset.reset` có chủ đích, và giám sát lag.
Nguồn: https://kafka.apache.org/43/configuration/broker-configs/

### 13. Hot partition do key lệch hoặc dùng balancer "consistent"

Triệu chứng: một partition quá tải trong khi các partition khác rảnh.
Nguyên nhân: phân phối key lệch; ở kafka-go và librdkafka các biến thể `consistent` hoặc `Consistent: true` hash key nil hoặc rỗng vào cùng một partition (kafka-go cảnh báo "you run the risk of creating a very hot partition").
Cách sửa: dùng biến thể `_random` hoặc `Consistent: false` (mặc định) cho message không có key, chọn key có độ phân tán cao.
Nguồn: https://raw.githubusercontent.com/segmentio/kafka-go/v0.4.51/balancer.go
Nguồn: https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md

### 14. kafka-go: mặc định dễ gây hiểu nhầm trong test

Triệu chứng: `WriteMessages` đơn lẻ mất tới khoảng 1 giây mới trả về; group mới đọc lại toàn bộ topic; message có key không cùng partition; lỗi bị nuốt khi `Async: true`.
Nguyên nhân: `BatchTimeout` mặc định 1 giây với `BatchSize` 100; `ReaderConfig.StartOffset` mặc định `FirstOffset`; `Balancer` mặc định round-robin; `Async` làm `WriteMessages` không bao giờ block và lỗi bị bỏ qua.
Cách sửa: đặt `BatchTimeout` nhỏ (ví dụ 10 ms) trong lab, đặt `StartOffset` tường minh, đặt `Balancer` tường minh, và không dùng `Async` khi cần đảm bảo.
Nguồn: https://raw.githubusercontent.com/segmentio/kafka-go/v0.4.51/writer.go
Nguồn: https://raw.githubusercontent.com/segmentio/kafka-go/v0.4.51/reader.go

### 15. Dùng kafka-go quá cũ với Kafka 4.x, hoặc hướng dẫn cũ dùng ZooKeeper

Triệu chứng: `JoinGroup` hay `CreateTopics` lỗi phiên bản không được hỗ trợ trên broker 4.0 (đã ghi nhận ở kafka-go v0.4.47 với Kafka 4.0.0: "Received request for api with key 11 (JoinGroup) and unsupported version 1"); hướng dẫn cũ dùng `KAFKA_ZOOKEEPER_CONNECT` hay `--zookeeper` không chạy trên 4.x.
Nguyên nhân: Kafka 4.0 gỡ các phiên bản protocol API cũ và gỡ ZooKeeper mode.
Cách sửa: dùng kafka-go từ v0.4.48 trở lên (PR "Kafka 4.0 support" nằm trong v0.4.48; lab dùng v0.4.51), và cấu hình KRaft như compose mẫu.
Cảnh báo chưa xác minh: PR kafka-go ghi chú chưa chạy CI với Kafka 4.0 do vấn đề bộ nhớ, và tôi chưa chạy v0.4.51 với broker 4.3.1, nên lab cần smoke test thật.
Nguồn: https://github.com/segmentio/kafka-go/issues/1378
Nguồn: https://github.com/segmentio/kafka-go/pull/1384
Nguồn: https://github.com/segmentio/kafka-go/releases/tag/v0.4.48
Nguồn: https://kafka.apache.org/43/getting-started/upgrade/

### 16. Port code KafkaJS sang kafka-javascript

Triệu chứng: lỗi tương thích ngay khi chạy (thư viện cố ý ném lỗi thông tin khi dùng tùy chọn không hỗ trợ).
Nguyên nhân: cấu hình phải bọc trong khối `kafkaJS`; `acks`, `compression`, `timeout` đặt ở cấp producer thay vì từng `send()`; `fromBeginning`, `autoCommit` đặt ở cấp consumer thay vì `subscribe` hoặc `run`; không hỗ trợ `createPartitioner`, `autoCommitThreshold`, assignor tùy biến và `consumer.stop()`.
Cách sửa: theo hướng dẫn migration chính thức.
Nguồn: https://github.com/confluentinc/confluent-kafka-javascript/blob/master/MIGRATION.md

### 17. Topic auto-create chỉ có 1 partition

Triệu chứng: test "spread" hay "nhiều consumer chia partition" không thấy phân tán vì topic chỉ có một partition, và consumer thứ hai ngồi không.
Nguyên nhân: `auto.create.topics.enable=true` mặc định và `num.partitions=1` trong cấu hình mặc định; kafka-javascript có `allowAutoTopicCreation=true` mặc định ở producer và consumer.
Cách sửa: tạo topic tường minh với N partition bằng admin client trước khi chạy lab.
Nguồn: https://github.com/apache/kafka/blob/4.3/config/server.properties
Nguồn: https://kafka.apache.org/43/configuration/broker-configs/
Nguồn: https://github.com/confluentinc/confluent-kafka-javascript/blob/master/MIGRATION.md

## Nguồn

https://kafka.apache.org/community/downloads/ 2026-10-06
https://kafka.apache.org/43/getting-started/upgrade/ 2026-10-06
https://registry.npmjs.org/@confluentinc/kafka-javascript 2026-10-06
https://github.com/confluentinc/confluent-kafka-javascript/blob/master/CHANGELOG.md 2026-10-06
https://proxy.golang.org/github.com/segmentio/kafka-go/@latest 2026-10-06
https://kafka.apache.org/42/getting-started/upgrade/ 2026-10-06
https://kafka.apache.org/43/getting-started/docker/ 2026-10-06
https://github.com/apache/kafka/blob/4.3/docker/examples/README.md 2026-10-06
https://github.com/apache/kafka/blob/4.3/config/server.properties 2026-10-06
https://github.com/apache/kafka/blob/4.3/docker/examples/docker-compose-files/single-node/plaintext/docker-compose.yml 2026-10-06
https://kafka.apache.org/43/getting-started/introduction/ 2026-10-06
https://kafka.apache.org/43/design/design/ 2026-10-06
https://kafka.apache.org/43/configuration/topic-configs/ 2026-10-06
https://kafka.apache.org/43/configuration/broker-configs/ 2026-10-06
https://kafka.apache.org/43/configuration/producer-configs/ 2026-10-06
https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md 2026-10-06
https://raw.githubusercontent.com/confluentinc/confluent-kafka-javascript/v1.10.1/lib/kafkajs/_producer.js 2026-10-06
https://github.com/confluentinc/confluent-kafka-javascript/blob/master/MIGRATION.md 2026-10-06
https://raw.githubusercontent.com/segmentio/kafka-go/v0.4.51/balancer.go 2026-10-06
https://raw.githubusercontent.com/segmentio/kafka-go/v0.4.51/writer.go 2026-10-06
https://raw.githubusercontent.com/segmentio/kafka-go/v0.4.51/README.md 2026-10-06
https://developer.confluent.io/courses/architecture/consumer-group-protocol/ 2026-10-06
https://kafka.apache.org/43/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html 2026-10-06
https://github.com/confluentinc/confluent-kafka-javascript/blob/master/INTRODUCTION.md 2026-10-06
https://kafka.apache.org/43/configuration/consumer-configs/ 2026-10-06
https://raw.githubusercontent.com/confluentinc/confluent-kafka-javascript/v1.10.1/lib/kafkajs/_consumer.js 2026-10-06
https://raw.githubusercontent.com/confluentinc/confluent-kafka-javascript/v1.10.1/lib/kafkajs/_admin.js 2026-10-06
https://raw.githubusercontent.com/segmentio/kafka-go/v0.4.51/reader.go 2026-10-06
https://cwiki.apache.org/confluence/display/KAFKA/KIP-735%3A+Increase+default+consumer+session+timeout 2026-10-06
https://raw.githubusercontent.com/segmentio/kafka-go/v0.4.51/consumergroup.go 2026-10-06
https://cwiki.apache.org/confluence/display/KAFKA/KIP-429%3A+Kafka+Consumer+Incremental+Rebalance+Protocol 2026-10-06
https://raw.githubusercontent.com/segmentio/kafka-go/v0.4.51/groupbalancer.go 2026-10-06
https://kafka.apache.org/43/operations/consumer-rebalance-protocol/ 2026-10-06
https://github.com/confluentinc/librdkafka/blob/master/CHANGELOG.md 2026-10-06
https://cwiki.apache.org/confluence/display/KAFKA/KIP-345%3A+Introduce+static+membership+protocol+to+reduce+consumer+rebalances 2026-10-06
https://www.confluent.io/blog/exactly-once-semantics-are-possible-heres-how-apache-kafka-does-it/ 2026-10-06
https://kafka.apache.org/43/operations/kraft/ 2026-10-06
https://github.com/apache/kafka/blob/4.3/docker/resources/common-scripts/configureDefaults 2026-10-06
https://kafka.apache.org/43/operations/basic-kafka-operations/ 2026-10-06
https://www.confluent.io/blog/kafka-listeners-explained/ 2026-10-06
https://github.com/segmentio/kafka-go/issues/1378 2026-10-06
https://github.com/segmentio/kafka-go/pull/1384 2026-10-06
https://github.com/segmentio/kafka-go/releases/tag/v0.4.48 2026-10-06
