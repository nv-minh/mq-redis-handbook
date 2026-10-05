# Ghi chú nghiên cứu: RabbitMQ (chương 05)

## Phiên bản

Series 4.3 có patch mới nhất là 4.3.6, theo trang release information (đã đọc ngày 2026-10-06) và GitHub releases (tag v4.3.6, published 2026-09-14).
Nguồn: https://www.rabbitmq.com/release-information và https://github.com/rabbitmq/rabbitmq-server/releases
Trang release information ghi series 4.3 có community support đến 30 Nov 2026.
Vì vậy image `rabbitmq:4.3.6-management` dùng trong repo là patch mới nhất của series 4.3 tại thời điểm viết (tag này tồn tại trên Docker Hub, kiểm tra qua registry API ngày 2026-10-06, last_pushed 2026-10-02).
Nguồn Docker Hub: https://hub.docker.com/_/rabbitmq
Series 4.3 sắp hết community support (30 Nov 2026), nên handbook nên ghi chú rằng người học cần theo dõi series mới hơn khi dùng ngoài lab.
amqplib mới nhất trên npm là 2.2.0 (kiểm tra qua registry.npmjs.org/amqplib/latest ngày 2026-10-06).
Nguồn: https://registry.npmjs.org/amqplib/latest
amqp091-go mới nhất là v1.15.0 (Go module proxy trả về Version v1.15.0, Time 2026-09-15).
Nguồn: https://pkg.go.dev/github.com/rabbitmq/amqp091-go
Thay đổi hành vi từ 4.0 liên quan đến lab:
- Classic queue mirroring đã bị gỡ hoàn toàn từ RabbitMQ 4.0 (deprecated từ 2021), thay bằng quorum queues và/hoặc streams.
  Nguồn: https://www.rabbitmq.com/docs/3.13/ha
- Quorum queue có `delivery-limit` mặc định là 20 từ 4.0 (trước đó không giới hạn, retry vô hạn).
  Nếu không cấu hình dead letter thì message bị drop sau 20 lần.
  Nguồn: https://www.rabbitmq.com/blog/2024/08/28/quorum-queues-in-4.0 và https://www.rabbitmq.com/docs/quorum-queues
- Quorum queue không hỗ trợ global QoS prefetch, chỉ hỗ trợ per-consumer prefetch.
  Nguồn: https://www.rabbitmq.com/docs/quorum-queues
- Loại queue mặc định khi declare không có `x-queue-type` là classic, trừ khi vhost hoặc node đặt `default_queue_type`.
  Chi tiết ở phần Khái niệm.
  Nguồn: https://www.rabbitmq.com/docs/vhosts
- Lab nên luôn truyền `x-queue-type` tường minh (classic hoặc quorum) để không phụ thuộc cấu hình broker.

## Khái niệm bắt buộc

### Ack, nack, reject, redelivery
`basic.ack` xác nhận một delivery, message sau đó bị broker xóa.
`basic.nack` là phần mở rộng của RabbitMQ có cờ `multiple`, còn `basic.reject` là negative ack không có `multiple`.
Với `requeue=true` message được đưa lại vào queue, với `requeue=false` message bị drop hoặc dead-letter nếu queue có DLX.
Message được giao lại có cờ `redelivered=true` (client hiển thị là `redelivered`), lần giao đầu tiên là `false`.
Delivery tag là số nguyên tăng dần và có phạm vi theo channel, nên phải ack trên đúng channel nhận message, ack sai channel hoặc ack hai lần gây channel error `PRECONDITION_FAILED - unknown delivery tag`.
Auto-ack (fire-and-forget) được tài liệu coi là không an toàn và không giới hạn prefetch nên consumer có thể bị quá tải.
Nguồn: https://www.rabbitmq.com/docs/confirms

### Unacked message khi channel hoặc connection đóng
Với manual ack, mọi delivery chưa được ack sẽ tự động được requeue khi channel (hoặc connection) nhận delivery đó bị đóng.
Điều này gồm mất kết nối TCP, consumer process crash và channel-level protocol exception.
Hệ quả: consumer phải idempotent vì sẽ nhận lại message đã xử lý dở.
Nguồn: https://www.rabbitmq.com/docs/confirms
Hủy consumer (basic.cancel) thì không discard cũng không requeue các delivery đang bay, muốn requeue thì phải đóng channel.
Nguồn: https://www.rabbitmq.com/docs/consumers

### Consumer acknowledgement timeout
Giá trị mặc định là 30 phút.
Khi vượt quá, channel bị đóng với `PRECONDITION_FAILED` và toàn bộ delivery đang chờ trên channel đó (của mọi consumer) được requeue.
Nguồn: https://www.rabbitmq.com/docs/consumers

### Prefetch (basic.qos) và fair dispatch
`basic.qos` giới hạn số message chưa ack được giao cho consumer.
Theo spec AMQP 0-9-1, prefetch_count dùng chung cho mọi consumer trên channel, còn RabbitMQ áp dụng riêng cho từng consumer mới trên channel (cờ `global` cho phép đặt cả giới hạn chung).
Giá trị 0 nghĩa là không giới hạn.
Mặc định: nếu client không gọi `basic.qos` thì không có giới hạn (0 = infinite), nhưng broker có thể đặt default prefetch qua `rabbit.default_consumer_prefetch` trong advanced.config (trang docs lấy ví dụ `{false, 250}`), giá trị built-in mặc định của setting này chưa xác minh.
Quorum queue chặn prefetch tối đa ở 2000 ngay cả khi client để unlimited, và không hỗ trợ global QoS.
Khuyến nghị tài liệu: giá trị 100-300 thường cho throughput tối ưu, prefetch 1 là thận trọng nhất nhưng làm giảm throughput đáng kể.
Fair dispatch trong tutorial work queues đạt được bằng prefetch 1 cùng manual ack, tránh round-robin mù khiến worker chậm bị dồn việc.
Nguồn: https://www.rabbitmq.com/docs/consumer-prefetch , https://www.rabbitmq.com/docs/confirms , https://www.rabbitmq.com/docs/quorum-queues

### Publisher confirms, mandatory, unroutable
Client gửi `confirm.select` để bật confirm mode trên channel, broker xác nhận bằng `basic.ack` (hoặc `basic.nack` khi lỗi nội bộ).
Không có message nào vừa được confirm vừa bị nack.
Message không route được vào queue nào vẫn được broker confirm (sau khi exchange xác định không có queue đích), nên confirm không chứng minh message đã vào queue.
Nếu publish với cờ `mandatory`, broker gửi `basic.return` cho publisher TRƯỚC `basic.ack`, đó là cách phát hiện unroutable message.
Message persistent vào durable queue chỉ được confirm sau khi ghi đĩa.
Nguồn: https://www.rabbitmq.com/docs/confirms
Khi `mandatory=false` (mặc định), message unroutable bị discard hoặc đưa sang alternate exchange nếu có, khi `mandatory=true` message được trả về publisher và publisher phải có returned-message handler.
Publish tới exchange không tồn tại gây channel-level exception và channel bị đóng.
Không publish đồng thời trên một channel dùng chung, dùng channel riêng cho mỗi publisher hoặc thread.
Nguồn: https://www.rabbitmq.com/docs/publishers
Kết luận kiểm tra bắt buộc "phát hiện unroutable": publish với `mandatory=true`, đăng ký handler cho `basic.return` (return event), đồng thời bật publisher confirms vì return đến trước ack.
Nguồn: https://www.rabbitmq.com/docs/publishers và https://www.rabbitmq.com/docs/confirms

### Mô hình AMQP 0-9-1: connection, channel, exchange, queue, binding
Connection AMQP 0-9-1 thường sống lâu và chạy trên TCP, channel là "lightweight connection" dùng chung một TCP connection.
Binding là rule exchange dùng để route message tới queue, routing key đóng vai trò filter.
Default exchange là direct exchange không tên (chuỗi rỗng) do broker khai báo sẵn, mọi queue tự động bind vào nó với routing key bằng tên queue.
Direct exchange route khi routing key khớp chính xác, fanout route tới mọi queue bound và bỏ qua routing key, topic route theo pattern, headers route theo header với `x-match` là `all` (mọi giá trị phải khớp) hoặc `any` (một giá trị khớp là đủ).
Nếu không route được vào queue nào, message bị drop hoặc trả về publisher tùy thuộc thuộc tính message (mandatory).
Queue có các thuộc tính name, durable, exclusive, auto-delete và arguments (message TTL, length limit...).
Khai báo lại queue với thuộc tính khác bản đang có gây channel exception 406 `PRECONDITION_FAILED`, khai báo cùng thuộc tính thì idempotent.
Tên queue tối đa 255 byte UTF-8, tên bắt đầu bằng `amq.` được dành cho broker.
Nguồn: https://www.rabbitmq.com/tutorials/amqp-concepts và https://www.rabbitmq.com/docs/queues

### Work queues
Mặc định broker giao message round-robin cho consumer kế tiếp.
Cần manual ack, queue `durable: true` và message `persistent: true` để sống sót qua restart, nhưng tutorial cảnh báo persistent vẫn có khoảng thời gian ngắn broker đã nhận mà chưa ghi đĩa, muốn chắc chắn thì dùng publisher confirms.
Quên ack khiến message bị giao lại khi client thoát và broker tốn thêm RAM vì không giải phóng được unacked message, theo dõi bằng `messages_unacknowledged`.
Fair dispatch dùng `prefetch(1)`, nếu mọi worker bận thì queue có thể đầy.
Nguồn: https://www.rabbitmq.com/tutorials/tutorial-two-javascript

### Topic routing
Routing key của topic exchange là danh sách từ phân tách bằng dấu chấm, tối đa 255 byte.
`*` thay thế đúng một từ, `#` thay thế không hoặc nhiều từ.
Binding chỉ có `#` nhận mọi message như fanout, binding không có wildcard hoạt động như direct.
Message không khớp binding nào bị discard (ví dụ `quick.orange.new.rabbit` không khớp `*.orange.*`).
Nguồn: https://www.rabbitmq.com/tutorials/tutorial-five-javascript

### Kiểm tra bắt buộc: quorum queue so với classic mirrored queue
Classic queue mirroring bị gỡ hoàn toàn từ RabbitMQ 4.0 (deprecated từ 2021), quorum queues và streams là hai cấu trúc dữ liệu replicated còn lại.
Tài liệu khuyến nghị dùng quorum queue và/hoặc stream thay cho mirrored classic queue.
Kết luận: lab trên 4.3.6 không thể dùng mirrored queue, mọi kịch bản HA/replication phải dùng quorum queue.
Nguồn: https://www.rabbitmq.com/docs/3.13/ha và https://www.rabbitmq.com/docs/quorum-queues
Khác biệt quorum so với classic (feature matrix): quorum không hỗ trợ non-durable queue, không hỗ trợ exclusive queue, không hỗ trợ server-named queue, không có global QoS, message luôn persistent, có poison message handling, consumer timeout, delayed retry (hai tính năng sau từ 4.3).
Nguồn: https://www.rabbitmq.com/docs/quorum-queues

### Kiểm tra bắt buộc: loại queue mặc định
Quorum queue phải được khai báo bằng argument `x-queue-type` = `quorum`, loại mặc định là classic.
Khi client declare không có `x-queue-type`, broker dùng default queue type có thể cấu hình (vhost metadata hoặc `default_queue_type` trong rabbitmq.conf, vhost ưu tiên hơn node), và setting này chỉ áp dụng cho queue khai báo mới vì queue type là immutable.
Trang quorum-queues ghi nguyên văn: "To declare a quorum queue set the x-queue-type queue argument to quorum (the default is classic)", và argument này không thể đặt hay đổi bằng policy vì queue type phải được chỉ định lúc declare.
Kết luận: classic là mặc định, quorum KHÔNG phải mặc định, phải khai báo bằng `x-queue-type: quorum` trừ khi vhost hoặc node đã đặt `default_queue_type`, nên lab phải truyền `x-queue-type` tường minh.
Declare lại cùng tên với type khác gây 406 PRECONDITION_FAILED (suy ra từ quy tắc declare idempotent của trang queues và việc queue type là immutable, chưa thử thực nghiệm).
Consume từ quorum queue trên channel đã bật global QoS gây channel error.
Nguồn: https://www.rabbitmq.com/docs/vhosts và https://www.rabbitmq.com/docs/quorum-queues

### Kiểm tra bắt buộc: delivery-limit, x-delivery-count, x-acquired-count (quorum queue)
Từ RabbitMQ 4.0 delivery-limit mặc định là 20, đặt lại hành vi cũ không giới hạn bằng `x-delivery-limit=-1` (không khuyến nghị), hoặc dùng policy key `delivery-limit` (giá trị -1 tắt giới hạn).
Quorum queue gắn header `x-delivery-count` vào message được giao lại, đếm số lần giao thất bại.
Khi message bị giao lại nhiều hơn limit thì bị drop hoặc dead-letter nếu có DLX, và x-death có `reason` là `delivery_limit`.
Từ 4.3, delivery limit dựa trên delivery-count thay vì acquired-count, nên `basic.nack` (kể cả requeue) không tăng delivery-count và không tính vào limit, còn `basic.reject` requeue=true, client crash hoặc connection loss thì có tăng.
Từ 4.3 có header mới `x-acquired-count` đếm số lần message được gán cho consumer, được khuyến nghị để theo dõi số lần assign.
Bảng "when is delivery count incremented" của docs: basic.nack tăng acquired-count nhưng không tăng delivery-count, basic.reject tăng cả hai, client crash hoặc connection loss tăng cả hai, consumer timeout và network partition chỉ tăng acquired-count.
Hệ quả cho lab assert retry count: dùng `basic.reject` (requeue=true) để thấy `x-delivery-count` tăng và thấy dead-letter ở lần thứ limit, đừng dùng `basic.nack` requeue vì không tăng count từ 4.3.
Header `x-delivery-count` chỉ xuất hiện trên message đã được giao lại (lần giao đầu chưa có), chưa xác minh giá trị chính xác ở lần redelivery đầu tiên (0 hay 1), lab phải đo bằng thực nghiệm trên 4.3.6 trước khi assert.
Nguồn: https://www.rabbitmq.com/docs/quorum-queues và https://www.rabbitmq.com/blog/2024/08/28/quorum-queues-in-4.0

### Kiểm tra bắt buộc: hình dạng header x-death (AMQP 0-9-1)
`x-death` là array các table, sắp xếp theo recency (lần dead-letter gần nhất ở phần tử đầu tiên), mỗi phần tử gom theo cặp {queue, reason}.
Trường mỗi phần tử: `queue` (longstr), `reason` (longstr), `count` (long, số lần dead-letter từ queue đó với reason đó), `time` (timestamp, lần đầu), `exchange` (longstr), `routing-keys` (array of longstr), `original-expiration` (longstr, tùy chọn, chỉ có khi message có per-message expiration).
`reason` thuộc `rejected`, `expired`, `maxlen`, `delivery_limit`.
Lần dead-letter đầu tiên thêm các header `x-first-death-queue`, `x-first-death-reason`, `x-first-death-exchange` (không bao giờ đổi) và `x-last-death-queue`, `x-last-death-reason`, `x-last-death-exchange` (cập nhật mỗi lần).
Per-message TTL (`expiration`) bị xóa khỏi message khi dead-letter để không hết hạn lần nữa ở queue kế tiếp, giá trị cũ được giữ trong `original-expiration`.
Header `x-death` do broker thêm chỉ khi dead-letter, không có khi message chỉ bị requeue.
Client: amqplib 2.2.0 giải mã `count` (kiểu `l`) thành JS number và `time` (kiểu `T`) thành object `{ '!': 'timestamp', value: <số> }` (đọc từ source lib/codec.js); amqp091-go v1.15.0 giải mã `count` thành `int64` (read.go case 'l'), `x-death` là `[]interface{}` các `amqp.Table` (`type Table map[string]any`), `time` được giải mã thành `time.Time` (read.go `readTimestamp`, độ phân giải giây), `routing-keys` là array nên là `[]interface{}` (suy ra từ `readArray`, chưa chạy thử).
Cách đếm retry cho lab: tổng `count` của phần tử có `queue` = work queue và `reason` = `rejected` (hoặc `expired` nếu đếm theo queue wait), vì `count` đã được nén theo cặp {queue, reason}.
Nguồn: https://www.rabbitmq.com/docs/dlx (header), https://github.com/amqp-node/amqplib (codec) và https://github.com/rabbitmq/amqp091-go

### Dead letter exchange (DLX)
Message bị dead-letter khi: bị reject/nack với requeue=false, hết TTL, queue vượt length limit, hoặc vượt delivery limit (quorum).
Dead-lettered message được route tới DLX với routing key `x-dead-letter-routing-key` của queue, nếu không đặt thì dùng routing key gốc.
Tài liệu khuyến nghị cấu hình DLX bằng policy thay vì x-arguments cứng, khi cả hai cùng có thì argument thắng.
RabbitMQ phát hiện cycle và drop message nếu không có rejection nào trong cả vòng.
Classic queue dead-letter là at-most-once (không dùng publisher confirms nội bộ), quorum queue hỗ trợ `dead-letter-strategy: at-least-once` với điều kiện `overflow: reject-publish` (với drop-head sẽ rơi về at-most-once), cần một DLX và feature flag `stream_queue`.
Nguồn: https://www.rabbitmq.com/docs/dlx và https://www.rabbitmq.com/docs/quorum-queues

### TTL
Per-message TTL đặt ở thuộc tính `expiration` khi publish, phải là chuỗi biểu diễn số mili giây.
Per-queue message TTL dùng `x-message-ttl` (số nguyên không âm, ms), queue TTL dùng `x-expires` (số nguyên dương).
Message hết hạn chỉ bị loại hoặc dead-letter khi nó tới head của queue, nên message TTL dài đứng trước message TTL ngắn sẽ chặn message sau (với per-message TTL khác nhau trong cùng queue).
Nguồn: https://www.rabbitmq.com/docs/ttl

### Kiểm tra bắt buộc: per-message TTL với quorum queue và delayed retry
Quorum queue hỗ trợ cả queue TTL và message TTL (gồm per-queue message TTL và per-message TTL ở publisher), tốn thêm 16 byte RAM mỗi message khi dùng TTL.
Trang TTL ghi quorum queue dead-letter message hết hạn khi message đó tới head of the queue (cùng quy tắc head-of-queue như classic).
Kết luận: per-message TTL chạy được trên quorum queue nhưng thời điểm hết hạn thực tế phụ thuộc vị trí ở head khi trộn nhiều TTL khác nhau, nên retry tier nên dùng một queue wait cho mỗi mức delay với `x-message-ttl` cố định (mọi message cùng TTL thì head-of-queue không gây vấn đề).
Cách retry kinh điển: work queue có DLX trỏ tới exchange retry, queue wait (không consumer) có `x-message-ttl` = delay và `x-dead-letter-exchange` trỏ ngược về work exchange, consumer reject (requeue=false) khi lỗi, đếm số lần bằng x-death và đẩy sang parking-lot queue khi vượt ngưỡng.
Từ RabbitMQ 4.3 quorum queue có delayed retry native với linear back-off: queue arguments `x-delayed-retry-type` (`disabled` mặc định, `all`, `failed`, `returned`), `x-delayed-retry-min` (ms, bắt buộc khi bật), `x-delayed-retry-max` (ms, tùy chọn), hoặc policy keys `delayed-retry-type`, `delayed-retry-min`, `delayed-retry-max`.
Công thức: delay = min(min_delay * delivery_count, max_delay), và delivery-count chỉ tăng với failure thật (reject, channel crash), nên nack thường chỉ bị delay mức tối thiểu.
Docs ghi đây là cách khuyến nghị thay vì dựa vào delivery limit để bắt poison message, và là tính năng "available as of RabbitMQ 4.3", nên chỉ dùng được trên image 4.3.x trở lên.
Plugin delayed-message-exchange (exchange type `x-delayed-message`, header `x-delay`) đã bị RabbitMQ team ngừng maintain và repo bị archive (GitHub API: archived=true, pushed_at 2026-09-24).
README giải thích plugin dựa trên Mnesia, vốn đã bị gỡ hoàn toàn trong chu kỳ phát triển 4.3.0, và các hạn chế: tin nhắn delay chỉ nằm trên một node nên mất khi node hỏng, không hỗ trợ mandatory, delay tối đa khoảng 2^32 ms.
README đề xuất: từ RabbitMQ 4.4 quorum queue hỗ trợ native message delivery delay, với các phiên bản trước dùng dead lettering kết hợp TTL.
Kết luận: lab trên 4.3.6 không nên dùng plugin delayed-message-exchange, dùng DLX + TTL (mọi phiên bản) hoặc delayed retry native của quorum queue (4.3).
Message delivery delay native của 4.4 chưa phát hành tại thời điểm viết (series mới nhất là 4.3), chưa xác minh chi tiết API, chỉ nên nhắc như hướng tương lai.
Nguồn: https://github.com/rabbitmq/rabbitmq-delayed-message-exchange và https://www.rabbitmq.com/docs/quorum-queues

### Streams (tóm tắt)
Stream là append-only log, nhiều consumer đọc lặp lại cho tới khi message hết hạn (non-destructive consumer semantics), khác với queue xóa message sau ack.
Khai báo bằng `x-queue-type` = `stream` ngay lúc declare (không đặt bằng policy).
Consumer qua AMQP 0-9-1 phải đặt prefetch (`basic.qos`) và manual ack, chọn điểm đọc bằng argument `x-stream-offset` (`first`, `last`, offset số, timestamp, interval).
Retention bằng `x-max-age` (ví dụ `7D`) và `x-max-length-bytes`.
Stream protocol riêng ở cổng 5552 (plugin `rabbitmq_stream`) cho throughput cao hơn AMQP 0-9-1.
Stream không hỗ trợ qua AMQP 0-9-1: non-durable, exclusive, global QoS, TTL, priority, dead-letter exchange.
Trong chương này chỉ giới thiệu, lab chính dùng quorum queue.
Nguồn: https://www.rabbitmq.com/docs/streams

### Ánh xạ API client dùng trong lab (đọc từ source đúng phiên bản)
amqplib 2.2.0 (index.d.ts): `createConfirmChannel()`, `ConfirmChannel.waitForConfirms()`, `channel.prefetch(count, global?)`, `channel.nack(message, allUpTo?, requeue?)`, `channel.reject(message, requeue?)`, và sự kiện `channel.on('return', (message) => ...)` cho mandatory.
amqplib 2.x có thêm connection recovery opt-in qua option `recovery` (từ 1.1.0, thêm `calculateDelay` và `initialMaxRetries` ở 2.2.0), và `heartbeat: 0` từ 2.0.0 nghĩa là tắt heartbeat thay vì dùng giá trị server gợi ý.
Nguồn: https://github.com/amqp-node/amqplib (CHANGELOG.md và index.d.ts trong package npm 2.2.0)
amqp091-go v1.15.0 (channel.go): `Channel.Qos(prefetchCount, prefetchSize, global)`, `Channel.Confirm(noWait)`, `Channel.NotifyPublish(chan Confirmation)`, `Channel.NotifyReturn(chan Return)`, `PublishWithDeferredConfirmWithContext(ctx, exchange, key, mandatory, immediate, msg)`, `Ack/Nack/Reject`.
Nguồn: https://pkg.go.dev/github.com/rabbitmq/amqp091-go

## Lỗi thường gặp ở production

### 1. Quên ack hoặc ack sai channel
Triệu chứng: `messages_unacknowledged` tăng mãi, RAM broker tăng, message "tự giao lại" khi worker thoát, hoặc channel bị đóng với `PRECONDITION_FAILED - unknown delivery tag`.
Sửa: manual ack sau khi xử lý xong, ack đúng channel đã nhận delivery, không ack hai lần, giám sát `messages_unacknowledged`.
Nguồn: https://www.rabbitmq.com/tutorials/tutorial-two-javascript và https://www.rabbitmq.com/docs/confirms

### 2. Vòng requeue vô hạn (poison message)
Triệu chứng: consumer reject/nack với requeue=true, message quay lại head ngay lập tức, CPU và băng thông tăng vọt.
Sửa: không requeue vô điều kiện, dùng DLX (reject requeue=false) kèm queue wait TTL và đếm retry bằng x-death, hoặc dùng quorum queue với delivery-limit (mặc định 20 từ 4.0) và delayed retry (4.3); luôn cấu hình DLX cho quorum queue vì không có DLX thì message bị drop sau khi vượt limit.
Nguồn: https://www.rabbitmq.com/docs/confirms , https://www.rabbitmq.com/docs/quorum-queues , https://www.rabbitmq.com/blog/2024/08/28/quorum-queues-in-4.0

### 3. Retry bằng nack(requeue=true) không tăng delivery-count từ 4.3
Triệu chứng: lab assert `x-delivery-count` hoặc kỳ vọng message bị dead-letter sau 20 lần nhưng nack requeue lặp mãi không bao giờ chạm limit.
Giải thích: từ 4.3 delivery limit dựa trên delivery-count, và `basic.nack` không tăng delivery-count (chỉ `basic.reject`, crash, connection loss tăng).
Sửa: dùng `basic.reject` khi muốn tính vào limit, hoặc dùng `x-acquired-count` để đếm số lần assign.
Nguồn: https://www.rabbitmq.com/docs/quorum-queues

### 4. Consumer chậm bị đóng channel bởi acknowledgement timeout
Triệu chứng: channel bị đóng với `PRECONDITION_FAILED` sau 30 phút (mặc định) khi xử lý tác vụ dài mà chưa ack, mọi delivery đang chờ trên channel đều bị requeue.
Sửa: ack sớm hơn, tách tác vụ dài, hoặc tăng timeout có chủ đích; từ 4.3 quorum queue còn có consumer timeout cấu hình được, với client AMQP 0-9-1 consumer bị cancel (nếu hỗ trợ `consumer_cancel_notify`) hoặc channel bị đóng.
Nguồn: https://www.rabbitmq.com/docs/consumers và https://www.rabbitmq.com/docs/quorum-queues

### 5. Round-robin mù và prefetch không đặt
Triệu chứng: một worker chậm giữ nhiều message trong khi worker khác rảnh, hoặc consumer không giới hạn bị ngập trong RAM.
Sửa: đặt `basic.qos` (prefetch 1 cho tác vụ nặng và đòi hỏi fair dispatch, 100-300 cho throughput), không dùng auto-ack với consumer chậm, nhớ quorum queue không hỗ trợ global QoS và chặn prefetch ở 2000.
Nguồn: https://www.rabbitmq.com/docs/confirms , https://www.rabbitmq.com/docs/consumer-prefetch , https://www.rabbitmq.com/docs/quorum-queues

### 6. Message biến mất vì unroutable
Triệu chứng: publish thành công, broker còn confirm, nhưng queue không nhận được gì (sai routing key, chưa bind, topic pattern sai).
Giải thích: unroutable message vẫn được confirm, mặc định bị discard.
Sửa: publish với `mandatory=true` cộng handler `basic.return` (amqplib `channel.on('return')`, Go `NotifyReturn`) hoặc cấu hình alternate exchange, và nhớ return đến trước ack.
Nguồn: https://www.rabbitmq.com/docs/confirms và https://www.rabbitmq.com/docs/publishers

### 7. Tin rằng persistent + durable là đủ, bỏ qua publisher confirms
Triệu chứng: mất message khi broker crash ngay sau publish.
Giải thích: tutorial cảnh báo vẫn có cửa sổ ngắn broker đã nhận nhưng chưa ghi đĩa, `basic.ack` của confirm cho message persistent vào durable queue chỉ được gửi sau khi ghi đĩa.
Sửa: bật publisher confirms (ưu tiên confirm bất đồng bộ theo luồng thay vì đợi từng message), xử lý `basic.nack`.
Nguồn: https://www.rabbitmq.com/tutorials/tutorial-two-javascript , https://www.rabbitmq.com/docs/confirms , https://www.rabbitmq.com/docs/publishers

### 8. Publish tới exchange không tồn tại hoặc declare lệch thuộc tính
Triệu chứng: channel đột ngột đóng với 404 NOT_FOUND hoặc 406 PRECONDITION_FAILED, mọi thao tác kế tiếp trên channel đó lỗi.
Sửa: declare topology idempotent với đúng thuộc tính (đặc biệt `x-queue-type`, TTL, DLX args, không thể đổi queue type sau khi tạo), mở channel mới sau lỗi; ưu tiên cấu hình DLX bằng policy để đổi được mà không phải xóa queue.
Nguồn: https://www.rabbitmq.com/docs/queues , https://www.rabbitmq.com/docs/publishers , https://www.rabbitmq.com/docs/channels , https://www.rabbitmq.com/docs/dlx

### 9. Channel leak, chia sẻ channel giữa thread
Triệu chứng: RAM và CPU broker tăng dần, tỷ lệ mở/đóng channel trên 100/giây, hoặc lỗi lạ khi nhiều luồng publish chung một channel.
Sửa: dùng connection và channel sống lâu, một channel cho mỗi thread/publisher, không mở channel cho từng message, tách connection publish và consume vì flow control chỉ ảnh hưởng connection publish.
Nguồn: https://www.rabbitmq.com/docs/channels , https://www.rabbitmq.com/docs/connections , https://www.rabbitmq.com/docs/publishers

### 10. Dead-letter loop và mất message khi DLX
Triệu chứng: message vòng giữa các queue rồi biến mất, hoặc dead-lettered message mất khi node/queue đích không sẵn sàng.
Giải thích: RabbitMQ drop message nếu phát hiện cycle mà không có rejection trong cả vòng, dead-letter mặc định là at-most-once.
Sửa: với dữ liệu quan trọng dùng quorum queue với `dead-letter-strategy: at-least-once` kèm `overflow: reject-publish` (drop-head làm rơi về at-most-once), và đặt `x-dead-letter-routing-key` rõ ràng để tránh vòng với default exchange.
Nguồn: https://www.rabbitmq.com/docs/dlx và https://www.rabbitmq.com/docs/quorum-queues

### 11. TTL head-of-queue và mất TTL gốc sau dead-letter
Triệu chứng: message TTL ngắn không hết hạn đúng giờ vì đứng sau message TTL dài trong cùng queue; message quay lại work queue không còn expiration.
Sửa: dùng một queue wait cho mỗi mức delay với `x-message-ttl` cố định, nhớ TTL gốc bị xóa khi dead-letter (chỉ còn trong `original-expiration` của x-death).
Nguồn: https://www.rabbitmq.com/docs/ttl và https://www.rabbitmq.com/docs/dlx

### 12. Dùng plugin delayed-message-exchange cho delay
Triệu chứng: delay message mất khi node hỏng, không dùng được mandatory, repo đã archive.
Sửa: dùng DLX + TTL hoặc delayed retry native của quorum queue (4.3), không đưa plugin vào lab mới.
Nguồn: https://github.com/rabbitmq/rabbitmq-delayed-message-exchange

### 13. Phụ thuộc queue type mặc định
Triệu chứng: cùng code, ở môi trường khác queue lại là classic hay quorum tùy `default_queue_type` của vhost hoặc node, và redeclare với type khác gây 406.
Sửa: luôn đặt `x-queue-type` tường minh trong code declare.
Nguồn: https://www.rabbitmq.com/docs/vhosts

## Nguồn

https://www.rabbitmq.com/release-information 2026-10-06
https://github.com/rabbitmq/rabbitmq-server/releases 2026-10-06
https://hub.docker.com/_/rabbitmq 2026-10-06
https://registry.npmjs.org/amqplib/latest 2026-10-06
https://pkg.go.dev/github.com/rabbitmq/amqp091-go 2026-10-06
https://github.com/amqp-node/amqplib 2026-10-06
https://github.com/rabbitmq/amqp091-go 2026-10-06
https://www.rabbitmq.com/docs/3.13/ha 2026-10-06
https://www.rabbitmq.com/docs/quorum-queues 2026-10-06
https://www.rabbitmq.com/blog/2024/08/28/quorum-queues-in-4.0 2026-10-06
https://www.rabbitmq.com/docs/queues 2026-10-06
https://www.rabbitmq.com/docs/vhosts 2026-10-06
https://www.rabbitmq.com/docs/confirms 2026-10-06
https://www.rabbitmq.com/docs/consumers 2026-10-06
https://www.rabbitmq.com/docs/consumer-prefetch 2026-10-06
https://www.rabbitmq.com/docs/publishers 2026-10-06
https://www.rabbitmq.com/docs/connections 2026-10-06
https://www.rabbitmq.com/docs/channels 2026-10-06
https://www.rabbitmq.com/docs/dlx 2026-10-06
https://www.rabbitmq.com/docs/ttl 2026-10-06
https://www.rabbitmq.com/docs/streams 2026-10-06
https://www.rabbitmq.com/tutorials/amqp-concepts 2026-10-06
https://www.rabbitmq.com/tutorials/tutorial-two-javascript 2026-10-06
https://www.rabbitmq.com/tutorials/tutorial-five-javascript 2026-10-06
https://github.com/rabbitmq/rabbitmq-delayed-message-exchange 2026-10-06
