# Nghiên cứu NATS và JetStream (chương 07)

## Phiên bản

nats-server v2.15.0 là bản stable mới nhất tại 2026-10-06, phát hành 2026-09-17.
Sau đó mới chỉ có các bản RC (v2.15.1-RC.1 và v2.14.8-RC.1 ngày 2026-09-28), nên chưa có patch stable nào.
Nguồn: https://github.com/nats-io/nats-server/releases/tag/v2.15.0
Docker image nats:2.15.0-alpine tồn tại trên Docker Hub (cập nhật 2026-09-18), dùng với cờ `-js` để bật JetStream.
Nguồn: https://hub.docker.com/_/nats
Client TypeScript: `@nats-io/transport-node` 3.4.0 và `@nats-io/jetstream` 3.4.0 là bản mới nhất trên npm (`npm view` ngày 2026-10-06), phát hành 2026-05-08.
Gói `nats` cũ đã được đổi tên thành repo nats.node; repo nats.js nay là mono-repo v3 gồm core, jetstream, kv, obj, services và các transport (Node/Bun, Deno, WebSocket).
Nguồn: https://github.com/nats-io/nats.js
Client Go: `github.com/nats-io/nats.go` v1.54.0 phát hành 2026-09-18, yêu cầu Go tối thiểu 1.26, và CI chạy với nats-server v2.15.0.
Nguồn: https://github.com/nats-io/nats.go/releases/tag/v1.54.0
Thay đổi gần đây liên quan lab:
- Từ server 2.15: mỗi stream mặc định giới hạn 1000 consumer nếu không đặt `max_consumers` trong stream config hoặc account limits (đặt `-1` để tắt giới hạn, hoặc dùng `default_max_consumers`). Giới hạn này chỉ chặn tạo consumer mới, không xóa consumer cũ. Nguồn: https://github.com/nats-io/nats-server/releases/tag/v2.15.0
- Từ server 2.15: `sync_interval: always` cho replicated stream chỉ sync WAL, không sync các lớp stream phía trên; R1 stream không bị ảnh hưởng. Nguồn: https://github.com/nats-io/nats-server/releases/tag/v2.15.0
- nats.go v1.54.0 sửa lỗi `Messages()` iterator im lặng sau reconnect, thêm các trường info của server 2.15. Nguồn: https://github.com/nats-io/nats.go/releases/tag/v1.54.0
- nats.js v3.4.0 sửa lỗi push consumer bị kẹt ở heartbeat, thêm `resetConsumer()` (cần server 2.14+), message schedules (cần server 2.14+), `getServers()` và `setServers()`. Nguồn: https://github.com/nats-io/nats.js/releases/tag/v3.4.0

## Khái niệm bắt buộc

### JetStream consumer: ack, AckWait, MaxDeliver, backoff

Mỗi message gửi cho consumer có `AckPolicy=explicit` nằm ở trạng thái in flight cho tới khi client trả lời; server giữ nó trong pending list và chạy một timer.
Timer đó là AckWait, mặc định 30 giây; nếu không có ack, nak hay in-progress trước khi hết AckWait thì server coi là worker đã chết và giao lại message (cho cùng worker hoặc worker khác trên cùng consumer).
Nguồn: https://docs.nats.io/learn/jetstream/acknowledgment
Client trả lời đúng một trong bốn cách: ack (xong, không giao lại), nak (thất bại, giao lại ngay hoặc sau một delay do client chọn), term (không bao giờ giao lại message này nữa), in-progress (reset timer AckWait, không phải câu trả lời cuối).
Nguồn: https://docs.nats.io/learn/jetstream/acknowledgment
MaxDeliver mặc định là `-1` (không giới hạn), nghĩa là message không ack có thể bị giao lại mãi mãi.
Nguồn: https://docs.nats.io/learn/jetstream/acknowledgment
Khi một message vượt MaxDeliver, server xóa nó khỏi pending list của consumer và không bao giờ giao lại cho consumer đó; message vẫn nằm trong stream.
JetStream không có dead-letter queue tích hợp sẵn.
Server phát advisory `$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.{stream}.{consumer}` (type `io.nats.jetstream.advisory.v1.max_deliver`, có `stream_seq` và `deliveries`) ngay khi message vượt giới hạn; app phải subscribe advisory này mới biết message bị bỏ.
Nguồn: https://docs.nats.io/learn/jetstream/acknowledgment và https://docs.nats.io/reference/jetstream/advisory/max-deliver
Term chạy qua cùng đường với ack: pending entry bị xóa và ack floor đi qua message; message vẫn ở trong stream với retention `Limits`, nhưng với stream `WorkQueue` hoặc `Interest` thì term xóa message giống như ack.
Server phát advisory `$JS.EVENT.ADVISORY.CONSUMER.MSG_TERMINATED.{stream}.{consumer}` kèm `reason` tùy chọn.
Nguồn: https://docs.nats.io/learn/jetstream/acknowledgment và https://docs.nats.io/reference/jetstream/advisory/terminated
Mỗi nak phát một nak advisory; trang reference ghi subject là `$JS.EVENT.ADVISORY.CONSUMER.MSG_NAK.{stream}.{consumer}` còn trang learn ghi `MSG_NAKED` (hai trang docs không khớp nhau, chưa xác minh subject nào đúng trên server 2.15; lab nên subscribe `$JS.EVENT.ADVISORY.CONSUMER.>` hoặc kiểm tra bằng `nats server` thực tế).
Nguồn: https://docs.nats.io/reference/jetstream/advisory/nak
Kiểm tra bằng source server v2.15.0: hằng số là `JSAdvisoryConsumerMsgNakPre = "$JS.EVENT.ADVISORY.CONSUMER.MSG_NAKED"`, nên subject đúng là `MSG_NAKED` (trang reference ghi `MSG_NAK` là sai); `MAX_DELIVERIES` và `MSG_TERMINATED` khớp docs.
Nguồn: https://github.com/nats-io/nats-server/blob/v2.15.0/server/jetstream_api.go
Source server v2.15.0 (`hasMaxDeliveries`): khi delivery count của một sequence đạt MaxDeliver, server phát advisory đúng một lần, xóa sequence khỏi pending và đẩy ack floor qua nó (message không bao giờ được giao lại cho consumer đó).
Nguồn: https://github.com/nats-io/nats-server/blob/v2.15.0/server/consumer.go
Backoff là danh sách delay theo từng lần giao; nếu list ngắn hơn MaxDeliver thì dùng lại phần tử cuối.
Đặt backoff sẽ thay thế AckWait: phần tử đầu tiên vừa là delay trước lần giao lại đầu tiên vừa là ack deadline của lần giao đầu.
Backoff chỉ định hình các lần giao lại do hết AckWait; nó không làm chậm nak thường (nak trần giao lại ngay), muốn trì hoãn thì client phải nak kèm delay.
Nguồn: https://docs.nats.io/learn/jetstream/acknowledgment
Redelivery không giữ thứ tự stream: consumer có nhiều message in flight cùng lúc (MaxAckPending, mặc định 1000) nên bản giao lại có thể đến sau các message có sequence cao hơn; muốn xử lý đúng thứ tự phải đặt MaxAckPending = 1.
Nguồn: https://docs.nats.io/learn/jetstream/delivery-and-acknowledgment
Ack policy: `explicit` ack từng message; `none` không cần ack (không pending list, không AckWait, không redelivery); `all` một ack xác nhận cả các message trước đó (chỉ hợp với xử lý tuần tự, vì ack message 10 cũng xóa message 7 đang chờ giao lại); `flow_control` là giá trị thứ tư dành cho push consumer nội bộ của mirror và source.
Nguồn: https://docs.nats.io/learn/jetstream/acknowledgment
Ack thường là fire-and-forget; nếu ack bị mất thì hết AckWait message bị giao lại. Double ack (`ackAck()` ở JS, `DoubleAck(ctx)` ở Go) chờ server xác nhận đã ghi ack, tốn thêm một round-trip.
Nguồn: https://docs.nats.io/learn/jetstream/delivery-and-acknowledgment

### JetStream consumer: pull, durable, ephemeral, deliver policy, resume

Consumer là object phía server (một cursor trên stream), không phải một phần của app; vị trí đọc nằm trên server nên client có thể ngắt kết nối rồi nối lại mà không mất chỗ.
Nguồn: https://docs.nats.io/learn/jetstream/reading-back
Resume sau reconnect (đã kiểm tra): durable consumer lưu vị trí gắn với tên của nó, nên sau khi client mất kết nối hoặc server restart rồi bind lại cùng tên thì consumer giao tiếp từ vị trí đã lưu (các message chưa ack vẫn được giao lại sau AckWait).
Consumer ephemeral (không có durable name) không giữ vị trí để quay lại: bị xóa khi idle, và sau reconnect thì đọc lại từ đầu.
Nguồn: https://docs.nats.io/learn/jetstream/reading-back
Durable không bị tự dọn trừ khi đặt `InactiveThreshold`; consumer được coi là inactive khi server không nhận pull request nào (pull consumer) hoặc không phát hiện interest trên deliver subject (push consumer), không phải khi hết message để giao.
Với consumer chỉ đặt `Name` (không `Durable`), nats.go v1.54.0 ghi mặc định `InactiveThreshold` của server là 5 giây trong doc comment của `ConsumerConfig`.
Nguồn: https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/consumer_config.go
Pull consumer: client chủ động xin message. Fetch xin một batch tối đa N message và trả về khi batch đầy hoặc hết timeout (`expires`); Consume tạo luồng liên tục, thư viện tự gửi pull request ở background và gọi handler cho từng message.
Hai trường của một pull request: `batch` (số message tối đa) và `expires` (thời gian server giữ request chờ message); fetch rỗng là bình thường (server trả 408 Request Timeout, hoặc 404 No Messages với no-wait), worker không được coi đó là lỗi.
Nguồn: https://docs.nats.io/learn/jetstream/pull-consumers
Redelivery sau AckWait với pull consumer (đã kiểm tra): cơ chế giống nhau cho mọi consumer; message chưa ack hết AckWait được giao lại cho một reader bất kỳ của cùng consumer (có thể là worker khác), và bản giao lại đi theo thứ tự giao chứ không theo thứ tự stream nên có thể đến sau message có sequence cao hơn. Ví dụ trong docs dùng chính pull consumer `shipping`, sau 30 giây (mặc định) pull lại thì nhận message đó với `tries: 2`.
Nguồn: https://docs.nats.io/learn/jetstream/delivery-and-acknowledgment
MaxAckPending (mặc định 1000, `-1` là không giới hạn) là số message tối đa đang chờ ack mà consumer giao ra trước khi dừng chờ; đặt thấp hơn batch size thì throughput bị chặn; mọi worker dùng chung consumer chia sẻ chung một giới hạn này.
Nguồn: https://docs.nats.io/learn/jetstream/pull-consumers và https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/consumer_config.go
Deliver policy (chọn một lần khi tạo, không sửa được; server báo `deliver policy can not be updated`): `all` (mặc định, từ message đầu tiên), `last` (từ message mới nhất rồi tiếp tục live), `new` (chỉ message đến sau khi tạo consumer), `by_start_sequence` (dùng `opt_start_seq`), `by_start_time` (dùng `opt_start_time`), `last_per_subject` (message mới nhất cho mỗi subject khớp, nền tảng của KV watch).
Nguồn: https://docs.nats.io/learn/jetstream/policies
By start time (đã kiểm tra): chọn message đầu tiên có timestamp >= StartTime, theo comment trong source `DeliverByStartTime will select the first messsage with a timestamp >= to StartTime` và server gọi `store.GetSeqFromTime(OptStartTime)` để đổi thời điểm thành sequence bắt đầu. Timestamp ở đây là thời điểm server lưu message. Hành vi khi không có message nào >= StartTime: chưa xác minh bằng source (suy ra consumer bắt đầu ở message tương lai, chưa kiểm chứng).
Nguồn: https://github.com/nats-io/nats-server/blob/v2.15.0/server/consumer.go
`new` chỉ định nghĩa điểm bắt đầu lịch sử của consumer; durable tạo bằng `new` vẫn lưu vị trí, nên sau restart client đọc tiếp từ vị trí đã lưu (kể cả backlog tích lũy), không nhảy tới live.
Nguồn: https://docs.nats.io/learn/jetstream/policies
Ack policy, deliver policy và replay policy cố định khi tạo consumer; tạo lại consumer thì mất vị trí đã lưu.
Nguồn: https://docs.nats.io/learn/jetstream/policies

### Core NATS: subjects, wildcards, pub/sub, at-most-once

Subject là chuỗi các token ngăn cách bởi dấu `.`; subject phân biệt hoa thường, không được chứa khoảng trắng, tab hay xuống dòng.
Subject bắt đầu bằng `$` là dành riêng cho server (`$SYS`, `$JS`, `$KV`, `$O`, `$SRV`) và `_INBOX` dành cho reply subject do client sinh ra.
Subject không cần khai báo trước; server chỉ giữ entry trong interest graph khi có subscription.
Nguồn: https://docs.nats.io/learn/core-nats/subjects-and-wildcards
Wildcard chỉ dùng ở phía subscriber: `*` khớp đúng một token (không phải 0 hay 2), `>` khớp một hoặc nhiều token và phải là token cuối (`orders.>.created` bị server từ chối).
`orders.>` không khớp subject `orders`; `orders.*.created` không khớp `orders.created`.
Publish tới `orders.*.created` không báo lỗi mà coi `*` là ký tự literal, tạo ra subject lạ.
Nguồn: https://docs.nats.io/learn/core-nats/subjects-and-wildcards
Publish là fire-and-forget: lệnh publish trả về ngay, không chờ subscriber và không cho biết có bao nhiêu subscriber nhận.
Mỗi subscriber khớp subject nhận một bản copy độc lập (fan-out do server thực hiện qua interest graph).
Nguồn: https://docs.nats.io/learn/core-nats/publish-subscribe
No subscriber (đã kiểm tra): publish vào subject không có subscriber vẫn thành công và message bị server bỏ, không có lỗi, không có backlog; publisher không phân biệt được "giao cho 3 subscriber" với "giao cho không ai".
Core NATS là at-most-once: subscriber đang kết nối và quan tâm lúc publish nhận 1 lần, subscriber vắng mặt, chậm hoặc mất kết nối lúc đó nhận 0 lần, không retry, không dedup.
Core NATS không có persistence; muốn giữ message phải dùng JetStream.
Nguồn: https://docs.nats.io/learn/core-nats/publish-subscribe
Client nên flush hoặc drain trước khi thoát, vì publish chỉ ghi vào buffer của client và gửi ở background; default `max_payload` là 1 MB (1048576 byte), client chính thức báo lỗi `nats: maximum payload exceeded` trước khi gửi.
Echo bật mặc định: một connection vừa publish vừa subscribe cùng subject sẽ nhận lại message của chính nó.
Subscriber chậm bị server ngắt (log `Slow Consumer Detected`).
Nguồn: https://docs.nats.io/learn/core-nats/publish-subscribe

### Core NATS: request/reply

Request-reply dùng reply subject tạm dưới prefix `_INBOX.`; client subscribe một wildcard `_INBOX.<connection>.*` một lần cho connection và mỗi request thêm một token riêng cuối cùng.
Mỗi request phải có timeout; hết timeout mà không có reply thì trả lỗi timeout và core NATS không giao câu trả lời trễ.
Khi gửi request tới subject không có subscriber, server trả ngay reply với status `503` (header `NATS/1.0 503`, "no responders") nên client báo lỗi khác với timeout; client cần hỗ trợ header để nhận tín hiệu này.
Nguồn: https://docs.nats.io/learn/core-nats/request-reply

### Core NATS: queue groups

Queue group là tập subscriber cùng subject và cùng tên group; với mỗi message server chọn đúng một member của group và chỉ giao cho member đó.
Server chọn ngẫu nhiên (random index) chứ không round-robin, nên có thể lệch trong vài message; membership động, không cần cấu hình server.
Nguồn: https://docs.nats.io/learn/core-nats/queue-groups
Queue group có đảm bảo đúng một member nhận mỗi message không (đã kiểm tra): chỉ trong phạm vi một group, mỗi message tới đúng một member được chọn ("not zero and not two" là mục tiêu thiết kế). Nhưng đó là at-most-once: nếu member được chọn chết sau khi nhận thì message mất và server không giao lại cho member khác; vì vậy không phải đảm bảo xử lý đúng một lần. Nhiều group khác tên trên cùng subject mỗi group nhận một bản, và subscriber thường (không queue) vẫn nhận mọi message.
Nguồn: https://docs.nats.io/learn/core-nats/queue-groups
Group chỉ chia tải trong phạm vi một subject (sau khi khớp subject mới chọn member); gõ sai tên group tạo ra group thứ hai nên mỗi message bị xử lý hai lần.
Nguồn: https://docs.nats.io/learn/core-nats/queue-groups

### JetStream stream: retention policy

Stream có đúng một retention policy, đặt lúc tạo: `limits` (mặc định; message ở lại tới khi chạm `MaxMsgs`, `MaxBytes` hoặc `MaxAge`, ack không xóa message), `interest` (message bị xóa khi mọi consumer có filter khớp đã ack; subject không có consumer quan tâm thì message bị bỏ ngay), `workqueue` (ack đầu tiên xóa message cho tất cả).
Limits vẫn áp dụng cho cả ba policy, là backstop khi consumer chậm.
Nguồn: https://docs.nats.io/learn/jetstream/retention-policies
Chỉ đổi được `limits` và `interest` qua lại trên stream đang chạy (và việc đổi áp dụng ngay lên message đã lưu, có thể xóa lịch sử); đổi từ hoặc sang `workqueue` bị từ chối với `stream configuration update can not change retention policy to/from workqueue`.
Nguồn: https://docs.nats.io/learn/jetstream/retention-policies
WorkQueue từ chối consumer chồng lấn: consumer thứ hai không filter báo `multiple non-filtered consumers not allowed on workqueue stream`, filter trùng báo `filtered consumer not unique on workqueue stream`; muốn scale thì nhiều worker dùng chung một consumer, không thêm consumer.
Nguồn: https://docs.nats.io/learn/jetstream/retention-policies
Với `interest`, một consumer bị kẹt giữ lại mọi message nó chưa ack nên stream phình tới giới hạn hoặc đầy đĩa; vẫn phải đặt limits.
Nguồn: https://docs.nats.io/learn/jetstream/retention-policies
Storage `file` ghi xuống đĩa và sống sót qua server restart; `memory` chỉ ở RAM và mất khi restart; lựa chọn áp dụng cho cả stream gồm replica, và cố định lúc tạo (đổi báo `stream configuration update can not change storage type`).
Discard `old` (mặc định) xóa message cũ nhất khi chạm limit, `new` từ chối publish mới.
Nguồn: https://docs.nats.io/learn/jetstream/policies
Persist mode mặc định flush xuống storage rồi mới trả PubAck; `async` trả PubAck trước và flush nền, đổi lại có thể mất message đã báo ack khi crash, chỉ nhận cho file storage với 1 replica.
Nguồn: https://docs.nats.io/learn/jetstream/policies

### JetStream: publish, PubAck, dedup bằng Nats-Msg-Id

Publish vào stream trả về `PubAck` gồm tên stream, sequence (bắt đầu từ 1, chỉ tăng) và `duplicate`; `PubAck` là bằng chứng duy nhất rằng message đã được lưu, nhưng không chứng minh có consumer nào đã nhận.
Subject không có stream nào bắt thì publish JetStream lỗi ngay (no responders); timeout nghĩa là không có xác nhận chứ không có nghĩa là không ghi, nên retry cần `Nats-Msg-Id` ổn định.
`nats pub` thường là core publish, in `Published N bytes` dù không có stream nào bắt subject; phải dùng `nats pub --jetstream` hoặc đọc PubAck trong code.
Nguồn: https://docs.nats.io/learn/jetstream/publishing
Dedup (đã kiểm tra): server từ chối lưu hai lần cùng `Nats-Msg-Id` trong duplicate window của stream; mặc định `Duplicate Window` là 2 phút (`2m0s` trong `nats stream info`); publish lặp trả PubAck cùng sequence ban đầu với `duplicate: true`; retry sau hơn 2 phút sẽ lưu bản thứ hai.
Nguồn: https://docs.nats.io/learn/jetstream/publishing và https://docs.nats.io/learn/jetstream/your-first-stream
Nên dùng `Nats-Msg-Id` mà producer tính lại được (order id, request id, hash payload). Cửa sổ dedup là cấu hình của stream chứ không phải header.
Nguồn: https://docs.nats.io/learn/jetstream/publishing
Async publish chồng các round trip để tăng throughput nhưng phải kiểm tra từng PubAck; retry muộn có thể đổi thứ tự (message 3 retry nằm sau 4, 5, 6); dùng `Nats-Expected-Last-Subject-Sequence` để retry sai thứ tự bị từ chối.
Atomic batch publish (`AllowAtomicPublish`, từ server 2.12): mặc định tối đa 1000 message mỗi batch và 50 batch đồng thời; batch im lặng 10 giây bị bỏ và chỉ có advisory `stream_batch_abandoned`.
Nguồn: https://docs.nats.io/learn/jetstream/advanced-publishing
Một subject chỉ thuộc đúng một stream; tên stream không đổi được; muốn giữ subject chồng lấn thì dùng mirror hoặc source.
Nguồn: https://docs.nats.io/learn/jetstream/your-first-stream

### API client mới (đã kiểm tra với package đã cài và source tag v1.54.0)

TypeScript (`@nats-io/transport-node` 3.4.0 + `@nats-io/jetstream` 3.4.0, kiểm tra bằng `npm i` và đọc file `.d.ts`):
- `connect` import từ `@nats-io/transport-node` (package này cũng re-export toàn bộ core, gồm `nanos`, `millis`, `headers`, `NoRespondersError`); `jetstream`, `jetstreamManager`, `AckPolicy`, `DeliverPolicy`, `RetentionPolicy`, `StorageType` import từ `@nats-io/jetstream`.
- Tạo stream: `const jsm = await jetstreamManager(nc); await jsm.streams.add({ name: "ORDERS", subjects: ["orders.>"] })`; cập nhật: `jsm.streams.update(name, cfg)`. Config dùng key snake_case như JSON của server (`duplicate_window`, `max_age`... kiểu `Nanos`, đơn vị nanosecond, dùng `nanos(ms)` để đổi).
- Tạo hoặc cập nhật consumer: `jsm.consumers.add(stream, { durable_name, ack_policy: AckPolicy.Explicit, deliver_policy: DeliverPolicy.All, ack_wait: nanos(30_000), max_deliver: 5, max_ack_pending: 1000 })`; theo docs `add` là idempotent khi gọi lại cùng config; cập nhật dùng `jsm.consumers.update(stream, durable, cfg)`. Không có hàm `createOrUpdate` riêng trong `ConsumerAPI` của 3.4.0 (kiểm tra bằng grep trên `types.d.ts`).
- Tiêu thụ: `const js = jetstream(nc); const c = await js.consumers.get("ORDERS", "shipping")`, rồi `await c.consume()` (async iterator hoặc truyền `callback`), `await c.fetch({ max_messages: 10, expires: 2000 })`, hoặc `await c.next()` (trả về `JsMsg | null`).
- Phản hồi message: `m.ack()`, `m.nak(millis?)` (có delay tùy chọn tính bằng ms), `m.term(reason?)`, `m.working()` (in-progress), `await m.ackAck()` (double ack).
- Publish: `await js.publish(subject, data, { msgID: "..." })` trả về `PubAck` có `stream`, `seq`, `duplicate`; `expect` nhận các `lastSubjectSequence`, `lastMsgID`... của `StreamExpectations`.
Nguồn: https://github.com/nats-io/nats.js/tree/main/jetstream và https://docs.nats.io/learn/jetstream/delivery-and-acknowledgment và https://docs.nats.io/learn/jetstream/acknowledgment

Go (`github.com/nats-io/nats.go` v1.54.0, package `jetstream`, kiểm tra từ source tag v1.54.0):
- `js, err := jetstream.New(nc)`; stream: `js.CreateStream(ctx, jetstream.StreamConfig{...})`, `js.UpdateStream`, `js.CreateOrUpdateStream`.
- Consumer: `js.CreateOrUpdateConsumer(ctx, stream, jetstream.ConsumerConfig{Durable, DeliverPolicy: jetstream.DeliverAllPolicy, AckPolicy: jetstream.AckExplicitPolicy, AckWait, MaxDeliver, BackOff, MaxAckPending})` trả về `Consumer`; `CreateConsumer` trả `ErrConsumerExists` nếu đã tồn tại với config khác (trả consumer cũ nếu config giống); `UpdateConsumer` trả `ErrConsumerDoesNotExist` nếu chưa có; `js.Consumer(ctx, stream, name)` để bind vào consumer đã có.
- Tiêu thụ: `cons.Consume(func(msg jetstream.Msg){...}, opts...)` trả `ConsumeContext` (có `Stop()` và drain); `cons.Messages()` trả iterator; `cons.Fetch(10, jetstream.FetchMaxWait(2*time.Second))` rồi `range batch.Messages()` và kiểm tra `batch.Error()`; `cons.Next()` lấy một message. Option của Consume: `PullMaxMessages`, `PullExpiry`, `ConsumeErrHandler`, `StopAfter`.
- Phản hồi: `msg.Ack()`, `msg.DoubleAck(ctx)`, `msg.Nak()` (giao lại ngay, không theo AckWait hay Backoff), `msg.NakWithDelay(d)`, `msg.InProgress()`, `msg.Term()`, `msg.TermWithReason(reason)` (cần server >= 2.10.4).
- Publish: `js.Publish(ctx, subject, data, jetstream.WithMsgID("..."))` trả `*PubAck` có `Stream`, `Sequence`, `Duplicate`; có `WithExpectLastSequence...`.
- Gói `jetstream` này thay cho API cũ `nc.JetStream()`/`js.Subscribe` trong package `nats`; README dùng API mới làm chuẩn.
Nguồn: https://github.com/nats-io/nats.go/tree/v1.54.0/jetstream và https://pkg.go.dev/github.com/nats-io/nats.go/jetstream

### JetStream: replication và clustering cơ bản

`Replicas: 1` (R=1) là mặc định, không chịu được mất node; R=3 là mức tối thiểu cho production và chịu được mất 1 server vì còn majority (2/3); R=5 là tối đa và chịu được mất 2 server.
Các replica thống nhất thứ tự message bằng Raft (bỏ phiếu đa số); mọi lần ghi đi qua stream leader, PubAck chỉ trả khi majority đã lưu message, nên PubAck trên stream R=3 là cam kết durability.
Khi leader chết, replica còn lại tự bầu leader mới, ghi tạm dừng trong lúc bầu rồi tiếp tục, không mất message đã có PubAck; message đang trên đường chưa tới majority thì mất, client phải retry (kèm `Nats-Msg-Id`).
Mất majority (ví dụ 2 trên 3 server chết) thì không bầu được leader và ghi bị chặn.
Nguồn: https://docs.nats.io/learn/jetstream/surviving-node-loss
Storage `file` là mặc định và bền; `memory` nhanh nhưng mất khi server restart; R=3 memory sống sót khi một server crash nhưng mất hết khi cả group restart cùng lúc; storage là thuộc tính của cả stream.
File storage không sync mọi lần ghi xuống đĩa ngay, nên một ghi đã có PubAck vẫn có thể mất khi OS crash; từ server 2.15 với `sync_interval: always` replicated stream chỉ sync WAL (xem mục Phiên bản).
Nguồn: https://docs.nats.io/learn/jetstream/surviving-node-loss
Consumer cũng được replicate: mặc định kế thừa số replica của stream; trên stream `limits` có thể đặt ít hơn stream nhưng không nhiều hơn; trên stream `interest` và `workqueue` phải bằng stream. Replica không tăng throughput (ghi vẫn qua một leader, consumer cũng có một leader), muốn scale phải thêm worker dùng chung consumer hoặc tách subject sang nhiều stream.
Nguồn: https://docs.nats.io/learn/jetstream/surviving-node-loss
Replica tốn tài nguyên: R=3 xấp xỉ gấp ba lưu trữ và lưu lượng ghi so với R=1; mỗi consumer cũng có leader riêng nằm trên một replica của stream.
Nguồn: https://docs.nats.io/learn/jetstream/surviving-node-loss

### KV và Object Store (ngắn)

Key-Value bucket là một JetStream stream tên `KV_<bucket>` với subject `$KV.<bucket>.>`; key là token cuối của subject, value là message; `put` thêm message, `get` đọc message cuối của subject qua direct get (không mở consumer, không có ack), `watch` mở một consumer.
History là `Maximum Per Subject` của stream; discard policy `New` nên khi chạm limit thì ghi mới bị từ chối; purge dùng rollup; compare-and-swap (`update` theo revision) tránh mất ghi khi đồng thời; TTL theo key hoặc theo bucket.
Nguồn: https://docs.nats.io/learn/key-value và https://docs.nats.io/learn/key-value/under-the-hood
Object Store bucket là stream `OBJ_<bucket>` với hai không gian subject: `$O.<bucket>.C.>` chứa chunk và `$O.<bucket>.M.>` chứa một `ObjectInfo` metadata cho mỗi object (kèm digest SHA-256).
Mỗi lần put metadata mang header `Nats-Rollup: sub` nên chỉ giữ metadata mới nhất, không có lịch sử phiên bản (khác KV).
Nguồn: https://docs.nats.io/learn/object-store và https://docs.nats.io/learn/object-store/under-the-hood

### Clustering cơ bản

Route là kết nối server-với-server trên route port riêng (ví dụ 6222), khác client port (4222); explicit route do cấu hình trong `routes`, implicit route do server tự mở sau khi gossip qua INFO.
Mỗi server chỉ cần một seed address; gossip dựng full mesh, thêm server thứ tư chỉ cần một route tới seed.
Nguồn: https://docs.nats.io/learn/clustering/forming-a-cluster
Quorum commit bảo vệ khỏi mất server, không bảo vệ khỏi mất đĩa: với file storage JetStream không `fsync` mỗi lần ghi mà gom theo `sync_interval` (mặc định 2 phút); đặt `sync_interval: always` thì mỗi ghi được sync trước khi leader trả PubAck, đổi lại ghi chậm nhất.
Nguồn: https://docs.nats.io/learn/clustering/replication-and-r3
Lab dùng một server đơn (`nats:2.15.0-alpine` với `-js -m 8222 -sd /data`), nên stream chỉ có thể R=1; không thể minh họa R=3 trong lab nếu không dựng cluster 3 node (nhận xét về repo, không phải sự kiện từ docs).



## Lỗi thường gặp ở production

Các lỗi dưới đây đều có nguồn; mỗi mục gồm triệu chứng, nguyên nhân, cách sửa.

### 1. Message "biến mất" vì dùng core NATS thay cho JetStream
Triệu chứng: subscriber restart xong không thấy message publish lúc nó vắng mặt; publisher không báo lỗi.
Nguyên nhân: core NATS là at-most-once, publish không có subscriber thì message bị bỏ; `nats pub` thường vẫn in `Published N bytes` dù không có stream nào bắt subject.
Cách sửa: dùng stream, publish bằng JetStream và đọc `PubAck` (hoặc `nats pub --jetstream`).
Nguồn: https://docs.nats.io/learn/core-nats/publish-subscribe và https://docs.nats.io/learn/jetstream/publishing

### 2. Retry publish tạo bản trùng
Triệu chứng: cùng một order xuất hiện hai lần trong stream sau khi publish timeout rồi retry.
Nguyên nhân: timeout không có nghĩa là chưa lưu (chỉ mất ack); không có `Nats-Msg-Id` thì retry lưu bản thứ hai; retry sau hơn duplicate window (mặc định 2 phút) cũng tạo bản trùng dù có Msg-Id.
Cách sửa: gắn `Nats-Msg-Id` ổn định (order id) cho mọi publish có thể retry và giữ khoảng retry trong duplicate window; consumer vẫn cần idempotent vì dedup chỉ phía publish.
Nguồn: https://docs.nats.io/learn/jetstream/publishing

### 3. Quên ack hoặc ack sau AckWait gây redelivery vô hạn hoặc xử lý hai lần
Triệu chứng: cùng message quay lại mãi, `tries` tăng; hoặc hai worker cùng xử lý một order.
Nguyên nhân: MaxDeliver mặc định `-1`; AckWait mặc định 30 giây ngắn hơn thời gian xử lý thật mà worker không gửi in-progress.
Cách sửa: luôn ack trên đường thành công; tăng AckWait hoặc gọi `m.working()` / `msg.InProgress()` cho job dài; làm side effect idempotent theo `order_id`.
Nguồn: https://docs.nats.io/learn/jetstream/acknowledgment và https://docs.nats.io/learn/jetstream/worker-pool

### 4. Poison message ăn hết MaxDeliver và bị bỏ im lặng
Triệu chứng: một message hỏng bị nak liên tục rồi biến mất khỏi consumer, không có dấu vết trong output bình thường.
Nguyên nhân: không có đường term; hết MaxDeliver server xóa message khỏi pending và không có dead-letter queue.
Cách sửa: gọi term khi chắc chắn không bao giờ thành công; đặt MaxDeliver hữu hạn kèm backoff; subscribe `$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.{stream}.{consumer}` và `$JS.EVENT.ADVISORY.CONSUMER.MSG_TERMINATED.{stream}.{consumer}` để lưu lại message lỗi (tự publish sang stream DLQ do app quản lý).
Nguồn: https://docs.nats.io/learn/jetstream/acknowledgment

### 5. Nak trần giao lại ngay, backoff không làm chậm nak
Triệu chứng: lỗi tạm thời (downstream chết) làm message lặp liên tục trong vài mili giây, tốn hết MaxDeliver.
Nguyên nhân: nak không theo AckWait hay Backoff; backoff chỉ áp dụng cho redelivery do hết AckWait.
Cách sửa: dùng nak kèm delay (`m.nak(10_000)` ở TS, `NakWithDelay(10*time.Second)` ở Go).
Nguồn: https://docs.nats.io/learn/jetstream/acknowledgment và https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/message.go

### 6. Redelivery làm đảo thứ tự, ack all làm mất message
Triệu chứng: message 3 đến sau 4 và 5; với `AckPolicy=all` message lỗi bị mất.
Nguyên nhân: bản giao lại đi theo thứ tự giao; ack message 10 trong policy `all` cũng xóa message 7 đang chờ giao lại.
Cách sửa: đặt `MaxAckPending = 1` khi cần đúng thứ tự (đổi lại mất song song); dùng `explicit` cho công việc không được mất.
Nguồn: https://docs.nats.io/learn/jetstream/delivery-and-acknowledgment và https://docs.nats.io/learn/jetstream/acknowledgment

### 7. MaxAckPending quá thấp làm worker nhàn rỗi
Triệu chứng: có 10 worker nhưng chỉ 3 message được xử lý cùng lúc, hoặc batch 100 chỉ nhận về 10.
Nguyên nhân: MaxAckPending (mặc định 1000) dùng chung cho cả consumer, không phải mỗi worker.
Cách sửa: đặt lớn hơn hoặc bằng số worker và batch size.
Nguồn: https://docs.nats.io/learn/jetstream/worker-pool và https://docs.nats.io/learn/jetstream/pull-consumers

### 8. Fetch rỗng bị coi là lỗi
Triệu chứng: worker báo lỗi hoặc crash khi stream im lặng.
Nguyên nhân: fetch trả về rỗng sau `expires` (server trả 408, hoặc 404 No Messages với no-wait) là bình thường; fetch với `expires` bằng 0 ở mức protocol có thể treo (thư viện mặc định khoảng 30 giây).
Cách sửa: coi rỗng là "chưa có việc", lặp lại fetch; đặt `expires` rõ ràng.
Nguồn: https://docs.nats.io/learn/jetstream/pull-consumers

### 9. Stream retention interest hoặc workqueue làm đầy đĩa hoặc bị từ chối
Triệu chứng: stream `interest` phình to; tạo consumer thứ hai trên stream `workqueue` bị lỗi.
Nguyên nhân: consumer chậm giữ message của stream `interest`; workqueue cấm consumer chồng lấn (`multiple non-filtered consumers not allowed on workqueue stream`).
Cách sửa: luôn đặt limits làm backstop và theo dõi consumer; một consumer cho workqueue và nhiều worker dùng chung nó, hoặc filter chia subject không trùng.
Nguồn: https://docs.nats.io/learn/jetstream/retention-policies

### 10. Cấu hình cố định lúc tạo bị đổi sau này
Triệu chứng: `deliver policy can not be updated`, `stream configuration update can not change storage type`, `...can not change retention policy to/from workqueue`.
Nguyên nhân: storage, persist mode, deliver/ack/replay policy và retention sang/từ workqueue không sửa được sau khi tạo; tên stream không đổi được.
Cách sửa: quyết định trước; nếu sai thì tạo object mới và chuyển dữ liệu (mirror hoặc source); tạo lại consumer sẽ mất vị trí đã lưu.
Nguồn: https://docs.nats.io/learn/jetstream/policies và https://docs.nats.io/learn/jetstream/your-first-stream

### 11. Kỳ vọng sai về deliver policy `new` và ephemeral consumer
Triệu chứng: client restart đọc cả backlog dù consumer tạo với `new`; hoặc consumer ephemeral biến mất rồi đọc lại từ đầu.
Nguyên nhân: deliver policy chỉ áp dụng một lần lúc tạo, durable giữ vị trí đã lưu; ephemeral bị xóa khi idle.
Cách sửa: dùng durable cho việc cần resume; muốn bỏ backlog thì tạo consumer mới hoặc dùng deliver policy phù hợp lúc tạo.
Nguồn: https://docs.nats.io/learn/jetstream/policies và https://docs.nats.io/learn/jetstream/reading-back

### 12. Mất message đã có PubAck do durability của storage
Triệu chứng: sau crash OS, message đã báo PubAck biến mất; hoặc stream memory trống sau restart.
Nguyên nhân: file storage chỉ sync theo `sync_interval` (mặc định 2 phút); `memory` mất khi restart; R=1 mất khi mất node.
Cách sửa: R=3 và file storage cho dữ liệu quan trọng, `sync_interval: always` nếu cần cam kết mạnh nhất (chậm hơn), không dùng persist mode `async` cho order log.
Nguồn: https://docs.nats.io/learn/clustering/replication-and-r3 và https://docs.nats.io/learn/jetstream/surviving-node-loss và https://docs.nats.io/learn/jetstream/policies

### 13. Async publish không kiểm tra ack
Triệu chứng: mất ghi im lặng, hoặc retry làm đảo thứ tự trong stream.
Nguyên nhân: không đọc PubAck của async publish; retry muộn nằm sau các message đến sau.
Cách sửa: thu và kiểm tra mọi PubAck; thêm `Nats-Expected-Last-Subject-Sequence` khi thứ tự quan trọng; `Nats-Msg-Id` để chặn trùng.
Nguồn: https://docs.nats.io/learn/jetstream/advanced-publishing

### 14. Queue group: gõ sai tên, kỳ vọng giao đúng một lần hoặc chia đều
Triệu chứng: mỗi order bị xử lý hai lần; một worker nhận liên tiếp nhiều message; worker chết thì order mất.
Nguyên nhân: `packers` và `packer` là hai group khác nhau; chọn ngẫu nhiên chứ không round-robin; core NATS at-most-once, không giao lại.
Cách sửa: dùng đúng một hằng số tên group; với việc không được mất dùng JetStream work queue; xử lý idempotent.
Nguồn: https://docs.nats.io/learn/core-nats/queue-groups

### 15. Slow consumer và flush khi thoát (core NATS)
Triệu chứng: log `Slow Consumer Detected`, subscriber bị ngắt hoặc mất message; publisher ngắn hạn thoát mà message không tới server.
Nguyên nhân: pending buffer đầy (mặc định client Go 500000 message và 64 MB; JS không giới hạn phía client); publish chỉ ghi buffer của client.
Cách sửa: đặt pending limits theo workload, đăng ký async error callback, chuyển việc nặng sang worker; gọi flush hoặc drain trước khi thoát.
Nguồn: https://docs.nats.io/learn/resilient-clients/slow-consumers và https://docs.nats.io/learn/core-nats/publish-subscribe

### 16. Giới hạn mới của server 2.15 và client Go 1.54
Triệu chứng: tạo consumer thứ 1001 trên một stream bị từ chối sau khi nâng cấp lên server 2.15; `go build` lỗi trên Go cũ hơn 1.26 với nats.go v1.54.0.
Nguyên nhân: server 2.15 giới hạn mặc định 1000 consumer mỗi stream; nats.go v1.54.0 nâng yêu cầu Go tối thiểu lên 1.26.
Cách sửa: đặt `max_consumers` trong stream config hoặc `default_max_consumers` (`-1` để tắt); dùng Go >= 1.26 (kiểm tra `go.mod` của lab).
Nguồn: https://github.com/nats-io/nats-server/releases/tag/v2.15.0 và https://github.com/nats-io/nats.go/releases/tag/v1.54.0

### 17. Gói npm `nats` cũ
Triệu chứng: `npm install nats` cài bản 2.29.x và API khác (`nats.connect`, `js.pullSubscribe`).
Nguyên nhân: gói `nats` đã bị deprecate với thông báo `Package moved. Use @nats-io/transport-node` (kiểm tra `npm view nats deprecated` ngày 2026-10-06).
Cách sửa: dùng `@nats-io/transport-node` và `@nats-io/jetstream` 3.4.0.
Nguồn: https://github.com/nats-io/nats.js và https://registry.npmjs.org/nats

### 18. Lỗi client đã sửa gần đây (cần dùng đúng phiên bản)
Triệu chứng: iterator `Messages()` im lặng sau reconnect (Go); push consumer kẹt ở heartbeat (JS).
Nguyên nhân: bug đã sửa trong nats.go v1.54.0 (Messages() sau reconnect) và nats.js v3.4.0 (push consumer heartbeat).
Cách sửa: giữ đúng phiên bản của lab (Go v1.54.0, JS 3.4.0), không hạ cấp.
Nguồn: https://github.com/nats-io/nats.go/releases/tag/v1.54.0 và https://github.com/nats-io/nats.js/releases/tag/v3.4.0

### 19. Docs chính thức có chỗ không khớp
Triệu chứng: subscribe advisory nak theo `MSG_NAK` không nhận gì.
Nguyên nhân: trang reference ghi `MSG_NAK`, source server v2.15.0 dùng `MSG_NAKED`.
Cách sửa: dùng `$JS.EVENT.ADVISORY.CONSUMER.MSG_NAKED.{stream}.{consumer}` hoặc wildcard `$JS.EVENT.ADVISORY.CONSUMER.>`.
Nguồn: https://github.com/nats-io/nats-server/blob/v2.15.0/server/jetstream_api.go và https://docs.nats.io/reference/jetstream/advisory/nak


## Nguồn

https://docs.nats.io/learn/clustering/forming-a-cluster (truy cập 2026-10-06)
https://docs.nats.io/learn/clustering/replication-and-r3 (truy cập 2026-10-06)
https://docs.nats.io/learn/core-nats/publish-subscribe (truy cập 2026-10-06)
https://docs.nats.io/learn/core-nats/queue-groups (truy cập 2026-10-06)
https://docs.nats.io/learn/core-nats/request-reply (truy cập 2026-10-06)
https://docs.nats.io/learn/core-nats/subjects-and-wildcards (truy cập 2026-10-06)
https://docs.nats.io/learn/jetstream/acknowledgment (truy cập 2026-10-06)
https://docs.nats.io/learn/jetstream/advanced-publishing (truy cập 2026-10-06)
https://docs.nats.io/learn/jetstream/delivery-and-acknowledgment (truy cập 2026-10-06)
https://docs.nats.io/learn/jetstream/policies (truy cập 2026-10-06)
https://docs.nats.io/learn/jetstream/publishing (truy cập 2026-10-06)
https://docs.nats.io/learn/jetstream/pull-consumers (truy cập 2026-10-06)
https://docs.nats.io/learn/jetstream/reading-back (truy cập 2026-10-06)
https://docs.nats.io/learn/jetstream/retention-policies (truy cập 2026-10-06)
https://docs.nats.io/learn/jetstream/surviving-node-loss (truy cập 2026-10-06)
https://docs.nats.io/learn/jetstream/worker-pool (truy cập 2026-10-06)
https://docs.nats.io/learn/jetstream/your-first-stream (truy cập 2026-10-06)
https://docs.nats.io/learn/key-value (truy cập 2026-10-06)
https://docs.nats.io/learn/key-value/under-the-hood (truy cập 2026-10-06)
https://docs.nats.io/learn/object-store (truy cập 2026-10-06)
https://docs.nats.io/learn/object-store/under-the-hood (truy cập 2026-10-06)
https://docs.nats.io/learn/resilient-clients/slow-consumers (truy cập 2026-10-06)
https://docs.nats.io/reference/jetstream/advisory/max-deliver (truy cập 2026-10-06)
https://docs.nats.io/reference/jetstream/advisory/nak (truy cập 2026-10-06)
https://docs.nats.io/reference/jetstream/advisory/terminated (truy cập 2026-10-06)
https://github.com/nats-io/nats-server/blob/v2.15.0/server/consumer.go (truy cập 2026-10-06)
https://github.com/nats-io/nats-server/blob/v2.15.0/server/jetstream_api.go (truy cập 2026-10-06)
https://github.com/nats-io/nats-server/releases/tag/v2.15.0 (truy cập 2026-10-06)
https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/consumer_config.go (truy cập 2026-10-06)
https://github.com/nats-io/nats.go/blob/v1.54.0/jetstream/message.go (truy cập 2026-10-06)
https://github.com/nats-io/nats.go/releases/tag/v1.54.0 (truy cập 2026-10-06)
https://github.com/nats-io/nats.go/tree/v1.54.0/jetstream (truy cập 2026-10-06)
https://github.com/nats-io/nats.js (truy cập 2026-10-06)
https://github.com/nats-io/nats.js/releases/tag/v3.4.0 (truy cập 2026-10-06)
https://github.com/nats-io/nats.js/tree/main/jetstream (truy cập 2026-10-06)
https://hub.docker.com/_/nats (truy cập 2026-10-06)
https://pkg.go.dev/github.com/nats-io/nats.go/jetstream (truy cập 2026-10-06)
https://registry.npmjs.org/@nats-io/jetstream (truy cập 2026-10-06)
https://registry.npmjs.org/@nats-io/transport-node (truy cập 2026-10-06)
https://registry.npmjs.org/nats (truy cập 2026-10-06)
