# Redis - ghi chú nghiên cứu cho MQ & Redis Handbook

## Phiên bản

Redis server: các bản stable mới nhất trên GitHub tại ngày 2026-10-06 là 8.10.2, 8.8.3, 8.6.7, 8.4.7 và 8.2.10 (cùng phát hành 2026-09-17).
Nguồn: https://github.com/redis/redis/releases (đã đối chiếu bằng GitHub API releases).
Lab nên pin một tag cụ thể (đề xuất `redis:8.10` hoặc `redis:8.2` nếu muốn tránh tính năng mới), và ghi rõ trong README lab.

ioredis: bản `latest` trên npm là 6.0.0 (publish 2026-07-31), yêu cầu Node.js >= 20.
Nguồn: https://registry.npmjs.org/ioredis
ioredis 6.0.0 dùng RESP3 mặc định, muốn giữ hành vi cũ phải đặt `protocol: 2`.
Nguồn: https://github.com/redis/ioredis/blob/main/CHANGELOG.md
Hệ quả cho lab: reply của một số lệnh (ví dụ map/set) có thể khác hình dạng so với tài liệu ioredis 5, và Pub/Sub trên cùng connection có thể khác; lab cần chạy thử cả hai protocol hoặc cố định `protocol: 2` (chưa xác minh chi tiết khác biệt từng lệnh).

go-redis: bản mới nhất là v9.23.0 (2026-10-05), yêu cầu tối thiểu Go 1.26.
Nguồn: https://github.com/redis/go-redis/releases/tag/v9.23.0 và https://proxy.golang.org/github.com/redis/go-redis/v9/@latest
v9.23.0 thêm pipeline pool riêng cho mỗi client (đặt `PipelinePoolSize: -1` để quay về một pool), `MaxBatchBytes` mặc định 128 KiB cho auto-pipelining, và sửa nhiều lỗi routing cho Cluster.
Hệ quả cho lab Go: `go.mod` phải khai báo `go 1.26` trở lên.

Thay đổi ở Redis server ảnh hưởng tới lab:
- `BLMOVE` có từ 6.2.0 và thay thế `BRPOPLPUSH` (đã deprecated). Nguồn: https://redis.io/docs/latest/commands/blmove/
- `XAUTOCLAIM` có từ 6.2.0, reply 3 phần từ 7.0 (phần thứ ba là danh sách ID đã bị xóa khỏi stream). Nguồn: https://redis.io/docs/latest/commands/xautoclaim/
- `XNACK` mới từ Redis 8.8.0, và trang lệnh ghi "Not supported" cho Redis Software và Redis Cloud, nên lab chỉ nên nhắc như tính năng tùy chọn, không dựng pattern chính trên nó. Nguồn: https://redis.io/docs/latest/commands/xnack/ và https://redis.io/docs/latest/develop/whats-new/8-8/
- Redis 8.10 thêm `LMOVEM`/`BLMOVEM` (di chuyển nhiều phần tử giữa các list) và `MAXCOUNT`/`MAXSIZE` cho `XREAD`/`XREADGROUP`. Nguồn: https://redis.io/docs/latest/develop/whats-new/8-10/
- Redis 8.10: `redis-cli --cluster reshard` và `rebalance` dùng atomic slot migration phía server. Nguồn: https://redis.io/docs/latest/develop/whats-new/8-10/
- Redis 8.10: sửa lỗi ACL bypass cho `XREAD`/`XREADGROUP` (#15478), nên tránh image cũ hơn cho lab về ACL. Nguồn: https://redis.io/docs/latest/develop/whats-new/8-10/

## Khái niệm bắt buộc

### Chương 02 - Core

#### Data types và keyspace

Redis Open Source hiện có các kiểu dữ liệu: Strings (kèm Bitmaps, Bitfields), Arrays, Geospatial, Hashes, JSON, Lists, các kiểu probabilistic (Bloom, Cuckoo, Count-min, HyperLogLog, t-digest, Top-K), Sets, Sorted sets, Streams, Time series, Vector sets.
Nguồn: https://redis.io/docs/latest/develop/data-types/
Kiểu Arrays là mới (Redis 8.8), không cần dạy trong lab. Nguồn: https://redis.io/docs/latest/develop/whats-new/8-8/
Lists là danh sách string sắp theo thứ tự insert, Sets là tập string không trùng và không có thứ tự, Sorted sets giữ thứ tự theo score, Streams là append-only log.
Nguồn: https://redis.io/docs/latest/develop/data-types/
Key là binary-safe, kích thước tối đa 512 MB, nhưng key rất dài là ý tưởng tệ (ví dụ 1024 byte tốn bộ nhớ và so sánh key tốn kém), nên theo schema `object-type:id`.
Nguồn: https://redis.io/docs/latest/develop/using-commands/keyspace/
Expire có độ phân giải 1 ms, thông tin expire được replicate và persist trên đĩa, thời gian "trôi" ngay cả khi server tắt (vì Redis lưu thời điểm hết hạn tuyệt đối).
Nguồn: https://redis.io/docs/latest/develop/using-commands/keyspace/
`KEYS` chặn server cho tới khi trả hết key, không dùng trong code ứng dụng; dùng `SCAN` để duyệt tăng dần (SCAN chỉ đảm bảo hạn chế vì tập key có thể đổi trong lúc duyệt).
Nguồn: https://redis.io/docs/latest/develop/using-commands/keyspace/

#### Persistence RDB và AOF

RDB là snapshot point-in-time, mặc định lưu ở `dump.rdb`, tạo bằng `fork()` (child ghi file tạm rồi thay file cũ, dựa trên copy-on-write); có thể mất vài phút dữ liệu gần nhất nếu crash.
Nguồn: https://redis.io/docs/latest/operate/oss_and_stack/management/persistence/
Save point mặc định trong redis.conf (8.10): 3600 giây nếu >= 1 thay đổi, 300 giây nếu >= 100 thay đổi, 60 giây nếu >= 10000 thay đổi; `save ""` tắt snapshot.
Nguồn: https://raw.githubusercontent.com/redis/redis/8.10/redis.conf
AOF ghi lại mọi lệnh ghi; `appendonly` mặc định `no` trong redis.conf; ba chính sách `appendfsync`: `always`, `everysec` (mặc định và được khuyến nghị, có thể mất khoảng 1 giây), `no` (phó mặc cho OS, Linux thường flush mỗi 30 giây).
Nguồn: https://redis.io/docs/latest/operate/oss_and_stack/management/persistence/ và https://raw.githubusercontent.com/redis/redis/8.10/redis.conf
Từ Redis 7.0, AOF là multi part: một base file (RDB hoặc AOF) cộng các incremental file, quản lý bằng manifest trong thư mục `appenddirname`; rewrite chạy nền bằng `BGREWRITEAOF` hoặc tự động.
Nguồn: https://redis.io/docs/latest/operate/oss_and_stack/management/persistence/
Khi cả AOF và RDB đều bật và Redis restart, AOF được dùng để dựng lại dữ liệu vì nó đầy đủ nhất; tài liệu khuyên dùng cả hai nếu muốn mức an toàn dữ liệu gần với PostgreSQL.
Nguồn: https://redis.io/docs/latest/operate/oss_and_stack/management/persistence/
Chuyển từ RDB sang AOF phải bật bằng `CONFIG SET appendonly yes` trên server đang chạy rồi `CONFIG REWRITE`; chỉ sửa config rồi restart có thể mất dữ liệu.
Nguồn: https://redis.io/docs/latest/operate/oss_and_stack/management/persistence/
Lệnh `BACKUP` mới từ Redis 8.10.0 (dựa trên MP-AOF); chưa cần đưa vào lab.
Nguồn: https://redis.io/docs/latest/operate/oss_and_stack/management/persistence/

#### Eviction

Các giá trị `maxmemory-policy`: `noeviction`, `allkeys-lru`, `allkeys-lrm`, `allkeys-lfu`, `allkeys-random`, `volatile-lru`, `volatile-lrm`, `volatile-lfu`, `volatile-random`, `volatile-ttl`.
Nguồn: https://redis.io/docs/latest/develop/reference/eviction/
Policy mặc định là `noeviction` (dòng `# The default is: maxmemory-policy noeviction` trong redis.conf 8.10); với `noeviction`, lệnh thêm dữ liệu mới trả lỗi còn lệnh chỉ đọc vẫn chạy.
Nguồn: https://raw.githubusercontent.com/redis/redis/8.10/redis.conf và https://redis.io/docs/latest/develop/reference/eviction/
`allkeys-lrm` và `volatile-lrm` chỉ có từ Redis 8.6 (LRM chỉ cập nhật timestamp khi ghi, không khi đọc); LFU từ 4.0.
Nguồn: https://redis.io/docs/latest/develop/reference/eviction/
Các policy `volatile-*` hoạt động như `noeviction` khi không có key nào có TTL.
Nguồn: https://redis.io/docs/latest/develop/reference/eviction/
LRU, LFU, LRM, `volatile-ttl` là thuật toán xấp xỉ bằng cách lấy mẫu; `maxmemory-samples` mặc định 5 (tăng lên 10 để gần LRU thật, tốn CPU hơn).
Nguồn: https://redis.io/docs/latest/develop/reference/eviction/ và https://raw.githubusercontent.com/redis/redis/8.10/redis.conf
`maxmemory 0` nghĩa là không giới hạn (mặc định trên 64-bit); bộ nhớ buffer cho replica và AOF không được tính vào so sánh với `maxmemory`, nên cần chừa RAM trống.
Nguồn: https://redis.io/docs/latest/develop/reference/eviction/
Một lệnh thêm nhiều dữ liệu (ví dụ giao tập lớn lưu vào key mới) có thể vượt giới hạn tạm thời.
Nguồn: https://redis.io/docs/latest/develop/reference/eviction/
Kết luận về lab `CONFIG SET maxmemory` có thể test an toàn: `CONFIG SET maxmemory <bytes>` và `CONFIG SET maxmemory-policy <policy>` có hiệu lực từ lệnh kế tiếp ("immediately loaded... take effect starting with the next command"). Lab nên (1) chạy trên container riêng, không bật replication, (2) đặt `maxmemory` nhỏ (ví dụ 20-50 MB) cao hơn một chút so với `used_memory` hiện tại, (3) quan sát `INFO stats` `evicted_keys`, `keyspace_hits`, `keyspace_misses` và `INFO memory`, (4) so sánh `noeviction` (ghi bị từ chối) với `allkeys-lru`/`allkeys-lfu` (ghi vẫn được, key cũ bị đẩy ra). Không đoán chính xác số key bị evict vì thuật toán là xấp xỉ; assert theo hướng "evicted_keys > 0" thay vì số cụ thể.
Nguồn: https://redis.io/docs/latest/commands/config-set/ và https://redis.io/docs/latest/develop/reference/eviction/
Nội dung chính xác của thông báo lỗi OOM khi `noeviction`: chưa xác minh trên docs (docs chỉ nói "return an error"); lab nên assert theo prefix `OOM` sau khi tự chạy thử.

#### Transactions

`MULTI`/`EXEC` đảm bảo các lệnh được tuần tự hóa và thực thi liên tục, không có request của client khác chen giữa; nếu client mất kết nối trước `EXEC` thì không lệnh nào chạy.
Nguồn: https://redis.io/docs/latest/develop/using-commands/transactions/
Lỗi trước `EXEC` (sai cú pháp, hết bộ nhớ khi `maxmemory`) làm `EXEC` từ chối cả transaction (từ 2.6.5); lỗi sau `EXEC` (ví dụ WRONGTYPE) không dừng các lệnh còn lại, và Redis không hỗ trợ rollback.
Nguồn: https://redis.io/docs/latest/develop/using-commands/transactions/
`WATCH` cho optimistic locking (CAS): nếu key bị sửa trước `EXEC`, `EXEC` trả Null reply và transaction bị hủy; thay đổi do expiration hoặc eviction cũng được tính (từ 6.0.9 với expired key).
Nguồn: https://redis.io/docs/latest/develop/using-commands/transactions/
Từ Redis 8.4 có `SET ... IFEQ/IFNE/IFDEQ/IFDNE` và `DELEX` để compare-and-set và compare-and-delete trên string, đơn giản hơn `WATCH`; hữu ích cho lock release an toàn.
Nguồn: https://redis.io/docs/latest/develop/using-commands/transactions/

#### Lua scripting

Script chạy atomic, mọi hoạt động khác của server bị chặn trong suốt thời gian chạy; script dùng Lua 5.1 và là một phần của ứng dụng client (không được đặt tên, version hay persist).
Nguồn: https://redis.io/docs/latest/develop/programmability/eval-intro/
Mọi key mà script truy cập phải truyền qua `KEYS`, tham số khác qua `ARGV`; không truy cập key có tên sinh động, để chạy đúng trên standalone lẫn Cluster.
Nguồn: https://redis.io/docs/latest/develop/programmability/eval-intro/
Script cache luôn volatile: có thể mất khi restart hoặc failover; dùng `EVALSHA` và xử lý lỗi `NOSCRIPT` bằng `SCRIPT LOAD` rồi gọi lại (ioredis `defineCommand` và go-redis `redis.NewScript` đã tự làm việc này, chưa xác minh chi tiết từng client).
Nguồn: https://redis.io/docs/latest/develop/programmability/eval-intro/
Script có shebang `#!lua flags=...` kế thừa default flags và không được truy cập key khác hash slot; script không có `#!` thì được phép (chỉ trong chế độ cũ).
Nguồn: https://redis.io/docs/latest/develop/programmability/eval-intro/
Script chạy lâu vượt ngưỡng `busy-reply-threshold` (mặc định 5000 ms trong redis.conf) bị coi là slow script; chỉ `SCRIPT KILL` (nếu script chưa ghi) hoặc shutdown mới dừng được.
Nguồn: https://raw.githubusercontent.com/redis/redis/8.10/redis.conf và https://redis.io/docs/latest/develop/programmability/eval-intro/
Từ 7.0, script replication luôn theo effects (các lệnh ghi bọc trong MULTI/EXEC), không còn verbatim; từ 7.0 có thêm Redis Functions.
Nguồn: https://redis.io/docs/latest/develop/programmability/eval-intro/
Khi vượt `maxmemory`, lệnh ghi đầu tiên cần thêm bộ nhớ trong script sẽ làm script abort (trừ khi dùng `redis.pcall`).
Nguồn: https://redis.io/docs/latest/develop/programmability/eval-intro/

#### Pipelining

Pipelining gửi nhiều lệnh mà không chờ reply từng lệnh để giảm RTT; throughput tăng gần tuyến tính theo độ dài pipeline cho tới khoảng 10 lần so với không pipeline, vì giảm syscall `read()`/`write()`.
Nguồn: https://redis.io/docs/latest/develop/using-commands/pipelining/
Pipeline không phải transaction: lệnh chạy đúng thứ tự gửi nhưng lệnh của client khác có thể xen kẽ.
Nguồn: https://redis.io/docs/latest/develop/programmability/eval-intro/ (phần EVALSHA in the context of pipelining)
Server phải xếp hàng reply trong bộ nhớ; nên gửi theo batch hợp lý (ví dụ 10k lệnh) rồi đọc reply.
Nguồn: https://redis.io/docs/latest/develop/using-commands/pipelining/
Pipeline không giúp pattern read-compute-write vì cần reply của lệnh đọc; dùng script cho trường hợp đó.
Nguồn: https://redis.io/docs/latest/develop/using-commands/pipelining/

### Chương 03 - Messaging

#### Pub/Sub

Kết luận về delivery guarantee: Pub/Sub có ngữ nghĩa at-most-once, tức message được giao một lần nếu có subscriber, và nếu subscriber lỗi hoặc đứt mạng thì message mất vĩnh viễn; Pub/Sub không lưu message (không có persistence).
Nguồn: https://redis.io/docs/latest/develop/pubsub/
Tài liệu chỉ rõ Streams lưu message và hỗ trợ cả at-most-once lẫn at-least-once, nên là lựa chọn khi cần đảm bảo mạnh hơn.
Nguồn: https://redis.io/docs/latest/develop/pubsub/
Subscriber nhận message theo đúng thứ tự publish.
Nguồn: https://redis.io/docs/latest/develop/pubsub/
Pub/Sub không liên quan tới keyspace và database number: publish ở db 10 thì subscriber ở db 1 vẫn nhận được; nên prefix channel theo môi trường.
Nguồn: https://redis.io/docs/latest/develop/pubsub/
Một connection ở trạng thái subscribed (RESP2) chỉ được dùng một tập lệnh hạn chế (`PING`, `SUBSCRIBE`, `PSUBSCRIBE`, `SSUBSCRIBE`, các lệnh unsubscribe, `QUIT`, `RESET`); với RESP3 có thể chạy lệnh bất kỳ. Hệ quả cho ioredis 6.0.0 dùng RESP3 mặc định: lab vẫn nên dùng connection riêng cho subscriber (thực hành chuẩn), nhưng không còn bị buộc bởi giới hạn RESP2 (chưa xác minh cách ioredis 6 xử lý thực tế).
Nguồn: https://redis.io/docs/latest/develop/pubsub/
Một message có thể đến nhiều lần nếu client vừa `SUBSCRIBE foo` vừa `PSUBSCRIBE f*` (một `message` và một `pmessage`).
Nguồn: https://redis.io/docs/latest/develop/pubsub/
Sharded Pub/Sub (Redis 7.0+: `SSUBSCRIBE`, `SPUBLISH`) gán shard channel vào slot bằng cùng thuật toán với key và chỉ lan truyền trong shard; Pub/Sub thường trong Cluster gửi message tới mọi node qua cluster bus.
Nguồn: https://redis.io/docs/latest/develop/pubsub/

#### List queue và reliable queue

Kết luận BLMOVE vs BRPOPLPUSH: `BRPOPLPUSH` (since 2.2.0) đã deprecated từ Redis 6.2.0 ("Deprecated as of Redis v6.2.0" trên trang lệnh); `BLMOVE` (since 6.2.0) thay thế, và `BLMOVE source destination RIGHT LEFT timeout` tương đương `BRPOPLPUSH`. Lab dùng `BLMOVE`, `BRPOPLPUSH` chỉ nhắc như lịch sử.
Nguồn: https://redis.io/docs/latest/commands/brpoplpush/ và https://redis.io/docs/latest/commands/blmove/
`BLMOVE` syntax: `BLMOVE source destination <LEFT|RIGHT> <LEFT|RIGHT> timeout`, timeout là số giây kiểu double, `0` là block vô hạn; hết timeout trả nil/null.
Nguồn: https://redis.io/docs/latest/commands/blmove/
Queue đơn giản bằng `LPUSH` + `BRPOP` không reliable: message mất nếu consumer crash ngay sau khi pop; pattern reliable queue là `LMOVE`/`BLMOVE` sang list `processing`, xử lý xong thì `LREM` khỏi `processing`, và một client khác theo dõi `processing` để đẩy lại các item quá hạn.
Nguồn: https://redis.io/docs/latest/commands/lmove/
Với list: nếu `source` rỗng thì `LMOVE` trả nil; nếu `source` và `destination` giống nhau thì là phép xoay vòng (circular list).
Nguồn: https://redis.io/docs/latest/commands/lmove/
Trong cluster, `BLMOVE` là multi-key command nên `source` và `destination` phải cùng hash slot (xem phần CROSSSLOT ở chương 04).
Nguồn: https://redis.io/docs/latest/develop/using-commands/multi-key-operations/ (bảng ghi `BLMOVE`, `LMOVE`, `BLPOP`, `BRPOPLPUSH` là single-slot khi cluster bật; xem thêm chương 04).
Redis 8.10 thêm `LMOVEM`/`BLMOVEM` để chuyển nhiều phần tử; mới, không dùng làm nền tảng của lab.
Nguồn: https://redis.io/docs/latest/develop/whats-new/8-10/

#### Streams và consumer group

ID của entry dạng `<millisecondsTime>-<sequenceNumber>`, `XADD key * ...` để server tự sinh, ID bắt buộc tăng dần (ID nhỏ hơn hoặc bằng ID cuối bị từ chối với lỗi "equal or smaller than the target stream top item").
Nguồn: https://redis.io/docs/latest/develop/data-types/streams/
Consumer group: mỗi message chỉ giao cho một consumer trong group; tạo bằng `XGROUP CREATE key group $|0 [MKSTREAM]`; consumer tự được tạo lần đầu xuất hiện trong `XREADGROUP`.
Nguồn: https://redis.io/docs/latest/develop/data-types/streams/ và https://redis.io/docs/latest/commands/xreadgroup/
`XREADGROUP ... STREAMS key >` đọc message chưa từng giao cho consumer nào trong group; ID khác (ví dụ `0`) đọc lại lịch sử pending của chính consumer đó và khi đó `BLOCK`, `NOACK`, `CLAIM` bị bỏ qua. Khi đọc lại pending với ID `0` mà trả rỗng thì consumer biết đã xử lý hết và chuyển sang `>`.
Nguồn: https://redis.io/docs/latest/commands/xreadgroup/
PEL (Pending Entries List) là danh sách ID đã giao nhưng chưa `XACK`; `XACK` gỡ ID khỏi PEL; `XPENDING` xem PEL kèm idle time và delivery count.
Nguồn: https://redis.io/docs/latest/commands/xreadgroup/ và https://redis.io/docs/latest/develop/data-types/streams/
Khi consumer đọc lại một message đã giao cho nó, thời điểm giao cuối được cập nhật và delivery count tăng 1.
Nguồn: https://redis.io/docs/latest/commands/xreadgroup/
`NOACK` bỏ qua PEL (tương đương ack ngay khi đọc), chấp nhận mất message, không dùng cho queue cần reliable.
Nguồn: https://redis.io/docs/latest/commands/xreadgroup/
Streams và trạng thái consumer group (last-delivered ID, PEL, consumer) đều được persist và replicate; mặc định là at-least-once.
Nguồn: https://redis.io/docs/latest/develop/data-types/streams/
Lưu ý: replication của Redis là bất đồng bộ nên failover có thể mất các ghi gần nhất, kể cả stream (xem chương 04, nguồn replication).
`XREADGROUP ... CLAIM min-idle-time` (Redis 8.4+) cho phép claim pending entry idle đủ lâu ngay trong lệnh đọc và trả thêm idle time cùng delivery count cho mỗi entry claimed; chỉ có hiệu lực khi ID là `>`.
Nguồn: https://redis.io/docs/latest/commands/xreadgroup/
`XREADGROUP` và `XREAD` có thêm `MAXCOUNT`/`MAXSIZE` từ Redis 8.10.0 (giới hạn tổng số entry và tổng byte của reply trên nhiều stream).
Nguồn: https://redis.io/docs/latest/commands/xreadgroup/

#### XAUTOCLAIM - ngữ nghĩa chính xác (kết luận bắt buộc)

Cú pháp: `XAUTOCLAIM key group consumer min-idle-time start [COUNT count] [JUSTID]`, có từ 6.2.0, độ phức tạp O(1) nếu COUNT nhỏ.
Nguồn: https://redis.io/docs/latest/commands/xautoclaim/
`min-idle-time` tính bằng mili giây: chỉ claim các pending entry idle đủ lâu và có ID >= `start`; claim xong thì idle time được reset, nên tại một thời điểm chỉ một consumer claim được một entry. Lưu ý trang lệnh có hai cách diễn đạt hơi lệch nhau ("pending for more than min-idle-time" và "filters out entries having an idle time less than or equal to min-idle-time"); lab nên tránh test đúng biên bằng nhau và dùng khoảng chênh rõ rệt.
Nguồn: https://redis.io/docs/latest/commands/xautoclaim/
`COUNT` mặc định 100 là giới hạn số entry cố gắng claim; số entry tối đa được quét trong PEL là `count * 10` (hard-coded), nên có thể claim được ít hơn `count`.
Nguồn: https://redis.io/docs/latest/commands/xautoclaim/
Reply là mảng 3 phần tử: (1) stream ID dùng làm `start` cho lần gọi sau (cursor kiểu SCAN), `0-0` nghĩa là đã quét hết PEL; (2) mảng entry đã claim, cùng format `XRANGE`; (3) mảng ID đã không còn trong stream và đã bị xóa khỏi PEL.
Nguồn: https://redis.io/docs/latest/commands/xautoclaim/
Xử lý entry đã bị xóa: từ Redis 7.0, nếu thấy entry trong PEL mà đã bị trim hoặc `XDEL` khỏi stream thì không claim, xóa khỏi PEL và trả ID đó ở phần tử thứ ba của reply. Client (go-redis, ioredis) cần đọc phần tử thứ ba, và với client cũ chỉ đọc 2 phần tử thì phải kiểm tra phiên bản (chưa xác minh từng client).
Nguồn: https://redis.io/docs/latest/commands/xautoclaim/
Claim bằng `XAUTOCLAIM` làm tăng delivery count của entry, trừ khi dùng `JUSTID` (khi đó chỉ trả ID và không tăng counter); delivery count cao là dấu hiệu poison message cần theo dõi để chuyển vào dead-letter.
Nguồn: https://redis.io/docs/latest/commands/xautoclaim/
Nên tiếp tục gọi `XAUTOCLAIM` với `0-0` ngay cả khi vừa trả `0-0`, vì thời gian trôi qua nên các entry cũ có thể đủ điều kiện claim.
Nguồn: https://redis.io/docs/latest/commands/xautoclaim/
`XAUTOCLAIM` tương đương `XPENDING` rồi `XCLAIM`, nhưng đơn giản hơn.
Nguồn: https://redis.io/docs/latest/commands/xautoclaim/
Redis 8.8 thêm `XNACK key group SILENT|FAIL|FATAL IDS n id...`: trả message về PEL ở trạng thái không có owner với delivery time 0, nên có thể claim ngay bất kể `min-idle-time`; chế độ `FATAL` đặt delivery counter về LLONG_MAX để đánh dấu poison. `XNACK` chưa được Redis Software và Redis Cloud hỗ trợ (theo trang lệnh), nên chỉ nhắc như mở rộng.
Nguồn: https://redis.io/docs/latest/commands/xnack/

#### Trimming và xóa

`XTRIM key MAXLEN|MINID [=|~] threshold [LIMIT count] [KEEPREF|DELREF|ACKED]`; `MINID` và `LIMIT` có từ 6.2.0; `KEEPREF|DELREF|ACKED` từ 8.2.
Nguồn: https://redis.io/docs/latest/commands/xtrim/
`=` là trim chính xác (mặc định); `~` là trim xấp xỉ, hiệu quả hơn, có thể giữ thêm vài chục entry so với threshold; `LIMIT` chỉ cho `~`, mặc định `100 * số entry trong macro node`, `0` tắt giới hạn.
Nguồn: https://redis.io/docs/latest/commands/xtrim/ (phần "LIMIT chỉ cho ~" suy ra từ cách tài liệu mô tả, chưa có câu khẳng định trực tiếp trong trích đoạn đã đọc: chưa xác minh)
Mặc định (`KEEPREF`) trim xóa entry kể cả khi còn nằm trong PEL của consumer group và giữ nguyên tham chiếu trong PEL; `DELREF` xóa luôn tham chiếu; `ACKED` chỉ xóa entry đã được mọi group đọc và ack.
Nguồn: https://redis.io/docs/latest/commands/xtrim/
Hệ quả: entry bị trim/`XDEL` mà còn trong PEL thì `XREADGROUP ... 0` trả `(nil)` thay cho payload của entry đó; consumer phải ack bỏ ID này, và `XAUTOCLAIM` sẽ dọn nó khỏi PEL (từ 7.0).
Nguồn: https://redis.io/docs/latest/commands/xreadgroup/ và https://redis.io/docs/latest/commands/xautoclaim/
`XDEL` xóa entry theo ID; Redis 8.2 thêm `XDELEX` và `XACKDEL` để điều khiển việc xóa với nhiều consumer group.
Nguồn: https://redis.io/docs/latest/develop/data-types/streams/ và https://redis.io/docs/latest/commands/xreadgroup/
Quan sát: `XINFO STREAM`, `XINFO GROUPS`, `XINFO CONSUMERS`.
Nguồn: https://redis.io/docs/latest/develop/data-types/streams/

### Chương 04 - Advanced

#### Replication

Replication mặc định là bất đồng bộ: master gửi stream lệnh cho replica, replica ack định kỳ lượng dữ liệu đã xử lý; master không chờ replica cho từng lệnh.
Nguồn: https://redis.io/docs/latest/operate/oss_and_stack/management/replication/
Mỗi master có replication ID và offset; replica kết nối lại bằng `PSYNC` gửi ID và offset cũ; nếu backlog không đủ hoặc ID lạ thì full resync (master `BGSAVE` ra RDB, buffer lệnh ghi mới, gửi RDB, rồi gửi phần buffer).
Nguồn: https://redis.io/docs/latest/operate/oss_and_stack/management/replication/
Replica được promote sinh replication ID mới nhưng nhớ ID cũ nên các replica khác thường vẫn partial resync được (từ 4.0).
Nguồn: https://redis.io/docs/latest/operate/oss_and_stack/management/replication/
`WAIT` chỉ đảm bảo số bản sao đã ack, không biến Redis thành hệ CP: write đã ack vẫn có thể mất khi failover.
Nguồn: https://redis.io/docs/latest/operate/oss_and_stack/management/replication/
`min-replicas-to-write` và `min-replicas-max-lag` làm master từ chối ghi nếu không đủ replica còn lag trong ngưỡng; chỉ là best effort để giới hạn cửa sổ mất dữ liệu.
Nguồn: https://redis.io/docs/latest/operate/oss_and_stack/management/replication/
Replica mặc định read-only (`replica-read-only`) và mặc định bỏ qua `maxmemory` (`replica-ignore-maxmemory yes`): việc evict do master quyết định và gửi `DEL` xuống replica.
Nguồn: https://redis.io/docs/latest/operate/oss_and_stack/management/replication/
Replica không tự expire key mà chờ `DEL` từ master, nhưng với lệnh đọc thì replica dùng logical clock để không trả key đã hết hạn.
Nguồn: https://redis.io/docs/latest/operate/oss_and_stack/management/replication/
Nguy hiểm khi master tắt persistence mà tự restart: master khởi động rỗng và replica sync theo sẽ bị xóa dữ liệu; kể cả với Sentinel nếu master restart nhanh hơn thời gian Sentinel phát hiện.
Nguồn: https://redis.io/docs/latest/operate/oss_and_stack/management/replication/
Docker/NAT: `ROLE` và `INFO replication` hiển thị replica theo IP kết nối và port trong redis.conf, có thể sai; dùng `replica-announce-ip` và `replica-announce-port` (từ 3.2.2).
Nguồn: https://redis.io/docs/latest/operate/oss_and_stack/management/replication/

#### Sentinel

Cần ít nhất 3 Sentinel đặt trên các máy hỏng độc lập; Sentinel + Redis không đảm bảo giữ write đã ack khi failover vì replication bất đồng bộ; client phải hỗ trợ Sentinel.
Nguồn: https://redis.io/docs/latest/operate/oss_and_stack/management/sentinel/
`quorum` chỉ dùng để phát hiện failure (đánh dấu ODOWN); để thực sự failover, một Sentinel phải được bầu làm leader bằng đa số (majority) tất cả Sentinel, nên không failover ở phía thiểu số của network partition.
Nguồn: https://redis.io/docs/latest/operate/oss_and_stack/management/sentinel/
Kết luận về tham số thời gian: `down-after-milliseconds` là thời gian một instance không trả lời `PING` hợp lệ liên tục để Sentinel coi là SDOWN, mặc định 30000 ms (30 giây) trong sentinel.conf; `failover-timeout` mặc định 180000 ms (3 phút) và được dùng cho nhiều việc: thời gian chờ trước khi một Sentinel thử lại failover với cùng master (gấp 2 lần failover-timeout), thời gian hủy một failover đang chạy mà chưa tạo thay đổi cấu hình, và thời gian tối đa chờ các replica được cấu hình lại; `parallel-syncs` mặc định 1 (số replica cấu hình lại đồng thời sau failover).
Nguồn: https://raw.githubusercontent.com/redis/redis/8.10/sentinel.conf và https://redis.io/docs/latest/operate/oss_and_stack/management/sentinel/
Cấu hình mẫu trong tài liệu dùng `down-after-milliseconds mymaster 5000`, `failover-timeout mymaster 60000`, `parallel-syncs mymaster 1`, quorum 2 với 3 Sentinel; đây là cấu hình hợp lý cho lab để failover diễn ra trong vài giây thay vì 30 giây.
Nguồn: https://redis.io/docs/latest/operate/oss_and_stack/management/sentinel/
Tổng thời gian failover end-to-end bằng một con số cố định: chưa xác minh (phụ thuộc down-after-milliseconds cộng thời gian bầu leader, promote và reconfigure); lab nên đo thực tế và không hard-code.
Điều client phải làm theo spec: duyệt danh sách Sentinel với timeout ngắn (vài trăm ms), hỏi `SENTINEL get-master-addr-by-name <name>`, kết nối rồi gọi `ROLE` để xác nhận đúng master; mỗi lần reconnect phải resolve lại qua Sentinel.
Nguồn: https://redis.io/docs/latest/develop/reference/sentinel-clients/
Khi Sentinel đổi cấu hình một instance (promote, demote), nó gửi `CLIENT KILL type normal` để ngắt mọi client và ép resolve lại địa chỉ master; connection pool phải đóng mọi connection khi địa chỉ master đổi.
Nguồn: https://redis.io/docs/latest/develop/reference/sentinel-clients/
Pub/Sub của Sentinel (kênh `+switch-master`, `+sdown`...) giúp client phản ứng nhanh nhưng không thay thế quy trình resolve, vì không bảo đảm nhận đủ message.
Nguồn: https://redis.io/docs/latest/develop/reference/sentinel-clients/
Giảm mất dữ liệu khi partition: đặt `min-replicas-to-write 1` và `min-replicas-max-lag 10` để master cũ ở phía thiểu số ngừng nhận ghi sau 10 giây.
Nguồn: https://redis.io/docs/latest/operate/oss_and_stack/management/sentinel/
Sentinel với Docker: port remapping phá auto-discovery của Sentinel khác và danh sách replica; cách sửa là map port 1:1, dùng `--net=host`, hoặc `sentinel announce-ip`/`announce-port`; nên dùng hostname nhất quán với `resolve-hostnames` và `announce-hostnames`.
Nguồn: https://redis.io/docs/latest/operate/oss_and_stack/management/sentinel/
Client library: ioredis dùng `sentinels` + `name`, có `sentinelRetryStrategy`, `retryStrategy` mặc định backoff mũ tối đa 5 giây cộng jitter, `maxRetriesPerRequest` mặc định 20; go-redis dùng `redis.NewFailoverClient` với `FailoverOptions{MasterName, SentinelAddrs}`.
Nguồn: https://github.com/redis/ioredis/blob/main/README.md và https://github.com/redis/go-redis/blob/v9.23.0/sentinel.go

#### Cluster, hash slot và hash tag

Keyspace chia thành 16384 slot; `HASH_SLOT = CRC16(key) mod 16384`; CRC16 là biến thể XMODEM (CRC-16/ACORN): poly 0x1021, init 0, không reflect input/output, xor output 0, kết quả của chuỗi "123456789" là 0x31C3.
Nguồn: https://redis.io/docs/latest/operate/oss_and_stack/reference/cluster-spec/
Quy tắc hash tag: nếu key có `{`, và có `}` bên phải nó, và có ít nhất một ký tự giữa `{` đầu tiên và `}` đầu tiên sau nó, thì chỉ phần giữa được hash. Ví dụ từ spec: `{user1000}.following` và `{user1000}.followers` cùng slot; `foo{}{bar}` hash toàn bộ key; `foo{{bar}}zap` hash chuỗi `{bar`; `foo{bar}{zap}` hash `bar`; key bắt đầu bằng `{}` luôn hash toàn bộ.
Nguồn: https://redis.io/docs/latest/operate/oss_and_stack/reference/cluster-spec/
Kết luận về CROSSSLOT: trong Redis Open Source ở chế độ cluster, lệnh multi-key (ví dụ `MSET`, `DEL`, `SUNION`, `LMOVE`, `BLMOVE`, `BLPOP`, `ZUNIONSTORE`, `XREAD`/`XREADGROUP` nhiều stream), lệnh trong `MULTI/EXEC` và script `EVAL`/`EVALSHA` đều phải dùng key cùng một hash slot, nếu không server trả lỗi `CROSSSLOT` ("Keys in request don't hash to the same slot"); dùng hash tag để gom key, ví dụ `{queue}:pending` và `{queue}:processing` cho reliable queue bằng `BLMOVE`.
Nguồn: https://redis.io/docs/latest/develop/using-commands/multi-key-operations/
Riêng `MGET` và `EXISTS` trên cluster Open Source cũng là single-slot theo bảng của trang multi-key operations.
Nguồn: https://redis.io/docs/latest/develop/using-commands/multi-key-operations/
`CLUSTER KEYSLOT <key>` trả slot của một key, dùng để lab kiểm chứng hash tag.
Nguồn: https://redis.io/docs/latest/develop/using-commands/multi-key-operations/
Đừng lạm dụng hash tag: quá nhiều key vào một slot sẽ làm giảm hiệu năng của database.
Nguồn: https://redis.io/docs/latest/develop/using-commands/keyspace/
`MOVED <slot> <host:port>` báo slot đã thuộc node khác (client nên cập nhật cả slot map, ví dụ bằng `CLUSTER SLOTS`); `ASK` xuất hiện trong lúc migrate slot; `TRYAGAIN` là lỗi tạm thời khi migrate; client phải khởi tạo slot map lúc đầu và khi nhận `MOVED`.
Nguồn: https://redis.io/docs/latest/operate/oss_and_stack/reference/cluster-spec/ và https://redis.io/docs/latest/develop/using-commands/multi-key-operations/
Cluster dùng replication bất đồng bộ nên có cửa sổ mất write ("last failover wins"); một master bị failover khi bị đa số master coi là không liên lạc được trong ít nhất `NODE_TIMEOUT`; cluster bus dùng port data + 10000 (hoặc `cluster-port`).
Nguồn: https://redis.io/docs/latest/operate/oss_and_stack/reference/cluster-spec/
Giá trị mặc định trong redis.conf 8.10 (dòng comment mặc định): `cluster-node-timeout 15000`, `cluster-require-full-coverage yes`, `cluster-allow-reads-when-down no`.
Nguồn: https://raw.githubusercontent.com/redis/redis/8.10/redis.conf
Từ Redis 8.10, `redis-cli --cluster reshard` và `rebalance` dùng server-side atomic slot migration (ASM); chi tiết giao thức ASM: chưa xác minh.
Nguồn: https://redis.io/docs/latest/develop/whats-new/8-10/
Cluster Docker/NAT: cấu hình `cluster-announce-ip`, `cluster-announce-port`, `cluster-announce-bus-port` (và `cluster-announce-hostname`) trong redis.conf; hướng dẫn từng bước cho docker-compose: chưa xác minh trên tài liệu chính thức, lab nên tự chạy thử.
Nguồn: https://raw.githubusercontent.com/redis/redis/8.10/redis.conf

#### Hành vi client khi failover và natMap/Dialer cho Docker

ioredis Cluster: `maxRedirections` mặc định 16; `retryDelayOnFailover` mặc định 100 ms và tài liệu yêu cầu `retryDelayOnFailover * maxRedirections > cluster-node-timeout` để không lệnh nào lỗi khi failover; `retryDelayOnClusterDown`, `retryDelayOnTryAgain` mặc định 100 ms; `retryDelayOnMoved` mặc định 0; `slotsRefreshTimeout` mặc định 1000 ms; `scaleReads` mặc định `master`.
Nguồn: https://github.com/redis/ioredis/blob/main/README.md
ioredis `natMap` ánh xạ địa chỉ nội bộ mà node tự khai sang địa chỉ truy cập được từ ngoài, dạng object `{"10.0.1.230:30001": {host, port}}` hoặc function `(key) => {host, port} | null`; tài liệu nói rõ hữu ích khi cluster chạy trong Docker. Đây là cách xử lý lab cluster Docker khi chạy client trên host.
Nguồn: https://github.com/redis/ioredis/blob/main/README.md
ioredis pipeline trong cluster: mọi key của pipeline phải thuộc slot cùng một node vì ioredis gửi cả pipeline đến một node; không dùng được `multi` không pipeline (`cluster.multi({ pipeline: false })`); `ssubscribe` yêu cầu `shardedSubscribers: true` và mọi channel trong một lệnh `ssubscribe` phải cùng slot.
Nguồn: https://github.com/redis/ioredis/blob/main/README.md
ioredis reconnect: `retryStrategy` mặc định backoff mũ tối đa 5 giây cộng jitter tới 199 ms; sau khi reconnect tự subscribe lại (`autoResubscribe`) và gửi lại lệnh blocking chưa hoàn tất (`autoResendUnfulfilledCommands`); mỗi 20 lần retry thì lệnh pending bị flush lỗi (`maxRetriesPerRequest`, đặt `null` để chờ vô hạn); `blockingTimeout` (mặc định tắt) chống kết nối zombie cho lệnh blocking như `blpop`, `xreadgroup`.
Nguồn: https://github.com/redis/ioredis/blob/main/README.md
go-redis `ClusterOptions`: `MaxRedirects` mặc định 3; `ReadOnly`, `RouteByLatency`, `RouteRandomly` để đọc từ replica; go-redis không có `natMap`; thay vào đó có `Dialer func(ctx, network, addr)` (có ưu tiên hơn `Addr`) và `ClusterSlots func(ctx)` để tự cung cấp topology. Dùng `Dialer` để remap địa chỉ nội bộ sang địa chỉ host đã publish là suy ra từ chữ ký và comment của option, tài liệu chính thức không có ví dụ NAT: chưa xác minh bằng thực nghiệm.
Nguồn: https://github.com/redis/go-redis/blob/v9.23.0/osscluster.go và https://github.com/redis/go-redis/blob/v9.23.0/options.go
go-redis `Options`: `MaxRetries` mặc định 3, `MinRetryBackoff` 10 ms, `MaxRetryBackoff` 1 giây, `DialTimeout` 5 giây, `ReadTimeout` 5 giây, `PoolSize` mặc định `10 * GOMAXPROCS`, `Protocol` mặc định 3 (RESP3). Hành vi chính xác của go-redis với lệnh blocking có timeout dài hơn `ReadTimeout`: chưa xác minh, lab phải thử.
Nguồn: https://github.com/redis/go-redis/blob/v9.23.0/options.go

#### HA tổng hợp, hot key và big key

Chọn mô hình: replication cơ bản không tự failover (phần HA do Sentinel hoặc Cluster thêm vào); Sentinel thêm giám sát và failover cho topology không shard; Cluster thêm sharding cùng failover cho từng shard.
Nguồn: https://redis.io/docs/latest/operate/oss_and_stack/management/replication/ và https://redis.io/docs/latest/operate/oss_and_stack/management/sentinel/ và https://redis.io/docs/latest/operate/oss_and_stack/reference/cluster-spec/
Phát hiện big key bằng `redis-cli --bigkeys` (theo số phần tử), `--memkeys` (theo bộ nhớ), `--keystats` (kết hợp); các chế độ này dùng `SCAN` nên chạy được trên server bận, dùng `-i 0.01` để giảm tải.
Nguồn: https://redis.io/docs/latest/develop/tools/cli/
Phát hiện hot key bằng `redis-cli --hotkeys`, chỉ hoạt động khi `maxmemory-policy` là LFU.
Nguồn: https://redis.io/docs/latest/develop/tools/cli/
Xóa big key bằng `UNLINK` (thu hồi bộ nhớ ở thread khác, không chặn) thay vì `DEL` (chặn); `UNLINK` có từ 4.0.0.
Nguồn: https://redis.io/docs/latest/commands/unlink/
`KEYS`, `SMEMBERS` trên collection lớn có thể chặn server nhiều giây; dùng `SCAN`/`SSCAN`.
Nguồn: https://redis.io/docs/latest/develop/using-commands/keyspace/
Trong Cluster, mỗi slot thuộc đúng một master nên hot key hoặc hash tag dùng quá nhiều dồn tải vào một master; thêm node không giải quyết được hot key đơn lẻ (suy luận từ mô hình slot, không có câu trực tiếp trong docs: chưa xác minh).
Nguồn: https://redis.io/docs/latest/operate/oss_and_stack/reference/cluster-spec/

## Lỗi thường gặp ở production

Mỗi mục gồm triệu chứng, nguyên nhân và cách khắc phục; nguồn ghi ngay sau.

### Messaging

Dùng Pub/Sub làm job queue. Triệu chứng: job biến mất khi worker restart hoặc đứt mạng. Nguyên nhân: Pub/Sub là at-most-once, không lưu message. Khắc phục: dùng List với `BLMOVE` hoặc Streams với consumer group.
Nguồn: https://redis.io/docs/latest/develop/pubsub/
Queue bằng `LPUSH` + `BRPOP`. Triệu chứng: message mất khi consumer crash giữa lúc pop và xử lý. Khắc phục: `BLMOVE queue processing RIGHT LEFT 0`, xử lý xong thì `LREM processing 1 <msg>`, và cần một reaper đẩy lại item treo quá lâu trong `processing`. `BRPOPLPUSH` đã deprecated từ 6.2.0.
Nguồn: https://redis.io/docs/latest/commands/lmove/ và https://redis.io/docs/latest/commands/brpoplpush/
Quên `XACK`. Triệu chứng: PEL phình to, `XPENDING` cho thấy hàng nghìn entry, delivery count tăng. Khắc phục: luôn `XACK` sau khi xử lý thành công, giám sát `XPENDING` và `XINFO GROUPS`, đưa message có delivery count cao vào dead-letter (xóa bằng `XDEL`/`XACKDEL` sau khi sao chép).
Nguồn: https://redis.io/docs/latest/commands/xreadgroup/ và https://redis.io/docs/latest/develop/data-types/streams/
Chỉ đọc bằng `>` mà không đọc lại pending khi khởi động. Triệu chứng: message đã giao cho consumer cũ (đã crash) bị kẹt mãi trong PEL của consumer đó. Khắc phục: khi khởi động đọc với ID `0` cho tới khi rỗng, và chạy một vòng `XAUTOCLAIM` định kỳ để tiếp quản message của consumer chết.
Nguồn: https://redis.io/docs/latest/commands/xreadgroup/ và https://redis.io/docs/latest/commands/xautoclaim/
Vòng lặp `XAUTOCLAIM` dừng hẳn khi gặp `0-0`. Triệu chứng: message đến hạn claim sau đó không bao giờ được nhặt. Khắc phục: gọi lại từ `0-0` theo chu kỳ, vì thời gian trôi qua làm các entry cũ đủ điều kiện.
Nguồn: https://redis.io/docs/latest/commands/xautoclaim/
Bỏ qua phần tử thứ ba của reply `XAUTOCLAIM` (hoặc dùng client cũ chỉ đọc 2 phần tử). Triệu chứng: ID đã bị trim/xóa nằm trong PEL vô hạn, hoặc consumer nhận entry với payload `(nil)`. Khắc phục: đọc phần tử thứ ba, ack/ghi log các ID đó; kiểm tra entry null khi đọc lại pending bằng `XREADGROUP ... 0`.
Nguồn: https://redis.io/docs/latest/commands/xautoclaim/ và https://redis.io/docs/latest/commands/xreadgroup/
`XTRIM`/`MAXLEN` quá tay trong khi consumer chậm. Triệu chứng: mất message chưa xử lý (mặc định `KEEPREF` vẫn xóa entry dù còn trong PEL). Khắc phục: trim theo `MINID` đủ xa, hoặc dùng `ACKED` (Redis 8.2+) để chỉ xóa entry mọi group đã ack; ưu tiên `~` để trim rẻ hơn.
Nguồn: https://redis.io/docs/latest/commands/xtrim/
Dùng `NOACK` vì tiện. Triệu chứng: crash là mất message mà không dấu vết. Khắc phục: chỉ dùng khi chấp nhận mất message.
Nguồn: https://redis.io/docs/latest/commands/xreadgroup/
Test claim đúng biên `min-idle-time`. Triệu chứng: test flaky. Nguyên nhân: tài liệu `XAUTOCLAIM` diễn đạt không nhất quán về biên (more than / less than or equal). Khắc phục: dùng khoảng chênh lớn so với `min-idle-time` trong test.
Nguồn: https://redis.io/docs/latest/commands/xautoclaim/
`XAUTOCLAIM` với `COUNT` lớn rồi tưởng đã claim hết. Triệu chứng: số entry claim được ít hơn `COUNT`. Nguyên nhân: server chỉ quét tối đa `COUNT * 10` entry PEL mỗi lần. Khắc phục: lặp theo cursor trả về.
Nguồn: https://redis.io/docs/latest/commands/xautoclaim/

### Core

Dùng Redis vừa làm cache vừa làm queue trên cùng instance với `allkeys-lru`. Triệu chứng: job hoặc stream bị evict. Khắc phục: tách instance (tài liệu cũng khuyên tách khi dùng `volatile-*` cho dữ liệu bền), hoặc dùng `noeviction` cho instance chứa queue và chấp nhận lỗi ghi khi đầy.
Nguồn: https://redis.io/docs/latest/develop/reference/eviction/
`noeviction` khi đầy bộ nhớ. Triệu chứng: lệnh ghi (kể cả `XADD`, `LPUSH`) trả lỗi OOM trong khi lệnh đọc vẫn chạy; transaction có lệnh cần bộ nhớ bị từ chối ở `EXEC`. Khắc phục: giám sát `used_memory` trên `maxmemory`, đặt cảnh báo, chọn policy phù hợp.
Nguồn: https://redis.io/docs/latest/develop/reference/eviction/ và https://redis.io/docs/latest/develop/using-commands/transactions/
Dùng policy `volatile-*` nhưng key không có TTL. Triệu chứng: không có key nào bị evict, ghi bị lỗi như `noeviction`. Khắc phục: đặt TTL hoặc đổi sang `allkeys-*`.
Nguồn: https://redis.io/docs/latest/develop/reference/eviction/
Đặt `maxmemory` sát tổng RAM khi có replication hoặc AOF. Triệu chứng: process bị OOM killer vì buffer replica/AOF không được tính vào `maxmemory`. Khắc phục: chừa RAM trống cho buffer (tham khảo `mem_not_counted_for_evict` trong `INFO memory`).
Nguồn: https://redis.io/docs/latest/develop/reference/eviction/
Replica không evict mà dùng nhiều bộ nhớ hơn master. Khắc phục: giám sát replica, đảm bảo đủ RAM; chỉ đổi `replica-ignore-maxmemory` khi hiểu rõ hậu quả.
Nguồn: https://redis.io/docs/latest/operate/oss_and_stack/management/replication/
Hy vọng `MULTI/EXEC` rollback. Triệu chứng: một lệnh lỗi runtime (WRONGTYPE) nhưng các lệnh khác vẫn ghi. Khắc phục: validate trước, hoặc dùng Lua; không có rollback.
Nguồn: https://redis.io/docs/latest/develop/using-commands/transactions/
Dùng `WATCH` với key có TTL trên Redis cũ hơn 6.0.9. Triệu chứng: key hết hạn không làm transaction abort. Khắc phục: dùng Redis >= 6.0.9; với compare-and-set trên string, Redis 8.4+ có `SET ... IFEQ`.
Nguồn: https://redis.io/docs/latest/develop/using-commands/transactions/
Lua script chạy lâu hoặc vòng lặp lớn. Triệu chứng: toàn server bị chặn, client nhận lỗi BUSY. Khắc phục: giữ script ngắn, ngưỡng `busy-reply-threshold` mặc định 5000 ms, dùng `SCRIPT KILL` (chỉ khi script chưa ghi) hoặc shutdown.
Nguồn: https://redis.io/docs/latest/develop/programmability/eval-intro/ và https://raw.githubusercontent.com/redis/redis/8.10/redis.conf
`NOSCRIPT` sau restart hoặc failover. Khắc phục: dùng `EVALSHA` và fallback `SCRIPT LOAD`; trong pipeline dùng `EVAL` hoặc `SCRIPT LOAD` trước vì lỗi `NOSCRIPT` trong pipeline không xử lý được.
Nguồn: https://redis.io/docs/latest/develop/programmability/eval-intro/
Script truy cập key không khai báo trong `KEYS` (key sinh động). Triệu chứng: chạy được ở standalone, hỏng ở Cluster. Khắc phục: khai báo mọi key qua `KEYS`, cùng hash slot.
Nguồn: https://redis.io/docs/latest/develop/programmability/eval-intro/
Pipeline hàng triệu lệnh một lần. Triệu chứng: server tốn bộ nhớ xếp hàng reply. Khắc phục: chia batch khoảng 10k lệnh.
Nguồn: https://redis.io/docs/latest/develop/using-commands/pipelining/
`KEYS *` và `SMEMBERS`/`HGETALL` trên collection lớn trong production. Triệu chứng: latency tăng vọt, các client khác timeout. Khắc phục: `SCAN`/`SSCAN`/`HSCAN`, tìm big key bằng `redis-cli --bigkeys`/`--memkeys`, xóa bằng `UNLINK`.
Nguồn: https://redis.io/docs/latest/develop/using-commands/keyspace/ và https://redis.io/docs/latest/develop/tools/cli/ và https://redis.io/docs/latest/commands/unlink/
Chỉ dùng RDB hoặc AOF `everysec` rồi kỳ vọng không mất dữ liệu. Triệu chứng: sau crash mất vài phút (RDB) hoặc khoảng 1 giây (AOF `everysec`) dữ liệu. Khắc phục: chọn `appendfsync always` nếu cần, hoặc chấp nhận và ghi rõ trong thiết kế; tài liệu khuyên dùng cả RDB và AOF.
Nguồn: https://redis.io/docs/latest/operate/oss_and_stack/management/persistence/
`fork()` lâu với dataset lớn. Triệu chứng: Redis ngừng phục vụ vài mili giây tới khoảng một giây khi `BGSAVE` hoặc rewrite. Khắc phục: giảm tần suất snapshot, dùng AOF, đo bằng latency tools.
Nguồn: https://redis.io/docs/latest/operate/oss_and_stack/management/persistence/
Bật AOF trên instance đang chạy chỉ bằng sửa file config rồi restart. Triệu chứng: mất dữ liệu. Khắc phục: bật bằng `CONFIG SET appendonly yes`, đợi rewrite xong, rồi `CONFIG REWRITE`.
Nguồn: https://redis.io/docs/latest/operate/oss_and_stack/management/persistence/

### HA, Cluster và client

Master tắt persistence và tự restart. Triệu chứng: sau restart master rỗng, replica sync và cũng bị xóa sạch. Khắc phục: bật persistence trên master và replica, hoặc tắt auto restart.
Nguồn: https://redis.io/docs/latest/operate/oss_and_stack/management/replication/
Chỉ 2 Sentinel hoặc đặt cùng một máy. Triệu chứng: không failover được hoặc failover sai ở phía thiểu số. Khắc phục: tối thiểu 3 Sentinel trên 3 máy độc lập, hiểu `quorum` chỉ để phát hiện còn bầu leader cần đa số.
Nguồn: https://redis.io/docs/latest/operate/oss_and_stack/management/sentinel/
Failover quá chậm trong lab. Nguyên nhân: `down-after-milliseconds` mặc định 30000 ms. Khắc phục: đặt 5000 ms cho lab, và đo thời gian failover thực tế.
Nguồn: https://raw.githubusercontent.com/redis/redis/8.10/sentinel.conf và https://redis.io/docs/latest/operate/oss_and_stack/management/sentinel/
Mất write sau failover dù đã nhận OK. Nguyên nhân: replication bất đồng bộ. Khắc phục: `WAIT` giảm xác suất nhưng không loại bỏ; `min-replicas-to-write`/`min-replicas-max-lag` giới hạn cửa sổ mất dữ liệu ở master phía thiểu số.
Nguồn: https://redis.io/docs/latest/operate/oss_and_stack/management/replication/ và https://redis.io/docs/latest/operate/oss_and_stack/management/sentinel/
Sentinel trong Docker bridge network. Triệu chứng: Sentinel không thấy nhau, replica bị liệt kê sai địa chỉ nên không bao giờ failover. Khắc phục: map port 1:1, `--net=host`, hoặc `sentinel announce-ip`/`announce-port` và `replica-announce-ip`/`replica-announce-port`.
Nguồn: https://redis.io/docs/latest/operate/oss_and_stack/management/sentinel/ và https://redis.io/docs/latest/operate/oss_and_stack/management/replication/
Cluster trong Docker, client chạy ngoài network Docker. Triệu chứng: kết nối seed node được nhưng sau `CLUSTER SLOTS` client cố nối tới IP nội bộ, gây timeout hoặc `ECONNREFUSED`, hay vòng lặp `MOVED`. Khắc phục: ioredis dùng `natMap`; go-redis dùng `Dialer` để remap (chưa xác minh bằng thực nghiệm); phía server dùng `cluster-announce-ip`/`cluster-announce-port`/`cluster-announce-bus-port`.
Nguồn: https://github.com/redis/ioredis/blob/main/README.md và https://github.com/redis/go-redis/blob/v9.23.0/osscluster.go và https://raw.githubusercontent.com/redis/redis/8.10/redis.conf
`CROSSSLOT` khi dùng `MSET`, `SUNION`, `BLMOVE`, `MULTI/EXEC`, script, `XREADGROUP` nhiều stream. Khắc phục: hash tag trong tên key (ví dụ `{orders}:pending` và `{orders}:processing`), kiểm tra bằng `CLUSTER KEYSLOT`.
Nguồn: https://redis.io/docs/latest/develop/using-commands/multi-key-operations/ và https://redis.io/docs/latest/operate/oss_and_stack/reference/cluster-spec/
Hash tag cho mọi key (ví dụ `{app}:...`). Triệu chứng: một master quá tải, các node khác rảnh. Khắc phục: chỉ gom những key thực sự cần multi-key operation.
Nguồn: https://redis.io/docs/latest/develop/using-commands/keyspace/
ioredis Cluster: lệnh lỗi trong lúc failover vì tổng thời gian retry nhỏ hơn `cluster-node-timeout`. Khắc phục: đảm bảo `retryDelayOnFailover * maxRedirections > cluster-node-timeout` (mặc định 100 ms * 16 = 1600 ms, nhỏ hơn 15000 ms của redis.conf mặc định nên cần tăng nếu muốn lệnh sống sót qua failover).
Nguồn: https://github.com/redis/ioredis/blob/main/README.md và https://raw.githubusercontent.com/redis/redis/8.10/redis.conf
ioredis: lệnh pending bị flush lỗi sau 20 lần retry (`maxRetriesPerRequest`) trong khi đang failover, hoặc ngược lại đặt `null` làm lệnh chờ vô hạn. Khắc phục: chọn có chủ đích theo SLA.
Nguồn: https://github.com/redis/ioredis/blob/main/README.md
Lệnh blocking (`BLMOVE`, `XREADGROUP BLOCK`) treo vì kết nối zombie sau khi mất mạng im lặng (ví dụ Docker network disconnect). Khắc phục: ioredis bật `blockingTimeout`; với go-redis đặt `ReadTimeout` và `ContextTimeoutEnabled` phù hợp (hành vi chính xác: chưa xác minh).
Nguồn: https://github.com/redis/ioredis/blob/main/README.md và https://github.com/redis/go-redis/blob/v9.23.0/options.go
Pub/Sub và lệnh blocking dùng chung một connection với lệnh thường. Triệu chứng: lệnh thường bị kẹt sau lệnh blocking hoặc connection ở trạng thái subscribed bị từ chối lệnh (RESP2). Khắc phục: dùng connection riêng cho subscriber và cho blocking consumer.
Nguồn: https://redis.io/docs/latest/develop/pubsub/ và https://redis.io/docs/latest/commands/blmove/ (BLMOVE chặn connection tới khi có phần tử hoặc hết timeout)
Sau failover, subscriber im lặng. Khắc phục: ioredis tự `autoResubscribe` sau reconnect; Pub/Sub vẫn là at-most-once nên message trong khoảng gián đoạn mất.
Nguồn: https://github.com/redis/ioredis/blob/main/README.md và https://redis.io/docs/latest/develop/pubsub/
Nâng cấp ioredis 6.0.0 mà không để ý RESP3 mặc định và Node >= 20; go-redis v9.23.0 yêu cầu Go >= 1.26 và có Protocol mặc định 3. Triệu chứng: lỗi build hoặc khác biệt hình dạng reply. Khắc phục: pin phiên bản, đặt `protocol: 2` (ioredis) hoặc `Protocol: 2` (go-redis) nếu cần hành vi cũ, và chạy lại lab.
Nguồn: https://github.com/redis/ioredis/blob/main/CHANGELOG.md và https://github.com/redis/go-redis/releases/tag/v9.23.0 và https://github.com/redis/go-redis/blob/v9.23.0/options.go

## Nguồn

https://github.com/redis/go-redis/blob/v9.23.0/options.go 2026-10-06
https://github.com/redis/go-redis/blob/v9.23.0/osscluster.go 2026-10-06
https://github.com/redis/go-redis/blob/v9.23.0/sentinel.go 2026-10-06
https://github.com/redis/go-redis/releases/tag/v9.23.0 2026-10-06
https://github.com/redis/ioredis/blob/main/CHANGELOG.md 2026-10-06
https://github.com/redis/ioredis/blob/main/README.md 2026-10-06
https://github.com/redis/redis/releases 2026-10-06
https://proxy.golang.org/github.com/redis/go-redis/v9/@latest 2026-10-06
https://raw.githubusercontent.com/redis/redis/8.10/redis.conf 2026-10-06
https://raw.githubusercontent.com/redis/redis/8.10/sentinel.conf 2026-10-06
https://redis.io/docs/latest/commands/blmove/ 2026-10-06
https://redis.io/docs/latest/commands/brpoplpush/ 2026-10-06
https://redis.io/docs/latest/commands/config-set/ 2026-10-06
https://redis.io/docs/latest/commands/lmove/ 2026-10-06
https://redis.io/docs/latest/commands/unlink/ 2026-10-06
https://redis.io/docs/latest/commands/xautoclaim/ 2026-10-06
https://redis.io/docs/latest/commands/xnack/ 2026-10-06
https://redis.io/docs/latest/commands/xreadgroup/ 2026-10-06
https://redis.io/docs/latest/commands/xtrim/ 2026-10-06
https://redis.io/docs/latest/develop/data-types/ 2026-10-06
https://redis.io/docs/latest/develop/data-types/streams/ 2026-10-06
https://redis.io/docs/latest/develop/programmability/eval-intro/ 2026-10-06
https://redis.io/docs/latest/develop/pubsub/ 2026-10-06
https://redis.io/docs/latest/develop/reference/eviction/ 2026-10-06
https://redis.io/docs/latest/develop/reference/sentinel-clients/ 2026-10-06
https://redis.io/docs/latest/develop/tools/cli/ 2026-10-06
https://redis.io/docs/latest/develop/using-commands/keyspace/ 2026-10-06
https://redis.io/docs/latest/develop/using-commands/multi-key-operations/ 2026-10-06
https://redis.io/docs/latest/develop/using-commands/pipelining/ 2026-10-06
https://redis.io/docs/latest/develop/using-commands/transactions/ 2026-10-06
https://redis.io/docs/latest/develop/whats-new/8-10/ 2026-10-06
https://redis.io/docs/latest/develop/whats-new/8-8/ 2026-10-06
https://redis.io/docs/latest/operate/oss_and_stack/management/persistence/ 2026-10-06
https://redis.io/docs/latest/operate/oss_and_stack/management/replication/ 2026-10-06
https://redis.io/docs/latest/operate/oss_and_stack/management/sentinel/ 2026-10-06
https://redis.io/docs/latest/operate/oss_and_stack/reference/cluster-spec/ 2026-10-06
https://registry.npmjs.org/ioredis 2026-10-06
