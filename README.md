# MQ & Redis Handbook

Tài liệu lý thuyết và lab thực hành về Message Queue và Redis, đi từ cơ bản đến các use case production.
Mỗi lab có hai phiên bản: TypeScript (Node 22) và Go, đều có test chạy với broker thật qua Docker.
Nội dung viết bằng tiếng Việt, thuật ngữ kỹ thuật giữ tiếng Anh.

## Lộ trình học

Đọc theo thứ tự 01 đến 09.
Cả chín chủ đề đã có đủ lý thuyết, lab và use case.

| #   | Chủ đề                                                                                         | Trạng thái |
| --- | ---------------------------------------------------------------------------------------------- | ---------- |
| 01  | [Fundamentals](./01-fundamentals/) - vì sao cần MQ, delivery semantics, ordering, backpressure | đã xong    |
| 02  | [Redis core](./02-redis-core/) - data types, persistence, eviction, Lua, pipelining            | đã xong    |
| 03  | [Redis messaging](./03-redis-messaging/) - Pub/Sub, List, Streams, consumer group              | đã xong    |
| 04  | [Redis nâng cao](./04-redis-advanced/) - replication, Sentinel, Cluster, hot key, big key      | đã xong    |
| 05  | [RabbitMQ](./05-rabbitmq/) - exchange, ack, prefetch, DLX, quorum queue                        | đã xong    |
| 06  | [Kafka](./06-kafka/) - log, partition, consumer group, offset, transaction                     | đã xong    |
| 07  | [NATS JetStream](./07-nats-jetstream/) - core NATS, stream, durable consumer                   | đã xong    |
| 08  | [Patterns](./08-patterns/) - idempotency, retry, DLQ, outbox, saga                             | đã xong    |
| 09  | [Production](./09-production/) - job queue, event-driven, rate limiter, realtime               | đã xong    |

## Nội dung hiện có

Handbook có 9 chủ đề, mỗi chủ đề một `theory.md`, cùng 26 lab (chủ đề 01 đến 08) và 4 use case (chủ đề 09).
Mỗi lab và use case có `README.md`, phiên bản TypeScript và phiên bản Go, và test chạy với broker thật qua Docker.
Chủ đề 09 là phần tổng hợp: bảng chọn công nghệ Redis, RabbitMQ, Kafka, NATS và bốn hệ thống nhỏ ghép các chương trước.

| #   | Số lab hoặc use case | Cần gì để chạy                                                                                                                    |
| --- | -------------------- | --------------------------------------------------------------------------------------------------------------------------------- |
| 01  | 2 lab                | Không cần broker hay Docker                                                                                                       |
| 02  | 3 lab                | Redis                                                                                                                             |
| 03  | 3 lab                | Redis                                                                                                                             |
| 04  | 3 lab                | Lab 01 và 02 cần profile `sentinel` và `cluster`, lab 03 chỉ cần Redis                                                            |
| 05  | 3 lab                | RabbitMQ                                                                                                                          |
| 06  | 3 lab                | Kafka                                                                                                                             |
| 07  | 3 lab                | NATS                                                                                                                              |
| 08  | 6 lab                | Lab 01, 03, 04 cần Redis, lab 05 cần Postgres, RabbitMQ và Redis (Redis giữ marker của test chaos), lab 02 và 06 không cần broker |
| 09  | 4 use case           | Use case 1, 3, 4 cần Redis, use case 2 cần Postgres và Kafka (xem phần Postgres khi port 5432 bị chiếm)                           |

## Phiên bản

Phiên bản server lấy từ `infra/docker-compose.yml`, phiên bản runtime và thư viện lấy từ `package.json`, `go.mod` và workflow CI.
Cập nhật gần nhất: 2026-10-06.

| Thành phần | Phiên bản                              | Dùng ở                              |
| ---------- | -------------------------------------- | ----------------------------------- |
| Redis      | `redis:8.10.2`                         | Chủ đề 02, 03, 04, 08, 09           |
| RabbitMQ   | `rabbitmq:4.3.6-management`            | Chủ đề 05, 08 (lab 05)              |
| Kafka      | `apache/kafka:4.3.1` (KRaft, một node) | Chủ đề 06, 09 (use case 2)          |
| NATS       | `nats:2.15.0-alpine` (bật JetStream)   | Chủ đề 07                           |
| PostgreSQL | `postgres:18.6`                        | Chủ đề 08 (lab 05), 09 (use case 2) |

RabbitMQ series 4.3 có community support đến 2026-11-30, nên khi dùng ngoài lab hãy theo dõi series mới hơn.

| Runtime và công cụ    | Phiên bản                                                                                               |
| --------------------- | ------------------------------------------------------------------------------------------------------- |
| Node                  | 22 (`engines` là `>=22`, CI dùng 22)                                                                    |
| pnpm                  | 11 (CI dùng `11.13.1`)                                                                                  |
| Go                    | 1.26 (`go 1.26.0` trong `go.mod`)                                                                       |
| TypeScript và công cụ | vitest 5.0.3, TypeScript 7.0.2 (`tsc`), tsx 4.23.15, eslint 10.12.0, prettier 3.9.9, mermaid-cli 12.0.0 |
| Go lint               | golangci-lint v2.14.0 (Makefile)                                                                        |

| Client TypeScript (`package.json`) | Phiên bản | Client Go (`go.mod`)                  | Phiên bản |
| ---------------------------------- | --------- | ------------------------------------- | --------- |
| `ioredis`                          | 6.0.0     | `github.com/redis/go-redis/v9`        | v9.23.0   |
| `amqplib`                          | 2.2.0     | `github.com/rabbitmq/amqp091-go`      | v1.15.0   |
| `@confluentinc/kafka-javascript`   | 1.10.1    | `github.com/segmentio/kafka-go`       | v0.4.51   |
| `@nats-io/transport-node`          | 3.4.0     | `github.com/nats-io/nats.go`          | v1.54.0   |
| `@nats-io/jetstream`               | 3.4.0     | `github.com/hibiken/asynq`            | v0.26.0   |
| `bullmq`                           | 6.3.11    | `github.com/prometheus/client_golang` | v1.24.1   |
| `@prometheus-io/client`            | 0.16.1    | `github.com/jackc/pgx/v5`             | v5.11.0   |
| `pg`                               | 8.23.1    | `github.com/coder/websocket`          | v1.8.15   |
| `ws`                               | 8.22.0    | `golang.org/x/sync`                   | v0.23.0   |

## Yêu cầu

- Node 22 và pnpm 11 (CI dùng `11.13.1`, lockfile và `pnpm-workspace.yaml` được tạo bằng bản này).
- Go 1.26 trở lên (`go.mod` dùng directive `go 1.26.0`, vì `go-redis` v9.23.0 yêu cầu Go >= 1.26).
  Nếu Go cài sẵn cũ hơn, `GOTOOLCHAIN=auto` (mặc định) tự tải toolchain 1.26 lần chạy đầu.
- Docker với Compose v2 (image đều multi-arch: Apple Silicon arm64 và amd64).

`make lint` chạy `golangci-lint` v2.14.0 qua `go run`, và bản này tự tải toolchain Go mới hơn nếu cần (`GOTOOLCHAIN=auto`).
Nếu đã cài sẵn `golangci-lint` trong `PATH` thì Makefile dùng bản đó.
Sơ đồ Mermaid được kiểm tra bằng `@mermaid-js/mermaid-cli` (devDependency, tải Chrome khi `pnpm install`).
`pnpm-workspace.yaml` có mục `allowBuilds` (cú pháp của pnpm 11), vì pnpm mặc định không chạy script cài đặt của dependency.
Ba package sau cần script cài đặt của chúng chạy, nên được đặt `true`:

- `@confluentinc/kafka-javascript` chạy `node-pre-gyp install` để có binary native cho client Kafka của TypeScript.
- `esbuild` chạy script `postinstall` của nó, và `vite` (dùng bởi `vitest`) phụ thuộc vào `esbuild`.
- `puppeteer` chạy script `postinstall` của nó, tải Chrome cho `mmdc` kiểm tra Mermaid.

`msgpackr-extract` (bộ tăng tốc native tùy chọn mà `bullmq` kéo theo) được đặt `false` trong cùng mục: build script của nó KHÔNG được phép chạy và JavaScript thuần là phương án dự phòng.
Đừng xóa mục này khỏi file.

## Bắt đầu nhanh

```bash
pnpm install
make up                      # redis, rabbitmq, kafka, nats, postgres
make up PROFILE=sentinel     # thêm Redis Sentinel (master + 2 replica + 3 sentinel)
make up PROFILE=cluster      # thêm Redis Cluster (6 node, tự khởi tạo)
make up PROFILE="sentinel cluster" # cả hai topology cùng lúc (test chủ đề 04 cần cả hai)
make lab-ts LAB=<NN-ten/lab-NN-ten>
make lab-go LAB=<NN-ten/lab-NN-ten>
make lab-ts LAB=09-production/usecase-01-job-queue   # use case chạy bằng cùng lệnh với lab
make test                    # test script, TS và Go (chủ đề 04 cần PROFILE="sentinel cluster")
make test-fast               # mọi test trừ lab sentinel và cluster, chỉ cần make up thường
make test-chaos              # chỉ lab sentinel và cluster (có test chaos), cần PROFILE="sentinel cluster"
make lint
make docs-check
make mermaid-check           # mọi khối mermaid phải parse được (mmdc)
make test-scripts            # test của các script kiểm tra (check-docs, check-compose, ...)
make down
```

Các target của Makefile (`make help` in danh sách ngắn):

| Target                                         | Việc làm                                                                      |
| ---------------------------------------------- | ----------------------------------------------------------------------------- |
| `up`, `down`                                   | Bật và tắt broker bằng Docker Compose                                         |
| `lab-ts`, `lab-go`                             | Chạy demo của một lab hoặc use case (`LAB=<thư mục>`)                         |
| `test`, `test-ts`, `test-go`                   | Mọi test, gồm cả lab sentinel và cluster                                      |
| `test-fast`, `test-fast-ts`, `test-fast-go`    | Mọi test trừ lab sentinel và cluster, đây là bộ test của CI                   |
| `test-chaos`, `test-chaos-ts`, `test-chaos-go` | Chỉ lab sentinel và cluster, kể cả test dừng và khởi động lại container Redis |
| `test-scripts`                                 | Test của các script kiểm tra tài liệu, compose, no-sleep, tsconfig và mermaid |
| `lint`, `lint-ts`, `lint-go`                   | eslint, prettier, tsc, golangci-lint, cổng no-sleep, kiểm tra compose         |
| `docs-check`, `mermaid-check`                  | Kiểm tra cấu trúc tài liệu và việc mọi khối mermaid parse được                |

Mọi port của broker chỉ bind `127.0.0.1`.
Nếu port mặc định đã bị chiếm, đổi port phía host rồi trỏ biến môi trường của lab tới port mới.
Các biến đổi port của broker là `REDIS_PORT`, `AMQP_PORT`, `RABBITMQ_MANAGEMENT_PORT`, `KAFKA_PORT`, `NATS_PORT` và `POSTGRES_PORT`.
Port của profile `sentinel` (6380 đến 6382 cho Redis, 26379 đến 26381 cho Sentinel) và profile `cluster` (7001 đến 7006) được cố định trong compose, không đổi được bằng biến môi trường.
Ví dụ:

```bash
REDIS_PORT=6390 KAFKA_PORT=9094 POSTGRES_PORT=5433 make up
export REDIS_URL=redis://127.0.0.1:6390 KAFKA_BROKERS=127.0.0.1:9094 DATABASE_URL=postgres://handbook:handbook@127.0.0.1:5433/handbook
```

### Postgres khi port 5432 bị chiếm

Hai nơi dùng Postgres: [lab 05 của chủ đề 08](./08-patterns/lab-05-outbox/) và [use case 2 của chủ đề 09](./09-production/usecase-02-order-events/).
Cả hai đọc `DATABASE_URL`, mặc định `postgres://handbook:handbook@127.0.0.1:5432/handbook`.
Nếu máy bạn đã có một Postgres khác giữ port 5432, hãy publish Postgres của handbook ra port khác rồi trỏ `DATABASE_URL` tới đó:

```bash
POSTGRES_PORT=5439 make up
export DATABASE_URL=postgres://handbook:handbook@127.0.0.1:5439/handbook
pnpm vitest run 09-production/usecase-02-order-events
go test -race ./09-production/usecase-02-order-events/...
```

Mỗi test dùng schema Postgres riêng với tên duy nhất và xóa nó khi kết thúc, nên chạy lại trên database còn dữ liệu cũ vẫn đúng.
Tương tự, `REDIS_PORT` và `KAFKA_PORT` đổi port của Redis và Kafka (nhớ đặt `REDIS_URL` và `KAFKA_BROKERS` tương ứng).

## Biến môi trường của lab

| Biến            | Mặc định                                               |
| --------------- | ------------------------------------------------------ |
| `REDIS_URL`     | `redis://127.0.0.1:6379`                               |
| `AMQP_URL`      | `amqp://guest:guest@127.0.0.1:5672`                    |
| `KAFKA_BROKERS` | `127.0.0.1:9092`                                       |
| `NATS_URL`      | `nats://127.0.0.1:4222`                                |
| `DATABASE_URL`  | `postgres://handbook:handbook@127.0.0.1:5432/handbook` |

## Testkit

Mọi lab dùng chung helper, không dùng `sleep` cố định.

| TypeScript (`@handbook/testkit`)     | Go (`internal/testkit`)         | Mục đích                                                                                      |
| ------------------------------------ | ------------------------------- | --------------------------------------------------------------------------------------------- |
| `waitForPort(host, port, timeoutMs)` | `WaitForPort(t, addr, timeout)` | Đợi broker mở port, quá hạn thì báo lỗi kèm `host:port`, không treo vô hạn                    |
| `uniqueName(prefix)`                 | `UniqueName(prefix)`            | Tên key, queue, topic duy nhất mỗi lần chạy, chạy lại lab trên broker còn dữ liệu cũ vẫn đúng |
| `eventually(fn, { timeoutMs })`      | `Eventually(t, timeout, fn)`    | Polling có timeout thay cho `sleep`                                                           |

## Redis Sentinel và Cluster từ máy host

Các node trong profile `sentinel` và `cluster` tự giới thiệu bằng tên Docker DNS (ví dụ `redis-master:6380`, `redis-cluster-1:7001`), vì các node phải gọi được nhau bên trong network của Docker.
Client chạy trên máy host cần ánh xạ các tên này về `127.0.0.1` với cùng port (`natMap` của ioredis, `Dialer` của go-redis).
Các lab ở chủ đề 04 làm sẵn phần này.

## CI

Workflow `.github/workflows/ci.yml` chạy khi push lên `main` và với mỗi pull request.
Các job chạy song song.
Các job test tự dựng broker bằng `make up` (không dùng service container riêng), nên môi trường giống lúc chạy local.
Job `lint` không cần broker, chỉ `check-compose` hỏi registry xem image có đủ kiến trúc.
Chủ đề 08 và 09 không thêm job nào: test của chúng chạy trong `test-ts` và `test-go` (`make test-fast`), vì chúng chỉ cần các broker của `make up` thường (Redis, RabbitMQ, Kafka, Postgres).

| Job          | Làm gì                                                                                                                                                        | Chạy local bằng                                         |
| ------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------- |
| `lint`       | eslint, prettier, tsc, `golangci-lint`, cổng no-sleep, `check-compose` (strict), kiểm tra tài liệu (theory, README của lab và use case), mermaid, test script | `make lint docs-check mermaid-check test-scripts`       |
| `test-ts`    | `make up`, rồi vitest cho mọi lab trừ sentinel và cluster                                                                                                     | `make up && make test-fast-ts`                          |
| `test-go`    | `make up`, rồi `go test -p 1` cho mọi lab trừ sentinel và cluster                                                                                             | `make up && make test-fast-go`                          |
| `test-chaos` | `make up PROFILE="sentinel cluster"`, rồi test của lab sentinel và cluster, gồm test chaos (mỗi bước tối đa 10 phút)                                          | `make up PROFILE="sentinel cluster" && make test-chaos` |

Có hai loại test chaos với hai cách chạy khác nhau.
Test chaos của chủ đề 08 và 09 giết một process con (consumer hoặc relay) bằng SIGKILL và không bao giờ dừng container dùng chung, nên chúng chạy trong `make test-fast` như mọi test khác.
Test chaos của chủ đề 04 nằm trong hai lab sentinel và cluster (`describe("chaos")` trong TypeScript, `TestChaos*` trong Go).
Chúng dừng và khởi động lại container Redis nên tách thành job riêng, không có `continue-on-error`.
Hai lab này cần cả hai profile, vì vậy `make test-fast` bỏ qua cả hai lab chứ không chỉ riêng test chaos.

Trên CI, `check-compose` chạy với `CHECK_COMPOSE_STRICT=1`: gặp rate limit của Docker Hub là lỗi.
Nếu cần, thêm secret `DOCKERHUB_USERNAME` và `DOCKERHUB_TOKEN` của repo, job `lint` sẽ tự `docker login` (bước này bị bỏ qua khi thiếu secret).
`mmdc` trên runner ubuntu cần Chromium chạy với `--no-sandbox`: workflow tạo file cấu hình puppeteer và trỏ biến `MERMAID_PUPPETEER_CONFIG` tới nó.
Local không cần biến này, dùng Chromium do `pnpm install` tải.

## Giới hạn đã biết

Handbook là tài liệu học và kho lab chạy trên máy cá nhân, không phải cấu hình production.
Những điều dưới đây được cố ý bỏ ra ngoài, mỗi chủ đề còn có mục giới hạn riêng trong `theory.md` và README của lab:

- Redlock và lock nhiều node: use case 3 chỉ cài lock trên MỘT node Redis, không cài Redlock và không đo việc lock bị mất khi failover (cần Sentinel hoặc Cluster).
  Phần tranh luận Kleppmann và antirez chỉ có ở mức lý thuyết, kèm khuyến nghị dùng fencing token hoặc hệ consensus khi cần correctness.
- Broker chết: test chaos của chủ đề 08 và 09 chỉ giết process con (consumer, relay) bằng SIGKILL.
  Không có test nào dừng hoặc restart RabbitMQ, Kafka, NATS hay Postgres, nên persistence (AOF của Redis), reconnect sau khi broker restart và publish chưa confirm khi broker chết chưa được kiểm tra.
  Chỉ hai lab Sentinel và Cluster của chủ đề 04 dừng và khởi động lại container Redis.
- Network partition, latency và message bị đảo hoặc nhân đôi do mạng: chưa có lab nào (Toxiproxy chỉ được nhắc như tên công cụ).
- Cụm nhiều node: RabbitMQ, Kafka và NATS đều chạy một node trong compose, nên chưa đo failover, ISR, replication factor 3 hay Raft của JetStream.
  Transaction và exactly-once của Kafka chưa được chạy (kafka-go không có API transaction).
- TLS và xác thực: mọi port bind `127.0.0.1` với credential mặc định, không có TLS, không có ACL hay xác thực người dùng, và use case 4 chỉ áp chính sách Origin mặc định.
  Đừng đưa cấu hình compose này ra ngoài máy cục bộ.
- Persistence: Redis của compose chạy với `--save "" --appendonly no`, nên RDB và AOF chỉ là lý thuyết ở chủ đề 02.
- Sizing production: không có số liệu về kích thước cụm, số partition, số worker hay chi phí.
  Các số đo trong README là kết quả của một lần chạy trên một máy, không phải benchmark và không phải cam kết.
  Bảng chọn công nghệ ở [chủ đề 09](./09-production/theory.md) không xếp hạng throughput vì research không có số đo so sánh.
- Phần chưa xác minh: mỗi chương ghi rõ điều nào chỉ đọc từ tài liệu hoặc source mà chưa chạy trong lab (ví dụ hành vi worker chết của asynq, chi phí `PUBLISH` theo số subscriber).
- Vòng đời phiên bản: RabbitMQ series 4.3 hết community support ngày 2026-11-30, và mọi phiên bản trong bảng trên được kiểm tra ngày 2026-10-06.
