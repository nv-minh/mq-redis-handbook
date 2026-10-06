# MQ & Redis Handbook

Tài liệu lý thuyết và lab thực hành về Message Queue và Redis, đi từ cơ bản đến các use case production.
Mỗi lab có hai phiên bản: TypeScript (Node 22) và Go, đều có test chạy với broker thật qua Docker.
Nội dung viết bằng tiếng Việt, thuật ngữ kỹ thuật giữ tiếng Anh.

## Lộ trình học

Đọc theo thứ tự 01 đến 09.
Chủ đề nào chưa có thư mục thì được đánh dấu "chưa viết".

| #   | Chủ đề                                                                                         | Trạng thái |
| --- | ---------------------------------------------------------------------------------------------- | ---------- |
| 01  | [Fundamentals](./01-fundamentals/) - vì sao cần MQ, delivery semantics, ordering, backpressure | chưa viết  |
| 02  | [Redis core](./02-redis-core/) - data types, persistence, eviction, Lua, pipelining            | chưa viết  |
| 03  | [Redis messaging](./03-redis-messaging/) - Pub/Sub, List, Streams, consumer group              | chưa viết  |
| 04  | [Redis nâng cao](./04-redis-advanced/) - replication, Sentinel, Cluster, hot key, big key      | chưa viết  |
| 05  | [RabbitMQ](./05-rabbitmq/) - exchange, ack, prefetch, DLX, quorum queue                        | chưa viết  |
| 06  | [Kafka](./06-kafka/) - log, partition, consumer group, offset, transaction                     | chưa viết  |
| 07  | [NATS JetStream](./07-nats-jetstream/) - core NATS, stream, durable consumer                   | chưa viết  |
| 08  | [Patterns](./08-patterns/) - idempotency, retry, DLQ, outbox, saga                             | chưa viết  |
| 09  | [Production](./09-production/) - job queue, event-driven, rate limiter, realtime               | chưa viết  |

## Yêu cầu

- Node 22 và pnpm.
- Go 1.26 trở lên (`go.mod` dùng directive `go 1.26.0`, vì `go-redis` v9.23.0 yêu cầu Go >= 1.26).
  Nếu Go cài sẵn cũ hơn, `GOTOOLCHAIN=auto` (mặc định) tự tải toolchain 1.26 lần chạy đầu.
- Docker với Compose v2 (image đều multi-arch: Apple Silicon arm64 và amd64).

`make lint` chạy `golangci-lint` v2.14.0 qua `go run`, và bản này tự tải toolchain Go mới hơn nếu cần (`GOTOOLCHAIN=auto`).
Nếu đã cài sẵn `golangci-lint` trong `PATH` thì Makefile dùng bản đó.
Sơ đồ Mermaid được kiểm tra bằng `@mermaid-js/mermaid-cli` (devDependency, tải Chrome khi `pnpm install`).

## Bắt đầu nhanh

```bash
pnpm install
make up                      # redis, rabbitmq, kafka, nats, postgres
make up PROFILE=sentinel     # thêm Redis Sentinel (master + 2 replica + 3 sentinel)
make up PROFILE=cluster      # thêm Redis Cluster (6 node, tự khởi tạo)
make up PROFILE="sentinel cluster" # cả hai topology cùng lúc (test chủ đề 04 cần cả hai)
make lab-ts LAB=<NN-ten/lab-NN-ten>
make lab-go LAB=<NN-ten/lab-NN-ten>
make test                    # test script, TS và Go
make lint
make docs-check
make down
```

Mọi port của broker chỉ bind `127.0.0.1`.
Nếu port mặc định đã bị chiếm, đổi port phía host rồi trỏ biến môi trường của lab tới port mới, ví dụ:

```bash
REDIS_PORT=6390 KAFKA_PORT=9094 POSTGRES_PORT=5433 make up
export REDIS_URL=redis://127.0.0.1:6390 KAFKA_BROKERS=127.0.0.1:9094 DATABASE_URL=postgres://handbook:handbook@127.0.0.1:5433/handbook
```

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
