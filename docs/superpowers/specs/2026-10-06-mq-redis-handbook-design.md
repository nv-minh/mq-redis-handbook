# Thiết kế: MQ & Redis Handbook

Ngày: 2026-10-06
Trạng thái: chờ review
Cập nhật 2026-10-06: áp dụng ba điều chỉnh của plan (xem cuối mục 4 và mục 5).

## 1. Mục tiêu và bối cảnh

Dựng một repo công khai gồm tài liệu lý thuyết và các bài lab thực hành về Message Queue và Redis.
Nội dung đi từ cơ bản đến nâng cao, kết thúc bằng các use case production thực tế.
Mỗi lab có hai phiên bản: TypeScript và Golang.
Người dùng chính là chủ repo, dùng để tự học và làm tài liệu tham khảo lâu dài.

Tiêu chí thành công:
- Đọc theo thứ tự 01 đến 09 là có lộ trình học liền mạch.
- Mọi lab chạy được bằng `make up` rồi `make lab-ts` hoặc `make lab-go`.
- Mọi lab có test tự động chạy với broker thật.
- Mỗi chủ đề có hình vẽ architecture và flow.
- CI xanh trên GitHub Actions.

## 2. Phạm vi

Trong phạm vi:
- Công nghệ: Redis, RabbitMQ, Kafka, NATS JetStream.
- Ngôn ngữ: TypeScript (Node 22) và Go (1.23).
- Ngôn ngữ tài liệu: tiếng Việt, thuật ngữ kỹ thuật giữ tiếng Anh.
- Đích push: GitHub account `nv-minh`, repo `mq-redis-handbook` (tên tạm), public.

Ngoài phạm vi (YAGNI):
- Web UI cho handbook.
- Bản tiếng Anh.
- Pulsar, ActiveMQ, SQS.

## 3. Lộ trình nội dung

| # | Chủ đề | Lý thuyết | Lab |
|---|---|---|---|
| 01 | Fundamentals | Vì sao cần MQ, queue vs pub/sub vs log, delivery semantics, ordering, backpressure | Producer/consumer in-memory, mô phỏng mất và trùng message |
| 02 | Redis core | Data types, persistence (RDB/AOF), eviction, transaction và Lua, pipelining | Cấu trúc dữ liệu cơ bản, pipeline, Lua script |
| 03 | Redis messaging | Pub/Sub, List (BRPOPLPUSH), Streams, consumer group, XAUTOCLAIM | Ba cách làm queue trên Redis và so sánh độ tin cậy |
| 04 | Redis nâng cao | Replication, Sentinel, Cluster, HA, hot key, big key | Failover Sentinel, hash slot trên Cluster |
| 05 | RabbitMQ | Exchange (direct, topic, fanout), ack, prefetch, DLX, quorum queue | Work queue, topic routing, retry bằng DLX và TTL |
| 06 | Kafka | Log, partition, consumer group, offset, rebalance, transaction | Producer key và partition, consumer group, replay |
| 07 | NATS JetStream | Core NATS, stream, durable consumer, ack policy | Pub/sub, queue group, JetStream replay |
| 08 | Patterns | Idempotency, retry và backoff, DLQ, outbox, saga, competing consumers | Cài từng pattern, có test chaos |
| 09 | Production | Bốn use case bên dưới, so sánh chọn công nghệ | Mini project chạy bằng Docker Compose |

Bốn use case của mục 09:
1. Job queue: BullMQ (TS) và asynq (Go), retry, DLQ, scheduling, graceful shutdown, Prometheus metrics.
2. Order/Payment event-driven: Outbox, Saga, idempotent consumer, theo dõi consumer lag.
3. Rate limiter, distributed lock, cache: sliding window, Redlock, cache-aside, chống cache stampede.
4. Realtime: chat, notification, leaderboard, WebSocket fan-out nhiều instance.

## 4. Cấu trúc repo

```
mq-redis-handbook/
  README.md                  # lộ trình học
  Makefile
  infra/docker-compose.yml   # Redis, RabbitMQ, Kafka (KRaft), NATS, Postgres
  package.json               # một package.json ở gốc cho toàn bộ TS
  go.mod                     # một module Go ở gốc
  pnpm-workspace.yaml
  packages/testkit-ts/       # helper test dùng chung cho TS
  internal/testkit/          # helper test dùng chung cho Go
  scripts/                   # check-docs.sh, check-compose.sh
  NN-ten-chu-de/
    theory.md
    lab-NN-ten/
      README.md
      ts/                    # lab.ts, demo.ts, lab.test.ts (vitest), thư mục thường
      go/                    # lab.go, lab_test.go, demo/main.go
  .github/workflows/ci.yml
  docs/superpowers/specs/    # spec này
```

Mỗi `theory.md` gồm:
- Architecture diagram (`flowchart`).
- Flow diagram (`sequenceDiagram`), có cả happy path và failure path.
- State diagram (`stateDiagram-v2`) cho vòng đời message khi cần.
- Bảng so sánh.
- Mục "Nguồn tham khảo" kèm phiên bản công nghệ.

Mỗi lab README gồm: mục tiêu, sơ đồ kiến trúc của lab, các bước chạy, kết quả mong đợi, bài tập mở rộng.
Lab ở mục 08 và 09 có thêm sơ đồ failure scenario.

Hình vẽ dùng Mermaid nhúng trong Markdown, vì GitHub render sẵn và diff được như code.

Điều chỉnh (2026-10-06):
- Hạ tầng thêm Postgres vào `infra/docker-compose.yml`, vì outbox và saga cần DB.
- Dùng một `package.json` ở gốc thay cho `package.json` từng lab, vì 30 lab lặp lại cùng một bộ dependency. Các lab là thư mục thường, không phải workspace package.

## 5. Tooling

TypeScript:
- Node 22, pnpm workspace.
- Thư viện: `ioredis`, `amqplib`, `@confluentinc/kafka-javascript`, `@nats-io/transport-node` và `@nats-io/jetstream`, `bullmq`.
- Điều chỉnh (2026-10-06): `kafkajs` được thay bằng `@confluentinc/kafka-javascript` vì `kafkajs` không được cập nhật từ 2023-02. `nats` được thay bằng `@nats-io/transport-node` và `@nats-io/jetstream` vì package cũ đã được đánh dấu moved.
- `typescript` 7 chạy song song với API TypeScript 6 (alias `@typescript/typescript6`), vì `typescript-eslint` chưa hỗ trợ TS 7.
- Test: vitest. Lint: eslint. Format: prettier.

Go:
- Go 1.23, một `go.mod` ở gốc.
- Thư viện: `go-redis/v9`, `amqp091-go`, `segmentio/kafka-go`, `nats.go`, `asynq`.
- Test: `testing`. Lint: `golangci-lint`.

Chung:
- Thư viện cụ thể và phiên bản được chốt lại khi lập plan, sau khi research xác nhận còn được duy trì.
- Lệnh chạy: `make up`, `make down`, `make lab-ts LAB=...`, `make lab-go LAB=...`, `make test`.

## 6. Kiểm thử

- Integration test chạy với broker thật qua Docker, không mock.
- Lab mục 08 có test chaos: kill container broker hoặc consumer giữa chừng, rồi kiểm tra không mất hoặc trùng message ngoài ý muốn.
- Số liệu và hành vi nêu trong lý thuyết phải có nguồn, hoặc được lab chứng minh bằng test.

## 7. CI

GitHub Actions, ba job:
1. Lint: eslint, prettier, golangci-lint, và `mermaid-cli` kiểm tra mọi sơ đồ parse được.
2. Test TS với service container.
3. Test Go với service container.

Account `nv-minh` có scope `workflow` nên push được file workflow.

## 8. Quy trình research

Năm sub-agent chạy song song theo nhóm: Redis (02, 03, 04), RabbitMQ, Kafka, NATS JetStream, Patterns và Production.

Thứ tự ưu tiên nguồn:
1. Tài liệu chính thức (redis.io, rabbitmq.com, kafka.apache.org, docs.nats.io).
2. Engineering blog của các công ty lớn.
3. Sách và talk uy tín.

Mỗi agent giao nộp: outline lý thuyết, khái niệm bắt buộc, lỗi thường gặp ở production, danh sách URL đã đọc.
Mỗi agent ghi rõ phiên bản công nghệ và loại bỏ nội dung lỗi thời (ví dụ Kafka với ZooKeeper, mirrored queue của RabbitMQ).

## 9. Milestone và quy trình push

- M1: khung repo, mục 01 đến 04 (Redis).
- M2: mục 05 đến 07 (RabbitMQ, Kafka, NATS).
- M3: mục 08 (Patterns).
- M4: mục 09 (Production) và CI hoàn chỉnh.

Quy tắc:
- Tạo repo cục bộ trước, commit theo Conventional Commits.
- Không thêm tên agent làm co-author trong commit.
- Hỏi xác nhận trước khi tạo repo `nv-minh/mq-redis-handbook` và push, vì đây là hành động công khai.
- Mỗi milestone push một lần, sau khi test cục bộ xanh.

## 10. Quy ước viết tài liệu

- Mỗi câu một dòng trong file Markdown dài.
- Không dùng em dash, chỉ dùng dấu gạch ngang "-".
- Thuật ngữ kỹ thuật giữ tiếng Anh.

## 11. Rủi ro

- Khối lượng lớn (4 công nghệ x 2 ngôn ngữ): giảm bằng milestone, mỗi mốc dùng được độc lập.
- Test với Kafka và cluster chậm trên CI: dùng KRaft một node, đặt timeout rõ ràng, đánh dấu các test chaos để có thể tách job.
- Thư viện Go hoặc TS ngừng duy trì: kiểm tra ở bước plan, thay thế nếu cần.
