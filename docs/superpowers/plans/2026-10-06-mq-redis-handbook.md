# MQ & Redis Handbook Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Dựng repo handbook tiếng Việt về Message Queue và Redis, gồm 9 chủ đề, 30 lab, mỗi lab có phiên bản TypeScript và Go, kèm sơ đồ Mermaid và CI.

**Architecture:** Một monorepo chia theo chủ đề `NN-ten/theory.md` và `NN-ten/lab-NN-ten/{ts,go}`. Hạ tầng dùng chung qua `infra/docker-compose.yml` với profile cho Sentinel và Cluster. Một `package.json` và một `go.mod` ở gốc, các lab là thư mục thường (xem "Điều chỉnh so với spec").

**Tech Stack:** Node 22, pnpm, TypeScript, vitest, tsx, Go 1.23, Docker Compose, Mermaid, GitHub Actions.

**Spec:** `docs/superpowers/specs/2026-10-06-mq-redis-handbook-design.md`

## Điều chỉnh so với spec

Plan này sửa ba điểm của spec, và spec được cập nhật theo trong Task 1:
1. **Kafka TS client:** `@confluentinc/kafka-javascript` thay cho `kafkajs` (kafkajs không được cập nhật từ 2023-02).
2. **NATS TS client:** `@nats-io/transport-node` và `@nats-io/jetstream` thay cho `nats` (package cũ đã được đánh dấu moved).
3. **Hạ tầng và layout:** thêm Postgres vào `infra/docker-compose.yml` (outbox và saga cần DB). Dùng một `package.json` ở gốc thay cho package.json từng lab, vì 30 lab lặp lại cùng một bộ dependency.

## Global Constraints

- Node 22, Go 1.23 (nếu dependency buộc nâng `go` directive thì nâng cả CI và ghi vào README).
- Tài liệu bằng tiếng Việt, thuật ngữ kỹ thuật giữ tiếng Anh.
- Mỗi câu một dòng trong Markdown dài. Không dùng em dash, chỉ dùng "-".
- Integration test chạy với broker thật qua Docker, không mock.
- Mỗi `theory.md` có: một `flowchart`, một `sequenceDiagram` có failure path, bảng so sánh, mục `## Nguồn tham khảo` kèm phiên bản công nghệ.
- Mỗi lab có `README.md` với ít nhất một sơ đồ Mermaid. Lab ở mục 08 và 09 có thêm sơ đồ failure scenario.
- Commit theo Conventional Commits. Không thêm tên agent làm co-author.
- Không tạo repo GitHub và không push khi chưa hỏi xác nhận người dùng ngay trước thao tác đó. Đích: `nv-minh/mq-redis-handbook`, public.
- Phiên bản đã xác minh ngày 2026-10-06: ioredis 6.0.0, amqplib 2.2.0, @confluentinc/kafka-javascript 1.10.1, @nats-io/transport-node 3.4.0, @nats-io/jetstream 3.4.0, bullmq 6.3.11, vitest 5.0.3, typescript 7.0.2, tsx 4.23.15, eslint 10.12.0, prettier 3.9.9, @mermaid-js/mermaid-cli 12.0.0, go-redis v9.23.0, amqp091-go v1.15.0, kafka-go v0.4.51, nats.go v1.54.0, asynq v0.26.0.

## Review Focus

Các tình huống spec ngầm đòi hỏi nhưng không có task nào test trực tiếp, sắp theo khả năng gặp:
1. Chạy lại lab khi broker còn dữ liệu của lần chạy trước (key, queue, topic cũ): mọi tên tài nguyên phải duy nhất mỗi lần chạy. Test ở Task 1 (`uniqueName`).
2. Docker chưa chạy hoặc service chưa sẵn sàng: lỗi phải nói rõ port nào, đợi bao lâu, không treo vô hạn. Test ở Task 1 (`waitForPort`).
3. Mac Apple Silicon (arm64) và CI (amd64): image phải multi-arch. Kiểm tra ở Task 1 (`scripts/check-compose.sh`).
4. Cổng broker bind ra mạng ngoài với credential mặc định: mọi port phải bind `127.0.0.1`. Kiểm tra ở Task 1 (`scripts/check-compose.sh`).
5. Test phụ thuộc `sleep` cố định gây flaky: mọi chờ phải là polling có timeout. Helper `eventually` ở Task 1, mọi lab dùng helper này.

## Quy ước chung cho task theo chủ đề (Task 3 đến 6, 8 đến 10, 12 đến 16)

Mỗi task chủ đề `NN-ten/` thực hiện chuỗi bước sau, áp dụng cho từng lab trong bảng của task.
Bảng liệt kê tên test, mỗi tên phải có test tương ứng ở cả TS và Go với cùng ý nghĩa.

- [ ] **Bước A: Đọc `docs/research/<nhóm>.md`** (tạo ở Task 2) và dùng nó làm nguồn cho theory.
- [ ] **Bước B: Viết test thất bại** cho lab trong `lab-NN-ten/ts/lab.test.ts` và `lab-NN-ten/go/lab_test.go`, dùng `uniqueName` và `eventually` của testkit.
- [ ] **Bước C: Chạy test, xác nhận FAIL** vì thiếu implementation. Lệnh: `pnpm vitest run NN-ten/lab-NN-ten/ts` và `go test ./NN-ten/lab-NN-ten/go/...`.
- [ ] **Bước D: Implement** `ts/lab.ts` và `go/lab.go` (package `lab`) cho tới khi test PASS. Thêm `ts/demo.ts` và `go/demo/main.go` in kết quả dễ đọc cho `make lab-ts` và `make lab-go`.
- [ ] **Bước E: Viết `lab-NN-ten/README.md`** (mục tiêu, sơ đồ Mermaid, các bước chạy, kết quả mong đợi, bài tập mở rộng).
- [ ] **Bước F: Commit mỗi lab:** `git add NN-ten/lab-NN-ten && git commit -m "feat(NN-ten): add lab-NN-ten"`.
- [ ] **Bước G (một lần mỗi chủ đề, riêng 08 làm ở Task 13 và 09 làm ở Task 17): Viết `NN-ten/theory.md`**, rồi chạy `scripts/check-docs.sh NN-ten`, kỳ vọng exit 0. Commit `docs(NN-ten): add theory`.
- [ ] **Bước H (một lần mỗi chủ đề): Chạy `make lint && make test`**, kỳ vọng xanh.

## File Structure

| Đường dẫn | Trách nhiệm |
|---|---|
| `Makefile` | Điểm vào duy nhất: up, down, lab-ts, lab-go, test, lint, docs-check |
| `infra/docker-compose.yml` | Redis, RabbitMQ, Kafka KRaft, NATS, Postgres; profile `sentinel`, `cluster` |
| `packages/testkit-ts/` | `waitForPort`, `uniqueName`, `eventually` cho TS |
| `internal/testkit/` | `WaitForPort`, `UniqueName`, `Eventually` cho Go |
| `scripts/check-docs.sh` | Kiểm tra theory.md: diagram, nguồn, em dash |
| `scripts/check-compose.sh` | Kiểm tra port bind 127.0.0.1 và image multi-arch |
| `docs/research/*.md` | Kết quả research và URL nguồn, đầu vào cho theory |
| `NN-ten/theory.md`, `NN-ten/lab-*/` | Nội dung từng chủ đề |
| `.github/workflows/ci.yml` | Lint (kèm mermaid-cli), test TS, test Go |

---

### Task 1: Khung repo, testkit và hạ tầng

**Files:**
- Create: `Makefile`, `package.json`, `pnpm-workspace.yaml`, `tsconfig.json`, `vitest.config.ts`, `eslint.config.js`, `.prettierrc`, `go.mod`, `.golangci.yml`, `.gitignore`, `README.md`
- Create: `infra/docker-compose.yml`, `scripts/check-compose.sh`, `scripts/check-docs.sh`, `scripts/check-docs.test.sh`
- Create: `packages/testkit-ts/src/index.ts`, `packages/testkit-ts/src/index.test.ts`, `internal/testkit/testkit.go`, `internal/testkit/testkit_test.go`
- Modify: `docs/superpowers/specs/2026-10-06-mq-redis-handbook-design.md` (áp dụng mục "Điều chỉnh so với spec")

**Interfaces:**
- Produces (TS, `packages/testkit-ts/src/index.ts`): `waitForPort(host: string, port: number, timeoutMs: number): Promise<void>`, `uniqueName(prefix: string): string`, `eventually<T>(fn: () => Promise<T | undefined | false>, opts: { timeoutMs: number; intervalMs?: number }): Promise<T>`.
- Produces (Go, `internal/testkit`): `WaitForPort(t testing.TB, addr string, timeout time.Duration)`, `UniqueName(prefix string) string`, `Eventually[T any](t testing.TB, timeout time.Duration, fn func() (T, bool)) T`.
- Produces (env, đọc bởi mọi lab, mặc định trong ngoặc): `REDIS_URL` (`redis://127.0.0.1:6379`), `AMQP_URL` (`amqp://guest:guest@127.0.0.1:5672`), `KAFKA_BROKERS` (`127.0.0.1:9092`), `NATS_URL` (`nats://127.0.0.1:4222`), `DATABASE_URL` (`postgres://handbook:handbook@127.0.0.1:5432/handbook`).
- Produces (Makefile): `make up [PROFILE=sentinel|cluster]`, `make down`, `make lab-ts LAB=<NN-ten/lab-NN-ten>`, `make lab-go LAB=<...>`, `make test`, `make test-ts`, `make test-go`, `make lint`, `make docs-check`.

- [ ] **Step 1: Viết test thất bại cho testkit TS** trong `packages/testkit-ts/src/index.test.ts`: `waitForPort rejects with host:port in message after timeout` (cổng đóng, timeout 300ms, message chứa `127.0.0.1:` và số port), `waitForPort resolves when a listener opens` (mở `net.createServer` trễ 100ms), `uniqueName returns different values for two calls with same prefix` và bắt đầu bằng prefix, `eventually returns first truthy value`, `eventually rejects after timeout with last error`.
- [ ] **Step 2: Chạy `pnpm vitest run packages/testkit-ts`, kỳ vọng FAIL** (module chưa có).
- [ ] **Step 3: Implement testkit TS** theo signature ở Interfaces. `uniqueName` = `${prefix}-${Date.now().toString(36)}-${random 6 ký tự}`. `eventually` poll mặc định 100ms.
- [ ] **Step 4: Chạy lại, kỳ vọng PASS.**
- [ ] **Step 5: Test và implement testkit Go** trong `internal/testkit` với bộ test tương ứng: `TestWaitForPortFailsWithAddrInMessage` (dùng một `testing.TB` fake ghi lại `Fatalf`), `TestWaitForPortSucceedsWhenListenerOpens`, `TestUniqueNameDiffers`, `TestEventuallyReturnsValue`. Chạy `go test ./internal/testkit/...`, kỳ vọng PASS sau khi implement (chạy FAIL trước khi implement).
- [ ] **Step 6: Viết `scripts/check-docs.test.sh`** với fixture tạm trong `mktemp -d`: (a) theory đủ flowchart, sequenceDiagram, `## Nguồn tham khảo` thì exit 0, (b) thiếu sequenceDiagram thì exit 1 và in tên file, (c) có ký tự em dash thì exit 1, (d) thiếu mục nguồn thì exit 1. Chạy, kỳ vọng FAIL (script chưa có).
- [ ] **Step 7: Implement `scripts/check-docs.sh <thư-mục-chủ-đề>...`** (bash, grep) để pass 4 ca trên, rồi chạy lại `bash scripts/check-docs.test.sh`, kỳ vọng PASS. Thêm vào đầu script: quét cả README của lab trong thư mục chủ đề, yêu cầu có khối ```` ```mermaid ````.
- [ ] **Step 8: Viết `infra/docker-compose.yml`:** service `redis` (6379), `rabbitmq` (5672, quản trị 15672, bật quorum queue mặc định), `kafka` (KRaft một node, 9092), `nats` (`-js`, 4222), `postgres` (5432, user/db/password `handbook`). Profile `sentinel` (master 6380, 2 replica, 3 sentinel 26379-26381) và `cluster` (6 node 7001-7006 kèm job khởi tạo). Mọi port ghi dạng `127.0.0.1:HOST:CONTAINER`. Mọi service có `healthcheck`. Image pin tag cụ thể, chọn bản stable mới nhất đã xác minh bằng `docker manifest inspect`.
- [ ] **Step 9: Viết `scripts/check-compose.sh`** kiểm tra (a) không có dòng port nào thiếu tiền tố `127.0.0.1:`, (b) mỗi image có manifest cho cả `linux/arm64` và `linux/amd64`. Chạy, kỳ vọng exit 0. Nếu image nào thiếu arm64, đổi image.
- [ ] **Step 10: Viết Makefile, config và README khung.** `make up` chạy `docker compose up -d --wait`. `README.md` có lộ trình 9 chủ đề (link tới thư mục, đánh dấu chưa viết) và mục yêu cầu (Node 22, Go, Docker). Chạy `go mod tidy` và `pnpm install`. Nếu dependency buộc nâng `go` directive thì cập nhật `go.mod`, ghi vào README.
- [ ] **Step 11: Áp dụng mục "Điều chỉnh so với spec" vào spec** (sửa mục 5 Tooling và mục 4 Cấu trúc, thêm Postgres vào hạ tầng, đánh dấu cập nhật 2026-10-06).
- [ ] **Step 12: Verify:** `make up` và `docker compose ps` đều healthy, `make lint`, `make test-ts`, `make test-go`, `make docs-check` (không có chủ đề nào thì không lỗi). Kỳ vọng tất cả exit 0.
- [ ] **Step 13: Commit:** `git add -A && git commit -m "chore: scaffold repo, testkit and infra"`.

---

### Task 2: Research song song và ghi chú nguồn

**Files:**
- Create: `docs/research/redis.md`, `docs/research/rabbitmq.md`, `docs/research/kafka.md`, `docs/research/nats.md`, `docs/research/patterns-production.md`

**Interfaces:**
- Produces: mỗi file có các mục `## Phiên bản`, `## Khái niệm bắt buộc`, `## Lỗi thường gặp ở production`, `## Nguồn` (mỗi dòng một URL kèm ngày truy cập). Các task chủ đề dùng làm nguồn cho `## Nguồn tham khảo` của theory.

- [ ] **Step 1: Chạy 5 agent song song**, mỗi agent một file: Redis (02, 03, 04), RabbitMQ (05), Kafka (06), NATS (07), Patterns và Production (08, 09). Thứ tự nguồn: tài liệu chính thức, engineering blog, sách và talk.
- [ ] **Step 2: Mỗi agent bắt buộc kiểm tra các điểm sau và ghi kết luận kèm URL:** `BLMOVE` thay `BRPOPLPUSH` ở Redis, hành vi `XAUTOCLAIM`, tranh luận Redlock (Kleppmann và antirez), RabbitMQ quorum queue so với classic mirrored queue (đã bỏ), Kafka KRaft không còn ZooKeeper, NATS JetStream `AckWait` và `MaxDeliver`, tính chất at-least-once của outbox.
- [ ] **Step 3: Rà soát:** mỗi file đủ 4 mục, mọi URL mở được (`curl -sI -o /dev/null -w "%{http_code}"` trả 200 hoặc 3xx), không có nội dung lỗi thời theo danh sách ở Step 2.
- [ ] **Step 4: Commit:** `git add docs/research && git commit -m "docs: add research notes with sources"`.

---

### Task 3: Chủ đề 01 - Fundamentals

Áp dụng "Quy ước chung cho task theo chủ đề". Không cần broker, lab chạy in-memory.

**Files:**
- Create: `01-fundamentals/theory.md`, `01-fundamentals/lab-01-delivery-semantics/{README.md,ts/,go/}`, `01-fundamentals/lab-02-backpressure/{README.md,ts/,go/}`

**Interfaces:**
- Produces (TS `lab-01-delivery-semantics/ts/lab.ts`): `createQueue(opts: { mode: 'at-most-once' | 'at-least-once'; lossRate: number; rand?: () => number }): Queue`, với `Queue.publish(id: string): void`, `Queue.consume(handler: (id: string) => void): void`, `Queue.drain(): Promise<void>`. Go tương ứng: `NewQueue(opts Options) *Queue`.
- Produces (`lab-02-backpressure`): `BoundedQueue<T>` với `publish(msg: T): Promise<void>` (chờ khi đầy), `consume(handler)`, `size(): number`. Go: `Publish(ctx, msg) error`.

| Lab | Tên test |
|---|---|
| lab-01-delivery-semantics | `at_most_once_loses_messages_when_consumer_crashes`, `at_least_once_duplicates_when_ack_is_lost`, `idempotent_consumer_applies_each_id_once` |
| lab-02-backpressure | `publish_blocks_when_queue_full`, `size_never_exceeds_capacity_with_slow_consumer` |

Review Focus: `at_least_once_duplicates_when_ack_is_lost` chạy với `lossRate = 1` (mất toàn bộ ack) để chốt hành vi cực biên. `publish_blocks_when_queue_full` có ca `capacity = 1`.

---

### Task 4: Chủ đề 02 - Redis core

Áp dụng "Quy ước chung cho task theo chủ đề". Cần `make up`.

**Files:**
- Create: `02-redis-core/theory.md`, `02-redis-core/lab-01-datatypes/`, `02-redis-core/lab-02-pipeline-lua/`, `02-redis-core/lab-03-eviction/`

**Interfaces:**
- Produces (`lab-01-datatypes`): `Leaderboard` với `add(name: string, score: number): Promise<void>`, `top(n: number): Promise<string[]>`. Go: `NewLeaderboard(rdb *redis.Client, key string)`.
- Produces (`lab-02-pipeline-lua`): `decrIfPositive(key: string): Promise<boolean>` (Lua, atomic). Go: `DecrIfPositive(ctx, rdb, key) (bool, error)`.
- Produces (`lab-03-eviction`): `fillUntilEviction(rdb, prefix: string, maxKeys: number): Promise<number>` trả số key bị đẩy ra.

| Lab | Tên test |
|---|---|
| lab-01-datatypes | `top3_returns_highest_scores_in_order`, `top_on_empty_board_returns_empty_list` |
| lab-02-pipeline-lua | `pipeline_uses_fewer_round_trips_than_sequential`, `decr_if_positive_never_goes_below_zero_under_50_concurrent_callers`, `exactly_n_callers_succeed_when_counter_is_n` |
| lab-03-eviction | `allkeys_lru_evicts_cold_keys_and_keeps_hot_key`, `noeviction_policy_rejects_writes_when_full` |

`lab-03-eviction` đặt `maxmemory` và `maxmemory-policy` bằng `CONFIG SET` trên một key prefix duy nhất và khôi phục cấu hình cũ trong `afterAll` và `t.Cleanup`.

---

### Task 5: Chủ đề 03 - Redis messaging

Áp dụng "Quy ước chung cho task theo chủ đề". Cần `make up`.

**Files:**
- Create: `03-redis-messaging/theory.md` (kèm bảng so sánh Pub/Sub, List, Streams), `03-redis-messaging/lab-01-pubsub/`, `03-redis-messaging/lab-02-list-queue/`, `03-redis-messaging/lab-03-streams/`

**Interfaces:**
- Produces (`lab-02-list-queue`): `ReliableQueue` với `enqueue(msg: string)`, `dequeue(consumerId: string): Promise<string | null>` (dùng `BLMOVE` sang list `processing:<consumerId>`), `ack(consumerId: string, msg: string)`, `recoverStale(consumerId: string): Promise<number>`.
- Produces (`lab-03-streams`): `StreamQueue` với `publish(fields: Record<string,string>): Promise<string>`, `consume(group: string, consumer: string, count: number)`, `ack(group: string, id: string)`, `claimStale(group: string, consumer: string, minIdleMs: number)`.

| Lab | Tên test |
|---|---|
| lab-01-pubsub | `subscriber_misses_messages_published_while_offline`, `all_subscribers_receive_each_message` |
| lab-02-list-queue | `message_stays_in_processing_list_until_ack`, `message_is_recovered_after_consumer_crash` |
| lab-03-streams | `each_message_goes_to_one_consumer_in_group`, `xack_removes_entry_from_pending_list`, `xautoclaim_recovers_pending_from_dead_consumer` |

---

### Task 6: Chủ đề 04 - Redis nâng cao

Áp dụng "Quy ước chung cho task theo chủ đề". Cần `make up PROFILE=sentinel` và `PROFILE=cluster`. Test dùng docker CLI để dừng container và gắn nhãn chaos (`describe('chaos')`, `TestChaos...`).

**Files:**
- Create: `04-redis-advanced/theory.md`, `04-redis-advanced/lab-01-sentinel/`, `04-redis-advanced/lab-02-cluster/`, `04-redis-advanced/lab-03-hot-big-key/`

**Interfaces:**
- Produces (`lab-01-sentinel`): `connectViaSentinel(): Redis` (TS dùng ioredis `sentinels` + `name`), Go dùng `redis.NewFailoverClient`.
- Produces (`lab-02-cluster`): `slotFor(key: string): number` (CRC16 mod 16384, tự cài, so sánh với `CLUSTER KEYSLOT`).
- Produces (`lab-03-hot-big-key`): `findBigKeys(rdb, thresholdBytes: number): Promise<string[]>` (SCAN + `MEMORY USAGE`).

| Lab | Tên test |
|---|---|
| lab-01-sentinel | `chaos_client_writes_succeed_within_30s_after_master_is_stopped`, `chaos_old_master_rejoins_as_replica` |
| lab-02-cluster | `slot_for_matches_cluster_keyslot`, `keys_with_same_hash_tag_share_a_slot`, `multi_key_command_across_slots_fails_with_crossslot` |
| lab-03-hot-big-key | `find_big_keys_returns_key_over_threshold`, `find_big_keys_ignores_small_keys` |

---

### Task 7: Cổng M1 - CI và push lần đầu

**Files:**
- Create: `.github/workflows/ci.yml`
- Modify: `README.md` (đánh dấu chủ đề 01 đến 04 đã xong)

**Interfaces:**
- Consumes: Makefile targets từ Task 1.
- Produces: ba job `lint`, `test-ts`, `test-go`. Job `lint` chạy eslint, prettier check, `golangci-lint`, `make docs-check`, và `mmdc` kiểm tra parse mọi khối mermaid. Job test dùng `docker compose up -d --wait` (không dùng service container riêng để đồng bộ với local). Test chaos tách bước riêng có `continue-on-error: false` và timeout 10 phút.

- [ ] **Step 1: Viết script `scripts/check-mermaid.sh`** trích mọi khối ```` ```mermaid ```` trong `*.md` ra file tạm và chạy `mmdc -i` cho từng khối, exit 1 nếu parse lỗi. Test: một fixture có khối mermaid sai cú pháp phải làm script exit 1, khối đúng exit 0.
- [ ] **Step 2: Viết `ci.yml`** theo Interfaces. Chạy `actionlint` nếu có để kiểm tra cú pháp.
- [ ] **Step 3: Verify cục bộ:** `make lint && make docs-check && bash scripts/check-mermaid.sh && make test`, kỳ vọng exit 0.
- [ ] **Step 4: Commit:** `git add -A && git commit -m "ci: add lint and test workflows"`.
- [ ] **Step 5: Hỏi người dùng xác nhận tạo repo `nv-minh/mq-redis-handbook` (public) và push.** Chỉ sau khi được đồng ý: `gh auth switch --user nv-minh`, `gh repo create nv-minh/mq-redis-handbook --public --source . --remote origin`, `git push -u origin main`. Kiểm tra `gh run watch` xanh. Sau khi xong, chuyển `gh` về account đang active trước đó (`mvn-minhngo-hn`).

---

### Task 8: Chủ đề 05 - RabbitMQ

Áp dụng "Quy ước chung cho task theo chủ đề". Mỗi test tạo exchange và queue có tên `uniqueName`, và xóa trong cleanup.

**Files:**
- Create: `05-rabbitmq/theory.md`, `05-rabbitmq/lab-01-work-queue/`, `05-rabbitmq/lab-02-topic-routing/`, `05-rabbitmq/lab-03-retry-dlx/`

**Interfaces:**
- Produces (`lab-03-retry-dlx`): `setupRetryTopology(ch, name: string, opts: { maxRetries: number; retryDelayMs: number }): Promise<{ work: string; retry: string; dlq: string }>` và `startWorker(ch, queues, handler: (msg: Buffer) => Promise<void>)`.

| Lab | Tên test |
|---|---|
| lab-01-work-queue | `prefetch_1_gives_slow_worker_fewer_messages`, `unacked_message_is_redelivered_after_worker_disconnects` |
| lab-02-topic-routing | `orders_star_created_matches_one_word_only`, `orders_hash_matches_zero_or_more_words`, `unrouted_message_is_dropped_without_mandatory_flag` |
| lab-03-retry-dlx | `failing_message_is_retried_max_retries_times_then_lands_in_dlq`, `dlq_message_carries_x_death_count_and_reason`, `retry_respects_retry_delay` |

---

### Task 9: Chủ đề 06 - Kafka

Áp dụng "Quy ước chung cho task theo chủ đề". TS dùng `@confluentinc/kafka-javascript` (API `KafkaJS`), Go dùng `kafka-go`. Topic tạo bằng `uniqueName` với số partition xác định.

**Files:**
- Create: `06-kafka/theory.md`, `06-kafka/lab-01-partitioning/`, `06-kafka/lab-02-consumer-group/`, `06-kafka/lab-03-offsets-replay/`

**Interfaces:**
- Produces (`lab-01-partitioning`): `produceKeyed(topic: string, key: string, value: string): Promise<{ partition: number; offset: string }>`.
- Produces (`lab-02-consumer-group`): `startGroupMember(topic: string, groupId: string, onAssign: (partitions: number[]) => void, onMessage: (m: { partition: number; value: string }) => void): Promise<{ stop(): Promise<void> }>`.
- Produces (`lab-03-offsets-replay`): `replayFromBeginning(topic: string, groupId: string): Promise<string[]>`.

| Lab | Tên test |
|---|---|
| lab-01-partitioning | `same_key_always_goes_to_same_partition`, `order_is_preserved_within_a_partition`, `null_key_spreads_across_partitions` |
| lab-02-consumer-group | `partitions_are_split_across_group_members`, `rebalance_reassigns_partitions_when_member_leaves`, `members_beyond_partition_count_stay_idle` |
| lab-03-offsets-replay | `new_group_with_earliest_replays_all_messages`, `commit_after_processing_loses_nothing_on_crash`, `commit_before_processing_loses_message_on_crash` |

---

### Task 10: Chủ đề 07 - NATS JetStream

Áp dụng "Quy ước chung cho task theo chủ đề". TS dùng `@nats-io/transport-node` và `@nats-io/jetstream`, Go dùng `nats.go` (`jetstream` package).

**Files:**
- Create: `07-nats-jetstream/theory.md`, `07-nats-jetstream/lab-01-core-queue-group/`, `07-nats-jetstream/lab-02-jetstream-durable/`, `07-nats-jetstream/lab-03-jetstream-replay/`

**Interfaces:**
- Produces (`lab-02-jetstream-durable`): `createStreamAndConsumer(name: string, opts: { ackWaitMs: number; maxDeliver: number }): Promise<Consumer>`.

| Lab | Tên test |
|---|---|
| lab-01-core-queue-group | `queue_group_delivers_each_message_to_one_member`, `core_nats_drops_messages_when_no_subscriber` |
| lab-02-jetstream-durable | `durable_consumer_resumes_after_reconnect`, `unacked_message_is_redelivered_after_ack_wait`, `message_stops_after_max_deliver` |
| lab-03-jetstream-replay | `deliver_all_replays_history`, `deliver_by_start_time_skips_older_messages` |

---

### Task 11: Cổng M2 - push

- [ ] **Step 1: Cập nhật `README.md`** đánh dấu chủ đề 05 đến 07, chạy `make lint && make docs-check && bash scripts/check-mermaid.sh && make test`, kỳ vọng exit 0.
- [ ] **Step 2: Commit** `docs: mark topics 05-07 done`.
- [ ] **Step 3: Hỏi người dùng xác nhận push M2.** Sau khi đồng ý: `gh auth switch --user nv-minh`, `git push`, `gh run watch` phải xanh, rồi chuyển `gh` về account trước đó.

---

### Task 12: Chủ đề 08 - Patterns (phần 1: xử lý lỗi message)

Áp dụng "Quy ước chung cho task theo chủ đề". Các lab này có test chaos (dừng consumer hoặc broker giữa chừng bằng docker CLI), gắn nhãn chaos.

**Files:**
- Create: `08-patterns/theory.md` (viết ở Task 13 sau khi đủ 6 lab), `08-patterns/lab-01-idempotency/`, `08-patterns/lab-02-retry-backoff/`, `08-patterns/lab-03-dlq/`, `08-patterns/lab-04-competing-consumers/`

**Interfaces:**
- Produces (`lab-01-idempotency`): `processOnce(rdb, messageId: string, effect: () => Promise<void>, ttlSeconds: number): Promise<'applied' | 'duplicate'>` (`SET NX EX`; nếu `effect` lỗi thì xóa key để cho phép retry).
- Produces (`lab-02-retry-backoff`): `backoffDelay(attempt: number, opts: { baseMs: number; capMs: number }, rand: () => number): number` (full jitter, hàm thuần).
- Produces (`lab-03-dlq`): `consumeWithDlq(opts: { maxAttempts: number }, handler)` trên Redis Streams, và `replayDlq(limit: number): Promise<number>`.
- Produces (`lab-04-competing-consumers`): `startWorkers(n: number, handler)` trên Redis Streams consumer group.

| Lab | Tên test |
|---|---|
| lab-01-idempotency | `duplicate_delivery_applies_effect_once`, `failed_effect_releases_key_so_retry_can_apply`, `concurrent_duplicates_apply_effect_once` |
| lab-02-retry-backoff | `delay_is_within_zero_and_exponential_bound`, `delay_never_exceeds_cap`, `attempt_zero_uses_base` |
| lab-03-dlq | `poison_message_moves_to_dlq_after_max_attempts_with_error_metadata`, `replay_dlq_returns_messages_to_main_stream`, `chaos_consumer_killed_mid_processing_message_is_reclaimed` |
| lab-04-competing-consumers | `four_workers_process_each_message_exactly_once_without_failures`, `chaos_worker_killed_mid_processing_message_is_processed_by_another` |

---

### Task 13: Chủ đề 08 - Patterns (phần 2: tin cậy giữa DB và broker)

Áp dụng "Quy ước chung cho task theo chủ đề". Dùng Postgres (`DATABASE_URL`) và RabbitMQ. TS dùng `pg`, Go dùng `pgx/v5`. Mỗi test tạo schema riêng bằng `uniqueName`.

**Files:**
- Create: `08-patterns/lab-05-outbox/`, `08-patterns/lab-06-saga/`, `08-patterns/theory.md`

**Interfaces:**
- Produces (`lab-05-outbox`): `placeOrder(pool, order: { id: string; amount: number }): Promise<void>` (ghi `orders` và `outbox` trong một transaction), `runRelay(pool, publish: (e: OutboxEvent) => Promise<void>, opts: { batchSize: number }): Promise<number>` (đánh dấu `published_at` sau khi publish, at-least-once).
- Produces (`lab-06-saga`): `runOrderSaga(steps: SagaStep[]): Promise<'completed' | 'compensated'>` với `SagaStep = { name: string; run(): Promise<void>; compensate(): Promise<void> }`, bù theo thứ tự ngược.

| Lab | Tên test |
|---|---|
| lab-05-outbox | `order_and_outbox_row_commit_atomically`, `rollback_leaves_no_outbox_row`, `relay_publishes_each_row_and_marks_it_published`, `chaos_relay_crash_after_publish_before_mark_republishes_and_consumer_dedupes` |
| lab-06-saga | `all_steps_succeed_returns_completed`, `failure_compensates_completed_steps_in_reverse_order`, `compensation_failure_is_reported_and_remaining_compensations_still_run` |

Review Focus: `compensation_failure_is_reported...` chốt hành vi khi bước bù cũng lỗi. Task này kết thúc bằng bước viết `08-patterns/theory.md` (cả 6 pattern) và `scripts/check-docs.sh 08-patterns` exit 0.

---

### Task 14: Chủ đề 09 - Production (use case 1: Job queue)

Áp dụng "Quy ước chung cho task theo chủ đề". TS dùng `bullmq`, Go dùng `asynq`. Mỗi use case là một mini project có `docker-compose` phụ (nếu cần) và `README.md` với sơ đồ failure scenario.

**Files:**
- Create: `09-production/theory.md` (viết ở Task 17), `09-production/usecase-01-job-queue/{README.md,ts/,go/}`

**Interfaces:**
- Produces: `enqueueEmail(to: string, subject: string): Promise<string>`, worker với `concurrency`, endpoint `GET /metrics` (Prometheus) có counter `jobs_processed_total{status="completed|failed"}`, và hàm `shutdown(timeoutMs: number): Promise<void>` chờ job đang chạy xong.

| Test |
|---|
| `failed_job_is_retried_with_exponential_backoff_up_to_attempts` |
| `job_exceeding_attempts_is_kept_in_failed_set_or_archive` |
| `scheduled_job_runs_not_before_its_delay` |
| `graceful_shutdown_waits_for_in_flight_job_to_finish` |
| `metrics_endpoint_reports_jobs_processed_total` |

---

### Task 15: Chủ đề 09 - Production (use case 2: Order/Payment event-driven)

Áp dụng "Quy ước chung cho task theo chủ đề". Postgres + outbox relay (tái dùng cách làm của lab 08-05, không import chéo, viết lại gọn trong use case) + Kafka + consumer idempotent + metric consumer lag.

**Files:**
- Create: `09-production/usecase-02-order-events/{README.md,ts/,go/}`

**Interfaces:**
- Produces: `POST /orders` trả 201 và ghi order cùng outbox trong một transaction. Relay publish `OrderPlaced` lên topic Kafka. Consumer `inventory` và `payment` idempotent theo `eventId`. Hàm `consumerLag(groupId: string, topic: string): Promise<number>`.

| Test |
|---|
| `post_orders_emits_exactly_one_order_placed_event` |
| `chaos_relay_killed_between_publish_and_mark_still_yields_single_effect_downstream` |
| `duplicate_event_does_not_double_charge` |
| `payment_failure_triggers_inventory_release_via_saga` |
| `consumer_lag_reaches_zero_after_catching_up` |

---

### Task 16: Chủ đề 09 - Production (use case 3 và 4)

Áp dụng "Quy ước chung cho task theo chủ đề".

**Files:**
- Create: `09-production/usecase-03-ratelimit-lock-cache/{README.md,ts/,go/}`, `09-production/usecase-04-realtime/{README.md,ts/,go/}`

**Interfaces:**
- Produces (use case 3): `slidingWindowAllow(rdb, key: string, limit: number, windowMs: number): Promise<boolean>` (Lua trên sorted set); `acquireLock(rdb, key: string, ttlMs: number): Promise<string | null>` và `releaseLock(rdb, key: string, token: string): Promise<boolean>` (khóa một node với token và Lua release, theory nêu rõ tranh luận Redlock); `cacheAside<T>(key: string, ttlSec: number, loader: () => Promise<T>): Promise<T>` có chống stampede (TS: map promise đang chạy; Go: `singleflight`).
- Produces (use case 4): hai instance server WebSocket (TS `ws`, Go `github.com/coder/websocket`) fan-out qua Redis Pub/Sub, `POST /leaderboard/:name` và `GET /leaderboard/top?n=`.

| Use case | Tên test |
|---|---|
| 03 | `allows_limit_requests_then_rejects_within_window`, `window_slides_so_old_requests_stop_counting`, `only_one_of_many_concurrent_callers_gets_the_lock`, `release_with_wrong_token_does_not_release`, `lock_expires_after_ttl`, `hundred_concurrent_cache_misses_call_loader_once` |
| 04 | `client_on_instance_a_receives_message_sent_via_instance_b`, `leaderboard_top_returns_scores_in_descending_order`, `disconnected_client_is_removed_from_room` |

---

### Task 17: Theory chủ đề 09, README hoàn chỉnh và push M3, M4

**Files:**
- Create: `09-production/theory.md` (bảng chọn công nghệ Redis, RabbitMQ, Kafka, NATS theo throughput, ordering, replay, vận hành; sơ đồ kiến trúc tổng và flow cho từng use case)
- Modify: `README.md` (lộ trình đầy đủ, hướng dẫn chạy, bảng phiên bản)

- [ ] **Step 1: Viết `09-production/theory.md`**, chạy `scripts/check-docs.sh 09-production`, kỳ vọng exit 0.
- [ ] **Step 2: Rà toàn repo theo spec mục 1:** `make up && make lint && make docs-check && bash scripts/check-mermaid.sh && make test`, kỳ vọng exit 0. Chạy ngẫu nhiên 3 lab bằng `make lab-ts` và `make lab-go` để xác nhận demo in kết quả.
- [ ] **Step 3: Commit:** `docs: complete roadmap and production theory`.
- [ ] **Step 4: Hỏi người dùng xác nhận push M3 và M4.** Sau khi đồng ý: `gh auth switch --user nv-minh`, `git push`, `gh run watch` xanh, chuyển `gh` về account trước đó.
- [ ] **Step 5: Báo cáo:** liệt kê số lab hoàn thành, kết quả CI, và mọi mục spec chưa đạt (nếu có).

---

## Tự review

- **Phủ spec:** mục 3 (lộ trình) gồm Task 3 đến 6, 8 đến 10, 12 đến 17. Mục 4 (cấu trúc và diagram) gồm Task 1 (check-docs) và các task chủ đề. Mục 5 (tooling) gồm Task 1. Mục 6 (kiểm thử và chaos) gồm Task 6, 12, 13, 15. Mục 7 (CI) gồm Task 7. Mục 8 (research) gồm Task 2. Mục 9 (milestone và push) gồm Task 7, 11, 17. Mục 10 (quy ước viết) gồm Global Constraints và `check-docs.sh`.
- **Nhất quán tên:** `uniqueName`, `eventually`, `waitForPort` (TS) và `UniqueName`, `Eventually`, `WaitForPort` (Go) khớp giữa Task 1 và các task sau. Script `check-docs.sh`, `check-compose.sh`, `check-mermaid.sh` khớp Makefile target `docs-check`.
- **Tỷ lệ:** plan có 17 task cho 30 lab, phần lặp lại được gom vào "Quy ước chung" thay vì chép lại mỗi task.
