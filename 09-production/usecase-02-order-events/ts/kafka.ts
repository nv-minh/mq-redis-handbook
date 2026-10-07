import { KafkaJS } from "@confluentinc/kafka-javascript";
import { eventually, uniqueName } from "@handbook/testkit";
import { parseEvent, type DomainEvent, type HandleResult } from "./events.js";

/** Danh sách broker. Use case đọc `KAFKA_BROKERS` (phân tách bằng dấu phẩy), mặc định là Kafka của `make up`. */
export function brokers(): string[] {
  return (process.env.KAFKA_BROKERS ?? "127.0.0.1:9092").split(",");
}

/** Tạo client Kafka (API KafkaJS của @confluentinc/kafka-javascript). Log của thư viện chỉ giữ mức ERROR. */
function newKafka(): KafkaJS.Kafka {
  return new KafkaJS.Kafka({
    kafkaJS: { brokers: brokers(), logLevel: KafkaJS.logLevel.ERROR },
  });
}

/** Chạy `fn` với một admin client đã connect, và luôn disconnect khi xong. */
export async function withAdmin<T>(fn: (admin: KafkaJS.Admin) => Promise<T>): Promise<T> {
  const admin = newKafka().admin();
  await admin.connect();
  try {
    return await fn(admin);
  } finally {
    await admin.disconnect().catch(() => undefined);
  }
}

/**
 * Tạo topic với số partition xác định và replication factor 1 (broker chỉ có một node), rồi chờ tới khi
 * metadata báo đủ partition và mọi partition đã có leader.
 */
export async function createTopic(topic: string, partitions: number): Promise<void> {
  await withAdmin(async (admin) => {
    await admin.createTopics({
      topics: [{ topic, numPartitions: partitions, replicationFactor: 1 }],
    });
    await eventually(
      async () => {
        const [metadata] = await admin.fetchTopicMetadata({ topics: [topic] });
        const ready =
          metadata !== undefined &&
          metadata.partitions.length === partitions &&
          metadata.partitions.every((p) => p.leader >= 0);
        return ready || undefined;
      },
      { timeoutMs: 20_000 },
    );
  });
}

/** Xóa topic. Topic không tồn tại không phải lỗi, để dọn dẹp luôn chạy được dù topic chưa kịp tạo. */
export async function deleteTopic(topic: string): Promise<void> {
  await withAdmin(async (admin) => {
    try {
      await admin.deleteTopics({ topics: [topic] });
    } catch (error) {
      if (
        error instanceof KafkaJS.KafkaJSError &&
        error.code === KafkaJS.ErrorCodes.ERR_UNKNOWN_TOPIC_OR_PART
      ) {
        return;
      }
      throw error;
    }
  });
}

/** Xóa consumer group (và offset đã commit của nó). Group phải rỗng nên thử lại tới khi member cuối đã rời group. */
export async function deleteGroup(groupId: string): Promise<void> {
  await withAdmin(async (admin) => {
    await eventually(
      async () => {
        try {
          await admin.deleteGroups([groupId]);
          return true;
        } catch (error) {
          // Group chưa từng tồn tại (chưa join, hoặc group rỗng không có offset đã tự biến mất) thì không có gì để xóa.
          if (
            error instanceof KafkaJS.KafkaJSDeleteGroupsError &&
            error.groups.every((g) => g.errorCode === KafkaJS.ErrorCodes.ERR_GROUP_ID_NOT_FOUND)
          ) {
            return true;
          }
          throw error;
        }
      },
      { timeoutMs: 20_000 },
    );
  });
}

/**
 * Lag của một group trên một topic: tổng theo partition của (log end offset trừ offset đã commit của group).
 *
 * - Log end offset là `high` của `fetchTopicOffsets` (offset của message KẾ TIẾP sẽ được ghi).
 * - Offset đã commit là `fetchOffsets` của admin: Kafka lưu "offset kế tiếp cần đọc", nên hiệu số là số message chưa xử lý xong.
 * - Partition chưa có offset đã commit (fetchOffsets trả offset âm) tính từ offset đầu `low`, vì consumer mới
 *   của group đọc từ `earliest`. Offset đã commit nhỏ hơn `low` (retention đã xóa đoạn đầu) cũng tính từ `low`.
 *
 * Message đang được consumer xử lý mà chưa commit vẫn tính là lag: lag là "việc chưa xong", không phải "việc chưa nhận".
 */
export async function consumerLag(groupId: string, topic: string): Promise<number> {
  return withAdmin((admin) => lagWith(admin, groupId, topic));
}

/** Tính lag bằng một admin client đã connect (dùng chung một admin cho nhiều group trong một lần scrape). */
export async function lagWith(
  admin: KafkaJS.Admin,
  groupId: string,
  topic: string,
): Promise<number> {
  // Ngay sau khi tạo topic, fetchTopicOffsets có thể báo lỗi leader tạm thời dù metadata đã có leader: eventually thử lại.
  const ends = await eventually(async () => admin.fetchTopicOffsets(topic), { timeoutMs: 10_000 });
  const [group] = await admin.fetchOffsets({
    groupId,
    topics: [{ topic, partitions: ends.map((end) => end.partition) }],
  });
  const committed = new Map<number, number>();
  for (const p of group?.partitions ?? []) committed.set(p.partition, Number(p.offset));
  let lag = 0;
  for (const end of ends) {
    const low = Number(end.low);
    const offset = committed.get(end.partition) ?? -1;
    lag += Math.max(0, Number(end.high) - (offset >= low ? offset : low));
  }
  return lag;
}

/** Producer acks=all (`acks: -1`) và idempotent. `allowAutoTopicCreation: false` để topic đã xóa không bị tạo lại. */
export async function createProducer(): Promise<KafkaJS.Producer> {
  const producer = newKafka().producer({
    kafkaJS: { acks: -1, idempotent: true, allowAutoTopicCreation: false },
  });
  await producer.connect();
  return producer;
}

/** Gửi một message có key và chờ ack của broker. Cùng key thì cùng partition nên giữ thứ tự. */
export async function sendRaw(
  producer: KafkaJS.Producer,
  topic: string,
  key: string,
  value: string,
): Promise<void> {
  await producer.send({ topic, messages: [{ key, value }] });
}

/** Một message đọc từ topic. `event` là undefined với poison event. */
export interface TopicRecord {
  partition: number;
  offset: string;
  key: string | null;
  value: string;
  event: DomainEvent | undefined;
}

/**
 * Đọc lại toàn bộ log của topic bằng một group mới (không commit gì) rồi xóa group đó.
 * Điều kiện dừng không dùng sleep: tổng (high trừ low) của các partition lúc bắt đầu là số message phải đọc.
 */
export async function readTopic(topic: string): Promise<TopicRecord[]> {
  const expected = await withAdmin((admin) =>
    eventually(
      async () => {
        const offsets = await admin.fetchTopicOffsets(topic);
        return offsets.reduce((sum, o) => sum + (Number(o.high) - Number(o.low)), 0);
      },
      { timeoutMs: 20_000 },
    ),
  );
  if (expected === 0) return [];
  const groupId = uniqueName("order-events-reader");
  const consumer = newKafka().consumer({
    kafkaJS: { groupId, fromBeginning: true, autoCommit: false, allowAutoTopicCreation: false },
  });
  const records: TopicRecord[] = [];
  // Cờ này tắt trước khi disconnect: lỗi do chính việc ngắt consumer gây ra không được log như lỗi thật.
  let reading = true;
  await consumer.connect();
  try {
    await consumer.subscribe({ topics: [topic] });
    // run() trả về ngay, việc đọc chạy nền. Bắt lỗi để không thành unhandled rejection, và log lỗi thật (lỗi do disconnect có chủ đích thì bỏ qua).
    void consumer
      .run({
        eachMessage: async ({ partition, message }) => {
          const value = message.value?.toString() ?? "";
          records.push({
            partition,
            offset: message.offset,
            key: message.key?.toString() ?? null,
            value,
            event: parseEvent(value),
          });
        },
      })
      .catch((error: unknown) => {
        if (!reading) return;
        console.error("[order-events] readTopic: consumer.run() lỗi:", error);
      });
    await eventually(async () => (records.length >= expected ? true : undefined), {
      timeoutMs: 30_000,
    });
  } finally {
    reading = false;
    await consumer.disconnect().catch(() => undefined);
    await deleteGroup(groupId);
  }
  return records;
}

/** Cách dựng hàm xử lý và theo dõi kết quả của một consumer. */
export interface ConsumerSpec {
  topic: string;
  groupId: string;
  /** Xử lý một event hợp lệ. Ném lỗi nghĩa là chưa xử lý xong: không commit offset, Kafka giao lại message. */
  handle: (event: DomainEvent) => Promise<HandleResult>;
  /** Báo kết quả của từng message (kể cả `poison`). */
  onResult?: (result: HandleResult | "poison") => void;
  /**
   * Chỉ để mô phỏng lỗi: trả true thì consumer "crash" ngay sau khi effect đã commit và TRƯỚC khi commit offset,
   * tức là dừng xử lý và không commit gì (message sẽ được giao lại cho consumer kế tiếp của group).
   */
  crashBeforeCommit?: (event: DomainEvent) => boolean;
}

export interface RunningConsumer {
  /** Rời group và đóng consumer. Gọi nhiều lần cũng được. */
  stop(): Promise<void>;
  /** True khi `crashBeforeCommit` đã kích hoạt. */
  crashed(): boolean;
}

/** Chờ ngắn trước khi giao lại message lỗi, để lỗi tạm thời (database chưa sẵn sàng) không thành vòng lặp nóng. */
const RETRY_BACKOFF_MS = 200;

/**
 * Chạy một consumer của group `groupId` với auto commit TẮT: offset chỉ được commit SAU KHI `handle` thành công
 * (và transaction của effect đã commit), nên crash giữa hai bước cho một bản trùng chứ không bao giờ làm mất event.
 *
 * Poison event (không parse được) được log, đếm qua `onResult("poison")` rồi commit offset để bỏ qua:
 * một message hỏng không chặn partition mãi mãi. Nội dung hỏng chỉ nằm trong log, use case này không có DLQ topic.
 * Nếu `handle` ném lỗi, hàm chờ một chút rồi ném lại: wrapper tự seek về message đó và giao lại.
 */
export async function startConsumer(spec: ConsumerSpec): Promise<RunningConsumer> {
  const consumer = newKafka().consumer({
    kafkaJS: {
      groupId: spec.groupId,
      fromBeginning: true,
      autoCommit: false,
      allowAutoTopicCreation: false,
    },
  });
  let stopping = false;
  let crashedFlag = false;
  let stopped: Promise<void> | undefined;
  const running: RunningConsumer = {
    stop: () => {
      stopping = true;
      stopped ??= consumer.disconnect().catch(() => undefined);
      return stopped;
    },
    crashed: () => crashedFlag,
  };
  const commit = async (partition: number, offset: string): Promise<void> => {
    try {
      await consumer.commitOffsets([
        { topic: spec.topic, partition, offset: (BigInt(offset) + 1n).toString() },
      ]);
    } catch (error) {
      // Lỗi commit trong lúc đang dừng là bình thường (consumer đã disconnect), message sẽ được giao lại.
      if (!stopping) throw error;
    }
  };
  await consumer.connect();
  try {
    await consumer.subscribe({ topics: [spec.topic] });
    // run() trả về ngay, việc đọc chạy nền. Bắt lỗi để không thành unhandled rejection, và log lỗi thật (lỗi do stop() có chủ đích thì bỏ qua).
    void consumer
      .run({
        eachMessage: async ({ partition, message }) => {
          if (crashedFlag || stopping) return;
          const value = message.value?.toString() ?? null;
          const event = parseEvent(value);
          if (event === undefined) {
            console.error(
              `[order-events] bỏ qua poison event ở partition ${partition}, offset ${message.offset}: ${String(value).slice(0, 80)}`,
            );
            spec.onResult?.("poison");
            await commit(partition, message.offset);
            return;
          }
          let result: HandleResult;
          try {
            result = await spec.handle(event);
          } catch (error) {
            console.error(
              `[order-events] lỗi khi xử lý event ${event.eventId}, sẽ thử lại:`,
              error,
            );
            await new Promise<void>((resolve) => setTimeout(resolve, RETRY_BACKOFF_MS));
            throw error;
          }
          spec.onResult?.(result);
          if (spec.crashBeforeCommit?.(event)) {
            crashedFlag = true;
            return;
          }
          await commit(partition, message.offset);
        },
      })
      .catch((error: unknown) => {
        if (!stopping) console.error(`[order-events] consumer ${spec.groupId}: run() lỗi:`, error);
      });
  } catch (error) {
    await running.stop();
    throw error;
  }
  return running;
}
