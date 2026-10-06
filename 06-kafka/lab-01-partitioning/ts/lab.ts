import { KafkaJS } from "@confluentinc/kafka-javascript";
import { eventually } from "@handbook/testkit";

/** Danh sách broker. Lab đọc `KAFKA_BROKERS` (phân tách bằng dấu phẩy), mặc định là Kafka của `make up`. */
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
async function withAdmin<T>(fn: (admin: KafkaJS.Admin) => Promise<T>): Promise<T> {
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
 * metadata báo đủ partition và mọi partition đã có leader. Wrapper chưa hỗ trợ `waitForLeaders`
 * nên lab tự chờ, nếu không produce ngay sau khi tạo có thể gặp lỗi leader chưa sẵn sàng.
 * Không để broker tự tạo topic: topic tự tạo chỉ có 1 partition.
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
          // Group chưa từng tồn tại (consumer chưa kịp join, hoặc group rỗng không có offset đã tự biến mất) thì không có gì để xóa.
          // Lỗi của deleteGroups là KafkaJSDeleteGroupsError, mã lỗi nằm trong từng phần tử của `groups`.
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

export interface ProduceResult {
  partition: number;
  /** Offset của message trong partition, dạng string vì offset là số nguyên 64 bit. */
  offset: string;
}

let sharedProducer: KafkaJS.Producer | undefined;

/** Producer dùng chung của lab, connect ở lần dùng đầu. `idempotent` giữ thứ tự khi producer phải retry. */
async function producer(): Promise<KafkaJS.Producer> {
  if (sharedProducer === undefined) {
    const p = newKafka().producer({
      kafkaJS: { acks: -1, idempotent: true, allowAutoTopicCreation: false },
    });
    await p.connect();
    sharedProducer = p;
  }
  return sharedProducer;
}

/** Ngắt producer dùng chung. Gọi trong teardown, an toàn khi gọi nhiều lần. */
export async function closeProducer(): Promise<void> {
  const p = sharedProducer;
  sharedProducer = undefined;
  await p?.disconnect().catch(() => undefined);
}

async function send(topic: string, key: string | null, value: string): Promise<ProduceResult> {
  const p = await producer();
  const [metadata] = await p.send({ topic, messages: [{ key, value }] });
  // Ack của wrapper dùng `baseOffset` (không phải `offset`); gửi một message nên baseOffset chính là offset của nó.
  if (metadata === undefined || metadata.baseOffset === undefined) {
    throw new Error("broker không trả partition và offset trong ack");
  }
  return { partition: metadata.partition, offset: metadata.baseOffset };
}

/**
 * Produce một message có key và chờ ack của broker. Partition và offset lấy từ ack.
 * Wrapper KafkaJS của kafka-javascript đặt partitioner `murmur2_random`: cùng key luôn vào cùng
 * partition (miễn số partition không đổi), và khớp với producer Java.
 */
export function produceKeyed(topic: string, key: string, value: string): Promise<ProduceResult> {
  return send(topic, key, value);
}

/** Produce một message có key null. Partitioner dùng sticky partitioning cho key null nên không bảo đảm phân phối đều. */
export function produceUnkeyed(topic: string, value: string): Promise<ProduceResult> {
  return send(topic, null, value);
}

export interface ConsumedRecord {
  partition: number;
  offset: string;
  key: string | null;
  value: string;
}

/**
 * Đọc mọi message đang có trong topic từ đầu bằng một consumer group mới (group id ngẫu nhiên, `fromBeginning`).
 * Điều kiện dừng không dùng sleep: so sánh số message đã nhận với tổng (high - low) watermark của các partition.
 * Consumer không commit offset và group bị xóa khi xong.
 */
export async function readTopic(topic: string): Promise<ConsumedRecord[]> {
  // Ngay sau khi tạo topic, fetchTopicOffsets có thể báo lỗi leader tạm thời dù metadata đã có leader: eventually thử lại.
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

  const groupId = `lab01-reader-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
  const consumer = newKafka().consumer({
    kafkaJS: { groupId, fromBeginning: true, autoCommit: false, allowAutoTopicCreation: false },
  });
  const records: ConsumedRecord[] = [];
  await consumer.connect();
  try {
    await consumer.subscribe({ topics: [topic] });
    // run() trả về ngay, việc đọc chạy nền. Promise lỗi (nếu có) được bắt để không thành unhandled rejection khi teardown.
    const running = consumer
      .run({
        eachMessage: async ({ partition, message }) => {
          records.push({
            partition,
            offset: message.offset,
            key: message.key?.toString() ?? null,
            value: message.value?.toString() ?? "",
          });
        },
      })
      .catch(() => undefined);
    await eventually(async () => (records.length >= expected ? true : undefined), {
      timeoutMs: 45_000,
    });
    await running;
  } finally {
    await consumer.disconnect().catch(() => undefined);
    await deleteGroup(groupId);
  }
  return records;
}
