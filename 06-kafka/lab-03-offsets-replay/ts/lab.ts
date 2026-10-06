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
 * metadata báo đủ partition và mọi partition đã có leader. Wrapper chưa hỗ trợ `waitForLeaders` nên lab tự chờ.
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

let sharedProducer: KafkaJS.Producer | undefined;

async function producer(): Promise<KafkaJS.Producer> {
  if (sharedProducer === undefined) {
    const p = newKafka().producer({ kafkaJS: { acks: -1, idempotent: true } });
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

/** Produce một message có key và chờ ack của broker. Cùng key thì cùng partition nên giữ thứ tự. */
export async function produce(topic: string, key: string, value: string): Promise<void> {
  const p = await producer();
  await p.send({ topic, messages: [{ key, value }] });
}

/**
 * Tạo một consumer của group `groupId`, đọc từ đầu khi group chưa có offset đã commit (`fromBeginning`, tức
 * `auto.offset.reset=earliest`) và TẮT auto commit: offset chỉ được lưu khi code gọi `commitOffsets`.
 */
function newConsumer(groupId: string): KafkaJS.Consumer {
  return newKafka().consumer({ kafkaJS: { groupId, fromBeginning: true, autoCommit: false } });
}

/** Đọc `message` thành chuỗi value. */
function valueOf(message: { value: Buffer | null }): string {
  return message.value?.toString() ?? "";
}

/**
 * Đọc lại toàn bộ log của topic bằng một group mới, rồi trả về các value đã đọc (thứ tự giữa các partition không xác định).
 *
 * Group mới chưa có offset đã commit nên `auto.offset.reset=earliest` đưa consumer về đầu mỗi partition.
 * Hàm không commit gì, nên chạy lại với cùng group id vẫn đọc lại từ đầu; test vẫn dùng group id mới cho mỗi lần.
 *
 * Điều kiện dừng không dùng sleep: tổng (high watermark - low watermark) của các partition lúc bắt đầu là số message
 * phải đọc. Topic rỗng cho 0 nên trả về mảng rỗng ngay mà không cần join group.
 */
export async function replayFromBeginning(topic: string, groupId: string): Promise<string[]> {
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
  return collect(topic, groupId, (values) => values.length >= expected);
}

/**
 * Consumer mới trong group đã có offset đã commit: đọc tiếp từ offset đó và trả về mọi value đã nhận
 * cho tới khi gặp `lastValue` (tính cả nó). Không commit gì.
 * Dùng để quan sát consumer thay thế sau một lần crash bắt đầu từ đâu.
 */
export function receiveUntil(topic: string, groupId: string, lastValue: string): Promise<string[]> {
  return collect(topic, groupId, (values) => values.at(-1) === lastValue);
}

/** Chạy consumer của `groupId` tới khi `done(values)` đúng (hoặc hết 45 giây), rồi luôn disconnect và xóa kết nối. */
async function collect(
  topic: string,
  groupId: string,
  done: (values: string[]) => boolean,
): Promise<string[]> {
  const consumer = newConsumer(groupId);
  const values: string[] = [];
  await consumer.connect();
  try {
    await consumer.subscribe({ topics: [topic] });
    // run() trả về ngay, việc đọc chạy nền. Bắt lỗi để không thành unhandled rejection khi disconnect.
    void consumer
      .run({
        eachMessage: async ({ message }) => {
          if (!done(values)) values.push(valueOf(message));
        },
      })
      .catch(() => undefined);
    await eventually(async () => (done(values) ? true : undefined), { timeoutMs: 45_000 });
  } finally {
    await consumer.disconnect().catch(() => undefined);
  }
  return values;
}

/** Thứ tự hai bước "xử lý" và "commit offset" của consumer. */
export type CommitOrder = "process-then-commit" | "commit-then-process";

export interface CrashedAt {
  partition: number;
  offset: string;
  value: string;
}

/**
 * Mô phỏng một consumer nhận một message rồi crash giữa hai bước xử lý và commit.
 *
 * - `process-then-commit` (at-least-once): gọi `process(value)`, rồi crash TRƯỚC khi commit.
 * - `commit-then-process` (at-most-once): commit offset + 1 của message, rồi crash TRƯỚC khi gọi `process`.
 *
 * Auto commit tắt. Message đầu tiên nhận được là message bị "crash"; các message sau (nếu kịp tới) bị bỏ qua, không xử lý và không commit.
 * Crash được mô phỏng bằng `disconnect()` ngay sau điểm crash, không commit thêm gì. Khác với kill -9 thật ở chỗ
 * disconnect gửi LeaveGroup nên consumer thay thế vào group ngay, còn kill thật phải chờ `session.timeout.ms`.
 * Offset mà group lưu giống hệt nhau trong cả hai trường hợp, và đó là thứ test cần kiểm chứng.
 * Commit offset là offset của message cộng 1, vì Kafka lưu "offset kế tiếp cần đọc".
 */
export async function consumeOneThenCrash(
  topic: string,
  groupId: string,
  order: CommitOrder,
  process: (value: string) => void,
): Promise<CrashedAt> {
  const consumer = newConsumer(groupId);
  await consumer.connect();
  try {
    await consumer.subscribe({ topics: [topic] });
    let crashed = false;
    const crashPoint = new Promise<CrashedAt>((resolve, reject) => {
      void consumer
        .run({
          eachMessage: async ({ partition, message }) => {
            if (crashed) return;
            crashed = true;
            try {
              const value = valueOf(message);
              if (order === "process-then-commit") {
                process(value);
              } else {
                await consumer.commitOffsets([
                  { topic, partition, offset: (BigInt(message.offset) + 1n).toString() },
                ]);
              }
              resolve({ partition, offset: message.offset, value });
            } catch (error) {
              reject(error);
            }
          },
        })
        .catch(reject);
    });
    // Giới hạn thời gian chờ để lab không treo nếu consumer không bao giờ nhận được message.
    let timer: NodeJS.Timeout | undefined;
    const timeout = new Promise<never>((_, reject) => {
      timer = setTimeout(
        () => reject(new Error("consumer không nhận được message trong 45 giây")),
        45_000,
      );
    });
    try {
      return await Promise.race([crashPoint, timeout]);
    } finally {
      clearTimeout(timer);
    }
  } finally {
    await consumer.disconnect().catch(() => undefined);
  }
}
