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
          // Group chưa từng tồn tại (consumer chưa kịp join) thì không có gì để xóa.
          if (
            error instanceof KafkaJS.KafkaJSError &&
            error.code === KafkaJS.ErrorCodes.ERR_GROUP_ID_NOT_FOUND
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

/** Produce một message thẳng vào partition chỉ định (bỏ qua partitioner) và chờ ack của broker. */
export async function produceToPartition(
  topic: string,
  partition: number,
  value: string,
): Promise<void> {
  const p = await producer();
  await p.send({ topic, messages: [{ partition, value }] });
}

export interface GroupMember {
  /** Rời group một cách có chủ đích (LeaveGroup) rồi đóng consumer. Gọi nhiều lần cũng được. */
  stop(): Promise<void>;
}

/**
 * Chạy một member của consumer group.
 *
 * - `onAssign(partitions)` được gọi mỗi lần member được gán partition, với TOÀN BỘ assignment mới
 *   (có thể là mảng rỗng khi member thừa). Lab dùng assignor mặc định của kafka-javascript là roundRobin (eager),
 *   nên mỗi rebalance kết thúc bằng một lần onAssign với assignment đầy đủ. Test chỉ nên assert trên assignment cuối cùng.
 * - `onMessage` nhận từng message đọc được. Group dùng `fromBeginning` và auto commit mặc định của wrapper.
 * - Hàm resolve khi consumer đã chạy, chưa chắc đã được gán partition: hãy chờ onAssign.
 *
 * `stop()` dùng `disconnect()` vì wrapper chưa cài `consumer.stop()`. Disconnect gửi LeaveGroup, nên group rebalance ngay.
 * Consumer chết đột ngột (kill -9, mất mạng) không gửi LeaveGroup, và coordinator chỉ phát hiện sau tối đa session.timeout.ms.
 */
export async function startGroupMember(
  topic: string,
  groupId: string,
  onAssign: (partitions: number[]) => void,
  onMessage: (m: { partition: number; value: string }) => void,
): Promise<GroupMember> {
  const consumer = newKafka().consumer({
    // rebalance_cb nằm ngoài khối kafkaJS (cấu hình librdkafka); wrapper vẫn tự assign/unassign sau khi gọi nó.
    rebalance_cb: (err: { code: number }, assignment: { topic: string; partition: number }[]) => {
      if (err.code === KafkaJS.ErrorCodes.ERR__ASSIGN_PARTITIONS) {
        onAssign(assignment.filter((a) => a.topic === topic).map((a) => a.partition));
      }
    },
    kafkaJS: { groupId, fromBeginning: true },
  });
  await consumer.connect();
  let stopped: Promise<void> | undefined;
  const member: GroupMember = {
    stop: () => {
      stopped ??= consumer.disconnect().catch(() => undefined);
      return stopped;
    },
  };
  try {
    await consumer.subscribe({ topics: [topic] });
    // run() trả về ngay, việc đọc chạy nền. Bắt lỗi để không thành unhandled rejection khi teardown đã ngắt consumer.
    void consumer
      .run({
        eachMessage: async ({ partition, message }) => {
          onMessage({ partition, value: message.value?.toString() ?? "" });
        },
      })
      .catch(() => undefined);
  } catch (error) {
    await member.stop();
    throw error;
  }
  return member;
}
