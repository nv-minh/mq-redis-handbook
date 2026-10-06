// Demo: group mới đọc lại toàn bộ log, rồi hai kịch bản crash cho thấy thứ tự "xử lý" và "commit" quyết định
// message được xử lý lại (at-least-once) hay mất (at-most-once).
import { uniqueName } from "@handbook/testkit";
import {
  closeProducer,
  consumeOneThenCrash,
  createTopic,
  deleteGroup,
  deleteTopic,
  produce,
  receiveUntil,
  replayFromBeginning,
  type CommitOrder,
} from "./lab.js";

const topics: string[] = [];
const groups: string[] = [];

/** Tạo topic 3 partition mới. Mỗi kịch bản dùng topic riêng để message của kịch bản khác không lẫn vào. */
async function newTopic(): Promise<string> {
  const topic = uniqueName("demo-offsets");
  topics.push(topic);
  await createTopic(topic, 3);
  return topic;
}

function newGroup(prefix: string): string {
  const group = uniqueName(prefix);
  groups.push(group);
  return group;
}

/** Chạy một kịch bản crash: M1 tới, consumer crash giữa xử lý và commit, M2 tới, consumer thay thế đọc tiếp. */
async function crashScenario(order: CommitOrder, key: string): Promise<void> {
  const topic = await newTopic();
  const groupId = newGroup("demo-crash");
  await produce(topic, key, `${key}-M1`);
  const processed: string[] = [];
  await consumeOneThenCrash(topic, groupId, order, (value) => processed.push(value));
  console.log(`consumer 1 đã xử lý: [${processed.join(", ")}] rồi crash`);
  await produce(topic, key, `${key}-M2`);
  const seen = await receiveUntil(topic, groupId, `${key}-M2`);
  console.log(`consumer 2 (cùng group) nhận: [${seen.join(", ")}]`);
}

try {
  const topic = await newTopic();
  console.log(`topic ${topic} có 3 partition`);

  console.log("--- replay: group mới đọc lại toàn bộ log ---");
  for (let i = 0; i < 6; i++) await produce(topic, `key-${i}`, `event-${i}`);
  for (const name of ["group A", "group B"]) {
    const values = await replayFromBeginning(topic, newGroup("demo-replay"));
    console.log(
      `${name} (mới, earliest) đọc được ${values.length} message: ${[...values].sort().join(", ")}`,
    );
  }
  console.log(
    "Kết luận: Kafka không xóa message sau khi đọc, nên mỗi group mới tự đọc lại từ đầu log (trong thời gian retention).",
  );

  console.log("--- xử lý xong rồi mới commit, crash trước khi commit (at-least-once) ---");
  await crashScenario("process-then-commit", "a");
  console.log(
    "Kết luận: offset chưa commit nên consumer thay thế đọc lại M1, M1 được xử lý lần hai. Không mất message, nhưng consumer phải idempotent.",
  );

  console.log("--- commit trước rồi mới xử lý, crash trước khi xử lý (at-most-once) ---");
  await crashScenario("commit-then-process", "b");
  console.log(
    "Kết luận: offset đã đi qua M1 nên consumer thay thế bắt đầu từ M2, M1 mất mà không ai biết.",
  );
} finally {
  await closeProducer();
  for (const group of groups) await deleteGroup(group);
  for (const topic of topics) await deleteTopic(topic);
}
