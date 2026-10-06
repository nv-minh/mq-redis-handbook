// Demo: durable consumer resume sau khi mất kết nối, redelivery khi hết AckWait, và message bị bỏ khi vượt MaxDeliver.
import { eventually, uniqueName } from "@handbook/testkit";
import {
  bindConsumer,
  consumerInfo,
  createStreamAndConsumer,
  disconnect,
  fetchMessages,
  publishMessages,
  teardown,
  watchMaxDeliveries,
} from "./lab.js";

const names: string[] = [];
function newName(prefix: string): string {
  const name = uniqueName(prefix);
  names.push(name);
  return name;
}

async function resumeDemo(): Promise<void> {
  console.log("--- durable consumer: mất kết nối giữa chừng ---");
  const name = newName("demo-resume");
  const consumer = await createStreamAndConsumer(name, { ackWaitMs: 5_000, maxDeliver: 5 });
  await publishMessages(
    name,
    Array.from({ length: 6 }, (_, i) => `job-${i}`),
  );
  const first = await fetchMessages(consumer, 3, 5_000);
  for (const m of first) await m.ackAck();
  console.log(`Worker 1 nhận và ack: ${first.map((m) => m.string()).join(", ")}`);
  await disconnect(name);
  console.log("Worker 1 mất kết nối (connection bị đóng đột ngột).");
  const rest = await fetchMessages(await bindConsumer(name), 3, 5_000);
  for (const m of rest) await m.ackAck();
  console.log(
    `Worker 2 bind lại cùng durable, chỉ nhận phần còn lại: ${rest.map((m) => m.string()).join(", ")}`,
  );
  console.log(
    "Kết luận: vị trí đọc nằm trên server, nên message đã ack không bị giao lại và message chưa đọc không bị mất.",
  );
}

async function ackWaitDemo(): Promise<void> {
  console.log("--- AckWait: worker nhận message nhưng không ack ---");
  const ackWaitMs = 1_000;
  const name = newName("demo-ackwait");
  const consumer = await createStreamAndConsumer(name, { ackWaitMs, maxDeliver: 5 });
  await publishMessages(name, ["job-1"]);
  const [first] = await fetchMessages(consumer, 1, 5_000);
  const receivedAt = performance.now();
  console.log(
    `Lần giao 1: ${first?.string()} (delivery count ${first?.info.deliveryCount}), worker không ack`,
  );
  const second = await eventually(
    async () => (await fetchMessages(consumer, 1, 1_000))[0] ?? undefined,
    {
      timeoutMs: 20_000,
      intervalMs: 50,
    },
  );
  console.log(
    `Lần giao 2: ${second.string()} (delivery count ${second.info.deliveryCount}, redelivered=${second.redelivered}) sau ${Math.round(performance.now() - receivedAt)} ms, AckWait là ${ackWaitMs} ms`,
  );
  await second.ackAck();
  console.log(
    "Kết luận: hết AckWait mà chưa ack thì server coi worker đã chết và giao lại, nên xử lý phải idempotent.",
  );
}

async function maxDeliverDemo(): Promise<void> {
  console.log("--- MaxDeliver = 3: message độc không bao giờ được ack ---");
  const name = newName("demo-maxdeliver");
  const consumer = await createStreamAndConsumer(name, { ackWaitMs: 500, maxDeliver: 3 });
  const watch = await watchMaxDeliveries(name);
  await publishMessages(name, ["poison"]);
  await eventually(
    async () => {
      for (const m of await fetchMessages(consumer, 1, 1_000))
        console.log(`Lần giao ${m.info.deliveryCount}: ${m.string()}, worker không ack`);
      return watch.advisories.length > 0 || undefined;
    },
    { timeoutMs: 30_000, intervalMs: 20 },
  );
  const advisory = watch.advisories[0];
  console.log(
    `Advisory MAX_DELIVERIES: stream_seq=${advisory?.stream_seq}, deliveries=${advisory?.deliveries}`,
  );
  const extra = await fetchMessages(consumer, 1, 1_000);
  console.log(`Fetch thêm một lần: nhận ${extra.length} message`);
  const info = await consumerInfo(name);
  console.log(
    `Thông tin consumer: num_pending=${info.num_pending}, num_ack_pending=${info.num_ack_pending}, num_redelivered=${info.num_redelivered}, ` +
      `delivered.stream_seq=${info.delivered.stream_seq}, ack_floor.stream_seq=${info.ack_floor.stream_seq}`,
  );
  console.log(
    "Kết luận: sau MaxDeliver server ngừng giao và không có dead-letter queue, advisory là dấu vết duy nhất (message vẫn nằm trong stream).",
  );
}

try {
  await resumeDemo();
  await ackWaitDemo();
  await maxDeliverDemo();
} finally {
  await Promise.allSettled(names.map((name) => teardown(name)));
}
