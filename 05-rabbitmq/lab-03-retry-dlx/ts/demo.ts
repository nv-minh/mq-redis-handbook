// Demo: ba message đi qua cùng một worker. "ok" xử lý thành công ngay, "flaky" lỗi hai lần rồi thành công,
// "poison" luôn lỗi nên đi hết vòng retry (reject -> queue retry chờ TTL -> work) rồi rơi vào DLQ.
// Cuối cùng in x-death của message trong DLQ.
import { eventually, uniqueName } from "@handbook/testkit";
import { openConnection, publishWork, setupRetryTopology, startWorker } from "./lab.js";

const name = uniqueName("demo-retry");
const maxRetries = 3;
const retryDelayMs = 500;
const connection = await openConnection();

try {
  const channel = await connection.createConfirmChannel();
  const queues = await setupRetryTopology(channel, name, { maxRetries, retryDelayMs });
  console.log(`topology: work=${queues.work}`);
  console.log(
    `          retry=${queues.retry} (TTL ${retryDelayMs} ms, dead-letter ngược về work)`,
  );
  console.log(`          dlq=${queues.dlq}, tối đa ${maxRetries} lần retry`);

  const started = performance.now();
  const attempts = new Map<string, number>();
  const log = (message: string) =>
    console.log(
      `[+${Math.round(performance.now() - started)
        .toString()
        .padStart(5)} ms] ${message}`,
    );

  await startWorker(channel, queues, async (msg) => {
    const body = msg.toString();
    const attempt = (attempts.get(body) ?? 0) + 1;
    attempts.set(body, attempt);
    const fails = body === "poison" || (body === "flaky" && attempt <= 2);
    log(`${body}: lần xử lý ${attempt} ${fails ? "LỖI" : "thành công"}`);
    if (fails) throw new Error(`${body} lỗi ở lần ${attempt}`);
  });

  for (const body of ["ok", "flaky", "poison"]) await publishWork(channel, queues, body);

  const check = await connection.createChannel();
  await eventually(
    async () => (await check.checkQueue(queues.dlq)).messageCount === 1 || undefined,
    {
      timeoutMs: 20_000,
    },
  );
  const dead = await check.get(queues.dlq, { noAck: true });
  if (dead === false) throw new Error("DLQ không có message");
  const headers = dead.properties.headers ?? {};
  log(`DLQ nhận ${dead.content.toString()}`);
  console.log(`x-failure-reason: ${headers["x-failure-reason"]}`);
  console.log(
    `x-attempts: ${headers["x-attempts"]} lần xử lý (1 lần đầu + ${maxRetries} lần retry)`,
  );
  for (const death of headers["x-death"] ?? []) {
    console.log(`x-death: queue=${death.queue} reason=${death.reason} count=${death.count}`);
  }
  console.log(
    "Kết luận: x-death đếm mỗi lần message bị dead-letter, worker dùng nó để biết khi nào bỏ cuộc và chuyển sang DLQ.",
  );
} finally {
  const channel = await connection.createChannel();
  for (const suffix of [".work", ".retry", ".dlq"]) await channel.deleteQueue(name + suffix);
  for (const suffix of [".work", ".retry", ".dlq"]) await channel.deleteExchange(name + suffix);
  await connection.close();
}
