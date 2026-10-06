// Demo: consumer mới đọc lại lịch sử bằng DeliverAll, rồi chỉ đọc batch mới bằng DeliverByStartTime.
import { uniqueName } from "@handbook/testkit";
import {
  createReplayStream,
  openReplay,
  parseNanos,
  publishBatch,
  startTimeBetween,
  storedMessageTime,
  teardown,
} from "./lab.js";

const name = uniqueName("demo-replay");
const show = (bodies: { body: string }[]) => bodies.map((m) => m.body).join(", ");

try {
  await createReplayStream(name);
  const older = ["old-0", "old-1", "old-2", "old-3"];
  const newer = ["new-0", "new-1", "new-2", "new-3"];
  const olderSeqs = await publishBatch(name, older);
  const newerSeqs = await publishBatch(name, newer);
  console.log(`Đã publish batch cũ (${older.join(", ")}) rồi batch mới (${newer.join(", ")}).`);

  console.log("--- DeliverAll: mỗi consumer mới đọc lại toàn bộ lịch sử ---");
  for (const label of ["thứ nhất", "thứ hai"]) {
    const reader = await openReplay(name, { kind: "all" });
    const got = await reader.read();
    console.log(`Consumer ${label} (pending lúc tạo ${reader.pendingAtStart}): ${show(got)}`);
  }
  console.log(
    "Kết luận: đọc không xóa message (retention limits), nên consumer mới nào cũng thấy đủ lịch sử.",
  );

  console.log("--- DeliverByStartTime: bỏ qua message cũ hơn mốc ---");
  const lastOlder = await storedMessageTime(name, olderSeqs.at(-1)!);
  const firstNewer = await storedMessageTime(name, newerSeqs[0]!);
  const startTime = startTimeBetween(lastOlder, firstNewer);
  console.log(`Timestamp server của message cũ cuối : ${lastOlder}`);
  console.log(`Timestamp server của message mới đầu : ${firstNewer}`);
  console.log(`Mốc start time (nằm giữa hai timestamp, lấy từ server): ${startTime}`);
  const byTime = await openReplay(name, { kind: "by_start_time", startTime });
  console.log(
    `Consumer by_start_time (pending lúc tạo ${byTime.pendingAtStart}): ${show(await byTime.read())}`,
  );
  console.log(
    "Kết luận: server chọn message đầu tiên có timestamp >= mốc. Mốc lấy từ timestamp server nên không phụ thuộc đồng hồ của client.",
  );

  console.log("--- DeliverByStartTime với mốc sau message cuối cùng ---");
  const future = new Date(Number(parseNanos(lastOlder) / 1_000_000n) + 3_600_000).toISOString();
  console.log(`Mốc = timestamp message cuối + 1 giờ = ${future}`);
  const empty = await openReplay(name, { kind: "by_start_time", startTime: future });
  console.log(`Pending lúc tạo: ${empty.pendingAtStart}, đọc được: [${show(await empty.read())}]`);
  const [lateSeq] = await publishBatch(name, ["late-0"]);
  const lateTime = await storedMessageTime(name, lateSeq!);
  console.log(`Publish late-0 (timestamp server ${lateTime}, nhỏ hơn mốc)`);
  console.log(`Consumer đó đọc được: [${show(await empty.read())}]`);
  console.log(
    "Kết luận (đo trên server 2.15.0): mốc sau message cuối thì consumer bắt đầu ở message kế tiếp, giống deliver policy new.",
  );
} finally {
  await teardown(name);
}
