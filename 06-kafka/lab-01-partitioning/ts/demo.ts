// Demo: produce message có key vào topic 3 partition và in ra key nào vào partition nào,
// rồi so sánh hành vi của key null khi gửi từng message một với khi gửi cả lô trong một lần send.
import { uniqueName } from "@handbook/testkit";
import { KafkaJS } from "@confluentinc/kafka-javascript";
import {
  brokers,
  closeProducer,
  createTopic,
  deleteTopic,
  produceKeyed,
  produceUnkeyed,
} from "./lab.js";

const topic = uniqueName("demo-partitioning");
const keys = ["alice", "bob", "carol", "dave", "erin"];

try {
  await createTopic(topic, 3);
  console.log(`topic ${topic} có 3 partition, client kafka-javascript (murmur2_random)`);

  console.log("--- cùng key luôn vào cùng partition ---");
  for (const key of keys) {
    const partitions: number[] = [];
    for (let round = 0; round < 3; round++) {
      partitions.push((await produceKeyed(topic, key, `${key}-${round}`)).partition);
    }
    console.log(`key ${key.padEnd(6)} -> partition ${partitions.join(", ")}`);
  }
  console.log(
    "Kết luận: mỗi key luôn rơi vào một partition, nên thứ tự theo key được giữ. Go dùng Murmur2Balancer nên cho cùng kết quả.",
  );

  console.log("--- key null: gửi từng message một, chờ ack giữa các lần ---");
  const one: number[] = [];
  for (let i = 0; i < 12; i++) one.push((await produceUnkeyed(topic, `one-${i}`)).partition);
  console.log(`partition của 12 message: ${one.join(" ")}`);

  console.log("--- key null: 12 message trong cùng một lần send (một batch) ---");
  const producer = new KafkaJS.Kafka({
    kafkaJS: { brokers: brokers(), logLevel: KafkaJS.logLevel.ERROR },
  }).producer({ kafkaJS: { acks: -1, allowAutoTopicCreation: false } });
  await producer.connect();
  try {
    const sent = await producer.send({
      topic,
      messages: Array.from({ length: 12 }, (_, i) => ({ key: null, value: `batch-${i}` })),
    });
    console.log(`partition trong ack: ${sent.map((m) => m.partition).join(" ")}`);
  } finally {
    await producer.disconnect();
  }
  console.log(
    "Kết luận: key null dùng sticky partitioning, các message gửi sát nhau có thể dồn vào cùng một partition, nên đừng kỳ vọng phân phối đều.",
  );
} finally {
  await closeProducer();
  await deleteTopic(topic);
}
