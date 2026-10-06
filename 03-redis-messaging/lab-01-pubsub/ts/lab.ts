import type { Redis } from "ioredis";

export interface Subscription {
  /** Các message đã nhận tới giờ, theo thứ tự đến. */
  messages(): string[];
  /** Ngừng lắng nghe. Bản thân connection vẫn mở: caller sở hữu nó. */
  close(): Promise<void>;
}

/**
 * Publish `message` lên `channel` và resolve với số subscriber đã nhận được nó.
 * 0 nghĩa là không ai đang lắng nghe và message đã mất: Pub/Sub không giữ lịch sử.
 */
export function publish(redis: Redis, channel: string, message: string): Promise<number> {
  return redis.publish(channel, message);
}

/**
 * Số client đang subscribe `channel` bằng SUBSCRIBE (PUBSUB NUMSUB). Đã đo với
 * ioredis 6.0.0 (RESP3, Redis 8.10.2): reply là mảng phẳng ["channel", 1, "other", 0] và
 * các số đếm đã là number sẵn.
 */
export async function numSubscribers(redis: Redis, channel: string): Promise<number> {
  const reply = (await redis.pubsub("NUMSUB", channel)) as [string, number];
  return reply[1];
}

/**
 * Cho `connection` subscribe `channel` và gom những gì tới.
 * `connection` phải dành riêng cho việc subscribe: không dùng chung với các lệnh thường.
 * `connection.subscribe` chỉ resolve sau khi Redis xác nhận subscription, nên khi hàm này
 * return, mọi PUBLISH về sau trên channel đều tới được subscriber này.
 */
export async function subscribe(connection: Redis, channel: string): Promise<Subscription> {
  const received: string[] = [];
  const onMessage = (from: string, message: string) => {
    if (from === channel) received.push(message);
  };
  connection.on("message", onMessage);
  await connection.subscribe(channel);
  return {
    messages: () => [...received],
    close: async () => {
      connection.off("message", onMessage);
      if (connection.status === "ready") await connection.unsubscribe(channel);
    },
  };
}
