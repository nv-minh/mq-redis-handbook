import type { Redis } from "ioredis";

export interface Subscription {
  /** Messages received so far, in arrival order. */
  messages(): string[];
  /** Stop listening. The connection itself stays open: the caller owns it. */
  close(): Promise<void>;
}

/**
 * Publish `message` on `channel` and resolve with the number of subscribers that received it.
 * 0 means nobody was listening and the message is gone: Pub/Sub keeps no history.
 */
export function publish(redis: Redis, channel: string, message: string): Promise<number> {
  return redis.publish(channel, message);
}

/**
 * Number of clients subscribed to `channel` with SUBSCRIBE (PUBSUB NUMSUB). Measured against
 * ioredis 6.0.0 (RESP3, Redis 8.10.2): the reply is a flat array ["channel", 1, "other", 0] and
 * the counts are already numbers.
 */
export async function numSubscribers(redis: Redis, channel: string): Promise<number> {
  const reply = (await redis.pubsub("NUMSUB", channel)) as [string, number];
  return reply[1];
}

/**
 * Subscribe `connection` to `channel` and collect what arrives.
 * `connection` must be dedicated to subscribing: do not share it with normal commands.
 * `connection.subscribe` resolves only after Redis confirmed the subscription, so once this
 * function returns, every later PUBLISH on the channel reaches this subscriber.
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
