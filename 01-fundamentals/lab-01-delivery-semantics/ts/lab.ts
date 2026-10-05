// In-memory broker that shows what "delivery semantics" mean, with no real network.
// A message goes pending -> delivered to the handler -> (maybe) acked. Loss is injected through
// `lossRate` and `rand`, so every failure in the lab is reproducible.

export type Mode = "at-most-once" | "at-least-once";

export interface QueueOptions {
  mode: Mode;
  /**
   * Probability (0..1) that something goes wrong in transit:
   * - at-most-once: the delivery is lost, the handler never sees the message.
   * - at-least-once: the consumer's ack is lost, so the broker redelivers (duplicates).
   * 1 means it always happens, 0 means it never does.
   */
  lossRate: number;
  /** Source of randomness in [0, 1). Defaults to Math.random. */
  rand?: () => number;
  /** at-least-once only: deliveries before the message is parked as dead. Default 5. */
  maxDeliveries?: number;
}

export interface Queue {
  publish(id: string): void;
  /**
   * Register the consumer. A handler that throws models a consumer crash:
   * at-most-once has already forgotten the message, at-least-once never gets the ack and retries.
   */
  consume(handler: (id: string) => void): void;
  /** Resolves when every message that can be delivered has been settled (acked, lost or dead). */
  drain(): Promise<void>;
  /** Ids that used up maxDeliveries without an ack. */
  deadLetters(): string[];
}

export function createQueue(opts: QueueOptions): Queue {
  const rand = opts.rand ?? Math.random;
  const maxDeliveries = opts.maxDeliveries ?? 5;
  const pending: string[] = [];
  const deliveries = new Map<string, number>();
  const dead: string[] = [];
  let handler: ((id: string) => void) | undefined;
  let pump: Promise<void> | undefined;

  const deliver = (id: string, handle: (id: string) => void): void => {
    if (opts.mode === "at-most-once") {
      // Fire and forget: the broker drops the message before or while handing it over.
      if (rand() < opts.lossRate) return;
      try {
        handle(id);
      } catch {
        // Consumer crashed, but there is nothing to retry: the message is already gone.
      }
      return;
    }

    const attempt = (deliveries.get(id) ?? 0) + 1;
    deliveries.set(id, attempt);
    let acked = false;
    try {
      handle(id);
      acked = rand() >= opts.lossRate; // the ack travels back and may be lost
    } catch {
      // Consumer crashed: no ack either.
    }
    if (acked) {
      deliveries.delete(id);
    } else if (attempt >= maxDeliveries) {
      deliveries.delete(id);
      dead.push(id);
    } else {
      pending.push(id); // redeliver, behind the messages already waiting
    }
  };

  const run = async (): Promise<void> => {
    await Promise.resolve(); // let a burst of publish() calls land before delivering
    while (handler !== undefined && pending.length > 0) {
      deliver(pending.shift() as string, handler);
    }
    pump = undefined;
  };

  const kick = (): void => {
    if (pump === undefined && handler !== undefined && pending.length > 0) pump = run();
  };

  return {
    publish(id) {
      pending.push(id);
      kick();
    },
    consume(h) {
      handler = h;
      kick();
    },
    async drain() {
      while (pump !== undefined) await pump;
    },
    deadLetters: () => [...dead],
  };
}

/**
 * Wrap `apply` so redelivered ids are skipped. The id is recorded before apply runs and removed
 * again if apply throws, otherwise a failed attempt would be mistaken for a duplicate forever.
 * In-memory only: a real consumer must store the marker atomically with the side effect.
 */
export function createIdempotentHandler(apply: (id: string) => void): (id: string) => void {
  const seen = new Set<string>();
  return (id) => {
    if (seen.has(id)) return;
    seen.add(id);
    try {
      apply(id);
    } catch (error) {
      seen.delete(id);
      throw error;
    }
  };
}
