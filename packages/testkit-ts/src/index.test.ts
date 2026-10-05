import net from "node:net";
import { describe, expect, it } from "vitest";
import { eventually, uniqueName, waitForPort } from "./index.js";

function getFreePort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const server = net.createServer();
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
      const { port } = server.address() as net.AddressInfo;
      server.close(() => resolve(port));
    });
  });
}

describe("waitForPort", () => {
  it("rejects with host:port in message after timeout", async () => {
    const port = await getFreePort();
    const started = Date.now();
    const error = await waitForPort("127.0.0.1", port, 300).then(
      () => undefined,
      (e: unknown) => e as Error,
    );
    expect(error).toBeInstanceOf(Error);
    expect(error?.message).toContain("127.0.0.1:");
    expect(error?.message).toContain(String(port));
    expect(Date.now() - started).toBeLessThan(3000);
  });

  it("resolves when a listener opens", async () => {
    const port = await getFreePort();
    const server = net.createServer();
    const opener = setTimeout(() => server.listen(port, "127.0.0.1"), 100);
    try {
      await waitForPort("127.0.0.1", port, 5000);
    } finally {
      clearTimeout(opener);
      server.close();
    }
  });
});

describe("uniqueName", () => {
  it("returns different values for two calls with same prefix", () => {
    const a = uniqueName("orders");
    const b = uniqueName("orders");
    expect(a).not.toBe(b);
    expect(a.startsWith("orders-")).toBe(true);
    expect(b.startsWith("orders-")).toBe(true);
  });
});

describe("eventually", () => {
  it("returns first truthy value", async () => {
    let calls = 0;
    const value = await eventually(
      async () => {
        calls += 1;
        return calls >= 3 ? "ready" : undefined;
      },
      { timeoutMs: 2000, intervalMs: 10 },
    );
    expect(value).toBe("ready");
    expect(calls).toBe(3);
  });

  it("rejects after timeout with last error", async () => {
    const error = await eventually(
      async () => {
        throw new Error("still failing");
      },
      { timeoutMs: 200, intervalMs: 20 },
    ).then(
      () => undefined,
      (e: unknown) => e as Error,
    );
    expect(error).toBeInstanceOf(Error);
    expect(error?.message).toContain("still failing");
  });
});
