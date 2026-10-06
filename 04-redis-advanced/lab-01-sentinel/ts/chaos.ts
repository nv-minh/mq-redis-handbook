import { execFile } from "node:child_process";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";

const run = promisify(execFile);

/** Compose project của handbook. Chaos không bao giờ đụng tới container nằm ngoài project này. */
const PROJECT = "mq-handbook";
const COMPOSE_FILE = fileURLToPath(new URL("../../../infra/docker-compose.yml", import.meta.url));

/** Chỉ ba node Redis của profile sentinel mới được phép dừng. */
export const CHAOS_SERVICES = ["redis-master", "redis-replica-1", "redis-replica-2"] as const;

function compose(action: "stop" | "start", service: string): Promise<unknown> {
  if (!(CHAOS_SERVICES as readonly string[]).includes(service)) {
    throw new Error(
      `từ chối ${action} "${service}": chỉ được phép với ${CHAOS_SERVICES.join(", ")}`,
    );
  }
  const extra = action === "stop" ? ["-t", "1"] : [];
  return run("docker", ["compose", "-p", PROJECT, "-f", COMPOSE_FILE, action, ...extra, service], {
    timeout: 120_000,
  });
}

/** Dừng một node Redis của profile sentinel qua docker compose (SIGTERM, chờ tối đa một giây). */
export const stopService = (service: string): Promise<unknown> => compose("stop", service);

/** Bật lại node đã dừng. Gọi cho node đang chạy cũng an toàn. */
export const startService = (service: string): Promise<unknown> => compose("start", service);
