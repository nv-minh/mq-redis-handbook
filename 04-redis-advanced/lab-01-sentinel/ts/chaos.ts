import { execFile } from "node:child_process";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";

const run = promisify(execFile);

/** The compose project of this handbook. Chaos never touches a container outside it. */
const PROJECT = "mq-handbook";
const COMPOSE_FILE = fileURLToPath(new URL("../../../infra/docker-compose.yml", import.meta.url));

/** The only services chaos may stop: the three Redis nodes of the sentinel profile. */
export const CHAOS_SERVICES = ["redis-master", "redis-replica-1", "redis-replica-2"] as const;

function compose(action: "stop" | "start", service: string): Promise<unknown> {
  if (!(CHAOS_SERVICES as readonly string[]).includes(service)) {
    throw new Error(`refusing to ${action} "${service}": not one of ${CHAOS_SERVICES.join(", ")}`);
  }
  const extra = action === "stop" ? ["-t", "1"] : [];
  return run("docker", ["compose", "-p", PROJECT, "-f", COMPOSE_FILE, action, ...extra, service], {
    timeout: 120_000,
  });
}

/** Stop one sentinel-profile Redis node (SIGTERM, one second grace) through docker compose. */
export const stopService = (service: string): Promise<unknown> => compose("stop", service);

/** Start a node that was stopped. Safe to call for a node that is already running. */
export const startService = (service: string): Promise<unknown> => compose("start", service);
