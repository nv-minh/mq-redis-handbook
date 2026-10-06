import { describe, expect, it } from "vitest";
import { SagaCompensationError, runOrderSaga, type SagaStep } from "./lab.js";

/** Tạo một bước ghi lại mọi lần gọi vào `events`. `failRun` và `failCompensate` ép bước đó ném lỗi. */
function step(
  events: string[],
  name: string,
  opts: { failRun?: boolean; failCompensate?: boolean } = {},
): SagaStep {
  return {
    name,
    async run() {
      events.push(`run:${name}`);
      if (opts.failRun) throw new Error(`${name} chạy lỗi`);
    },
    async compensate() {
      events.push(`compensate:${name}`);
      if (opts.failCompensate) throw new Error(`${name} bù lỗi`);
    },
  };
}

describe("lab-06 saga: orchestration với compensation", () => {
  it("all_steps_succeed_returns_completed", async () => {
    const events: string[] = [];
    const outcome = await runOrderSaga([step(events, "a"), step(events, "b"), step(events, "c")]);
    expect(outcome).toBe("completed");
    // Chạy đúng thứ tự khai báo và không có compensation nào.
    expect(events).toEqual(["run:a", "run:b", "run:c"]);
  });

  it("failure_compensates_completed_steps_in_reverse_order", async () => {
    const events: string[] = [];
    const outcome = await runOrderSaga([
      step(events, "a"),
      step(events, "b"),
      step(events, "c", { failRun: true }),
      step(events, "d"),
    ]);
    expect(outcome).toBe("compensated");
    // Bước c lỗi nên d không bao giờ chạy. Chỉ a và b (đã xong) được bù, theo thứ tự ngược, và bước lỗi c không được bù.
    expect(events).toEqual(["run:a", "run:b", "run:c", "compensate:b", "compensate:a"]);
  });

  it("compensation_failure_is_reported_and_remaining_compensations_still_run", async () => {
    const events: string[] = [];
    const saga = runOrderSaga([
      step(events, "a", { failCompensate: true }),
      step(events, "b"),
      step(events, "c", { failCompensate: true }),
      step(events, "d", { failRun: true }),
    ]);

    // Saga không được nói "compensated" khi còn bước chưa bù được: promise reject với SagaCompensationError.
    const error = await saga.then(
      () => undefined,
      (e: unknown) => e,
    );
    expect(error).toBeInstanceOf(SagaCompensationError);
    const report = error as SagaCompensationError;

    // c lỗi khi bù nhưng b và a vẫn được bù: mỗi bước đã xong được thử bù đúng một lần, theo thứ tự ngược.
    expect(events).toEqual([
      "run:a",
      "run:b",
      "run:c",
      "run:d",
      "compensate:c",
      "compensate:b",
      "compensate:a",
    ]);
    expect(report.failedStep).toBe("d");
    expect((report.stepError as Error).message).toBe("d chạy lỗi");
    expect(report.compensated).toEqual(["b"]);
    expect(report.compensationErrors.map((e) => e.step)).toEqual(["c", "a"]);
    expect(report.compensationErrors.map((e) => (e.error as Error).message)).toEqual([
      "c bù lỗi",
      "a bù lỗi",
    ]);
  });

  it("compensation_errors_name_the_steps_in_the_error_message", async () => {
    const events: string[] = [];
    const error = await runOrderSaga([
      step(events, "reserve-inventory", { failCompensate: true }),
      step(events, "charge-payment", { failRun: true }),
    ]).then(
      () => undefined,
      (e: unknown) => e as Error,
    );
    // Người vận hành đọc message này: phải thấy bước nào lỗi, lỗi gì, và bước nào chưa bù được.
    expect(error?.message).toContain("charge-payment");
    expect(error?.message).toContain("charge-payment chạy lỗi");
    expect(error?.message).toContain("reserve-inventory");
    expect(error?.message).toContain("reserve-inventory bù lỗi");
  });

  it("saga_with_zero_steps_returns_completed", async () => {
    expect(await runOrderSaga([])).toBe("completed");
  });

  it("first_step_failing_compensates_nothing", async () => {
    const events: string[] = [];
    const outcome = await runOrderSaga([step(events, "a", { failRun: true }), step(events, "b")]);
    expect(outcome).toBe("compensated");
    expect(events).toEqual(["run:a"]);
  });
});
