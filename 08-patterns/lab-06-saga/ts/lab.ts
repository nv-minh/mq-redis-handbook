/**
 * Một bước của saga: `run` là local transaction của một service, `compensate` là giao dịch bù hoàn tác tác dụng của `run`.
 * `compensate` phải idempotent: saga thật có thể phải gọi nó lại sau khi lần đầu lỗi hoặc không rõ kết quả.
 */
export interface SagaStep {
  name: string;
  run(): Promise<void>;
  compensate(): Promise<void>;
}

/** Kết quả khi mọi thứ diễn ra sạch sẽ: `completed` (mọi bước xong) hoặc `compensated` (mọi bước đã xong đều đã được bù). */
export type SagaOutcome = "completed" | "compensated";

export interface CompensationFailure {
  /** Tên bước mà giao dịch bù bị lỗi. */
  step: string;
  error: unknown;
}

/**
 * Saga không hoàn tác được hết: có bước chạy lỗi VÀ ít nhất một giao dịch bù cũng lỗi, nên hệ thống đang ở trạng thái
 * không nhất quán và cần người vận hành (hoặc một hàng đợi retry cho compensation) xử lý tiếp.
 * Đây là lý do `runOrderSaga` reject thay vì resolve `compensated`: kết quả "compensated" chỉ được trả khi nó đúng.
 */
export class SagaCompensationError extends Error {
  /** Tên bước chạy lỗi đã kích hoạt compensation. */
  readonly failedStep: string;
  /** Lỗi gốc của bước chạy lỗi. */
  readonly stepError: unknown;
  /** Các bước bù thất bại, theo thứ tự bù (ngược). */
  readonly compensationErrors: CompensationFailure[];
  /** Các bước đã được bù thành công, theo thứ tự bù. */
  readonly compensated: string[];

  constructor(
    failedStep: string,
    stepError: unknown,
    compensationErrors: CompensationFailure[],
    compensated: string[],
  ) {
    const describe = (error: unknown) => (error instanceof Error ? error.message : String(error));
    const stuck = compensationErrors.map((f) => `"${f.step}" (${describe(f.error)})`).join(", ");
    super(
      `saga lỗi ở bước "${failedStep}" (${describe(stepError)}) và không bù được các bước: ${stuck}`,
      { cause: stepError },
    );
    this.name = "SagaCompensationError";
    this.failedStep = failedStep;
    this.stepError = stepError;
    this.compensationErrors = compensationErrors;
    this.compensated = compensated;
  }
}

/**
 * Chạy saga theo kiểu orchestration: bước nào chạy trước, bước nào bù, đều do hàm này quyết định ở một chỗ.
 *
 * - Chạy các bước lần lượt theo thứ tự. Hết bước thì trả `completed` (kể cả khi không có bước nào).
 * - Bước đầu tiên lỗi: các bước SAU nó không chạy, và các bước ĐÃ XONG được bù theo thứ tự NGƯỢC rồi trả `compensated`.
 *   Bước lỗi không được bù: ta coi local transaction của nó đã tự rollback. Nếu một bước có thể xong dở thì phải
 *   tự dọn bên trong `run`.
 * - Giao dịch bù lỗi: mỗi bước còn lại VẪN được bù đúng một lần (một compensation hỏng không được chặn các bước kia),
 *   rồi promise reject với `SagaCompensationError` mang tên bước lỗi, lỗi gốc, mọi lỗi bù và danh sách bước đã bù.
 *
 * Lab không retry: saga thật nên retry bước lỗi như lỗi tạm thời (có backoff) trước khi bù, và retry compensation
 * (hoặc đẩy vào hàng đợi retry) trước khi gọi người vận hành.
 */
export async function runOrderSaga(steps: SagaStep[]): Promise<SagaOutcome> {
  const completed: SagaStep[] = [];
  for (const step of steps) {
    try {
      await step.run();
    } catch (error) {
      return compensate(step.name, error, completed);
    }
    completed.push(step);
  }
  return "completed";
}

async function compensate(
  failedStep: string,
  stepError: unknown,
  completed: SagaStep[],
): Promise<SagaOutcome> {
  const compensated: string[] = [];
  const failures: CompensationFailure[] = [];
  for (const step of [...completed].reverse()) {
    try {
      await step.compensate();
      compensated.push(step.name);
    } catch (error) {
      failures.push({ step: step.name, error });
    }
  }
  if (failures.length > 0) {
    throw new SagaCompensationError(failedStep, stepError, failures, compensated);
  }
  return "compensated";
}
