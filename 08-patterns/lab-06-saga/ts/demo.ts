// Demo saga đặt hàng qua ba service giả lập (kho, thanh toán, vận chuyển), mỗi service giữ trạng thái trong bộ nhớ.
// Ba kịch bản: thành công, thanh toán lỗi (kho được bù), và vận chuyển lỗi cùng lúc hoàn tiền cũng lỗi
// (saga báo SagaCompensationError, cần người vận hành). Giao dịch bù idempotent: gọi lại không làm sai trạng thái.
import { SagaCompensationError, runOrderSaga, type SagaStep } from "./lab.js";

interface Failures {
  payment?: boolean;
  shipment?: boolean;
  refund?: boolean;
}

function orderSteps(orderId: string, failures: Failures) {
  const reserved = new Set<string>();
  const charged = new Set<string>();
  const shipped = new Set<string>();
  const log = (message: string) => console.log(`    ${message}`);

  const steps: SagaStep[] = [
    {
      name: "reserve-inventory",
      async run() {
        reserved.add(orderId);
        log(`kho: giữ hàng cho ${orderId}`);
      },
      async compensate() {
        // Idempotent: nhả hàng hai lần vẫn cho cùng trạng thái (đã nhả).
        reserved.delete(orderId);
        log(`kho: nhả hàng của ${orderId} (bù)`);
      },
    },
    {
      name: "charge-payment",
      async run() {
        if (failures.payment) throw new Error("thẻ bị từ chối");
        charged.add(orderId);
        log(`thanh toán: trừ tiền ${orderId}`);
      },
      async compensate() {
        if (failures.refund) throw new Error("cổng thanh toán không phản hồi khi hoàn tiền");
        charged.delete(orderId);
        log(`thanh toán: hoàn tiền ${orderId} (bù)`);
      },
    },
    {
      name: "create-shipment",
      async run() {
        if (failures.shipment) throw new Error("hãng vận chuyển hết xe");
        shipped.add(orderId);
        log(`vận chuyển: tạo đơn giao ${orderId}`);
      },
      async compensate() {
        shipped.delete(orderId);
        log(`vận chuyển: hủy đơn giao ${orderId} (bù)`);
      },
    },
  ];
  const state = () =>
    `giữ hàng=${reserved.has(orderId)}, đã trừ tiền=${charged.has(orderId)}, đã tạo đơn giao=${shipped.has(orderId)}`;
  return { steps, state };
}

async function scenario(title: string, orderId: string, failures: Failures): Promise<void> {
  console.log(`\n== ${title} ==`);
  const { steps, state } = orderSteps(orderId, failures);
  try {
    const outcome = await runOrderSaga(steps);
    console.log(`  kết quả saga: ${outcome}`);
  } catch (error) {
    if (!(error instanceof SagaCompensationError)) throw error;
    console.log(`  saga KHÔNG hoàn tác được hết: ${error.message}`);
    console.log(`  bước đã bù được: ${error.compensated.join(", ") || "(không có)"}`);
    console.log("  -> cần người vận hành hoặc hàng đợi retry cho compensation");
  }
  console.log(`  trạng thái cuối: ${state()}`);
}

await scenario("1. Mọi bước thành công", "order-1", {});
await scenario("2. Thanh toán lỗi: chỉ bước kho (đã xong) được bù", "order-2", { payment: true });
await scenario("3. Vận chuyển lỗi và hoàn tiền cũng lỗi", "order-3", {
  shipment: true,
  refund: true,
});
console.log(
  "\nKết luận: bù theo thứ tự ngược, bước lỗi không được bù, và một compensation hỏng không chặn compensation còn lại nhưng phải được báo.",
);
