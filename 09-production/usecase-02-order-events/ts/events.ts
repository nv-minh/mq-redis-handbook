/** Các loại event của luồng order. Mọi event đi qua cùng một topic, key của message là order id. */
export type EventType =
  | "OrderPlaced"
  | "InventoryReserved"
  | "InventoryRejected"
  | "PaymentCompleted"
  | "PaymentFailed"
  | "InventoryReleased";

/**
 * Envelope của một event trên Kafka (JSON trong value của message).
 * `eventId` được Postgres sinh khi ghi dòng outbox, ổn định qua mọi lần relay publish lại: consumer dedupe theo nó.
 */
export interface DomainEvent {
  eventId: string;
  type: EventType;
  orderId: string;
  data: Record<string, unknown>;
}

/**
 * Kết quả xử lý một event của consumer:
 * `applied` là effect đã được áp dụng, `duplicate` là eventId đã xử lý (bỏ qua), `ignored` là loại event consumer không quan tâm.
 */
export type HandleResult = "applied" | "duplicate" | "ignored";

/**
 * Đọc value của message thành event. Trả undefined cho poison event: không phải JSON, không phải object,
 * thiếu `eventId` / `type` / `orderId` dạng chuỗi hay `data` dạng object, hoặc `OrderPlaced` thiếu `sku`, `quantity`, `amount` hợp lệ.
 * Loại event lạ (`type` không nằm trong EventType) vẫn hợp lệ: consumer bỏ qua chúng để tương thích về sau.
 */
export function parseEvent(value: string | null): DomainEvent | undefined {
  if (value === null) return undefined;
  let parsed: unknown;
  try {
    parsed = JSON.parse(value);
  } catch {
    return undefined;
  }
  if (typeof parsed !== "object" || parsed === null) return undefined;
  const { eventId, type, orderId, data } = parsed as Record<string, unknown>;
  if (typeof eventId !== "string" || eventId === "") return undefined;
  if (typeof type !== "string" || typeof orderId !== "string" || orderId === "") return undefined;
  if (typeof data !== "object" || data === null || Array.isArray(data)) return undefined;
  if (type === "OrderPlaced") {
    // Consumer cần các trường này để reserve và charge. Thiếu trường thì retry mãi cũng không xử lý được, nên là poison.
    const { sku, quantity, amount } = data as Record<string, unknown>;
    if (typeof sku !== "string" || sku === "") return undefined;
    // Cột `integer` của Postgres là int32: quantity lớn hơn làm insert lỗi mãi và chặn partition.
    if (
      typeof quantity !== "number" ||
      !Number.isInteger(quantity) ||
      quantity < 1 ||
      quantity > 2_147_483_647
    )
      return undefined;
    if (typeof amount !== "number" || !Number.isFinite(amount)) return undefined;
  }
  return { eventId, type: type as EventType, orderId, data: data as Record<string, unknown> };
}
