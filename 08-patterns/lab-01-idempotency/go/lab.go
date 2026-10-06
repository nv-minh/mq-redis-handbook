// Package lab cài đặt idempotent consumer bằng một lệnh Redis: SET key NX EX. Một message id
// chỉ chạy effect một lần trong thời gian sống của key, các bản giao lại trùng bị bỏ qua.
package lab

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// Result cho biết effect đã chạy hay message này đã được thấy rồi.
type Result string

const (
	// Applied nghĩa là lời gọi này đã chiếm key và effect chạy thành công.
	Applied Result = "applied"
	// Duplicate nghĩa là key đã tồn tại: một lời gọi khác đã chiếm (xong hoặc còn đang chạy).
	Duplicate Result = "duplicate"
)

// DedupKey trả về key dedup của một message id. Id phải ổn định qua mọi lần redeliver
// (id do producer gán, không phải id do transport sinh lại).
func DedupKey(messageID string) string {
	return "idempotency:" + messageID
}

// ProcessOnce chạy `SET key 1 NX EX ttlSeconds` để chiếm messageID, rồi chạy effect.
//
//   - SET NX là atomic set-if-absent: trong N lời gọi đồng thời cùng một id, đúng một lời gọi chiếm
//     được key và chạy effect, những lời gọi còn lại nhận Duplicate.
//   - Nếu effect trả lỗi, key bị xóa (nhả) rồi lỗi gốc được trả lại (errors.Is vẫn khớp), để lần
//     redeliver sau còn áp dụng được. Nếu không nhả, mọi lần retry sẽ bị coi là trùng và effect bị mất.
//   - Key sống ttlSeconds giây sau khi thành công. TTL phải lớn hơn cửa sổ mà broker còn có thể
//     redeliver (và cả replay từ DLQ), vì khi key hết hạn một bản trùng đến muộn sẽ được áp dụng lại.
//
// Giới hạn đã biết (xem README): trong lúc effect đầu còn chạy, bản trùng nhận Duplicate và consumer
// thường sẽ ack nó. Nếu effect đầu rồi lỗi và nhả key thì bản trùng đã ack đó không ai xử lý nữa.
func ProcessOnce(ctx context.Context, rdb *redis.Client, messageID string, effect func(context.Context) error, ttlSeconds int) (Result, error) {
	if ttlSeconds <= 0 {
		return "", fmt.Errorf("ttlSeconds phải là số nguyên dương (SET EX), nhận được %d", ttlSeconds)
	}
	key := DedupKey(messageID)
	// TTL là số giây nguyên nên go-redis gửi EX (không phải PX).
	err := rdb.SetArgs(ctx, key, "1", redis.SetArgs{Mode: "NX", TTL: time.Duration(ttlSeconds) * time.Second}).Err()
	if errors.Is(err, redis.Nil) {
		return Duplicate, nil
	}
	if err != nil {
		return "", err
	}

	if err := effect(ctx); err != nil {
		// Nhả key bằng context riêng: ctx của caller có thể đã bị hủy chính vì lỗi này.
		// Nếu DEL cũng lỗi thì key tự hết hạn sau TTL; lỗi của effect vẫn là lỗi được báo.
		releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_ = rdb.Del(releaseCtx, key).Err()
		return "", err
	}
	return Applied, nil
}
