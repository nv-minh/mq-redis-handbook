// Command demo cho thấy group mới đọc lại toàn bộ log, rồi hai kịch bản crash cho thấy thứ tự "xử lý" và "commit"
// quyết định message được xử lý lại (at-least-once) hay mất (at-most-once).
package main

import (
	"context"
	"fmt"
	"log"
	"slices"
	"strings"
	"time"

	lab "github.com/nv-minh/mq-redis-handbook/06-kafka/lab-03-offsets-replay/go"
	"github.com/nv-minh/mq-redis-handbook/internal/testkit"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() (err error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	var topics, groups []string
	// newTopic tạo topic 3 partition mới. Mỗi kịch bản dùng topic riêng để message của kịch bản khác không lẫn vào.
	newTopic := func() (string, error) {
		topic := testkit.UniqueName("demo-offsets")
		topics = append(topics, topic)
		if err := lab.CreateTopic(ctx, topic, 3); err != nil {
			return "", fmt.Errorf("không tạo được topic (%v): %w. Đã chạy make up chưa?", lab.Brokers(), err)
		}
		return topic, nil
	}
	newGroup := func(prefix string) string {
		g := testkit.UniqueName(prefix)
		groups = append(groups, g)
		return g
	}
	producer := lab.NewProducer()
	defer func() {
		if closeErr := producer.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
		cleanup := context.Background()
		for _, g := range groups {
			if delErr := lab.DeleteGroup(cleanup, g); delErr != nil && err == nil {
				err = delErr
			}
		}
		for _, topic := range topics {
			if delErr := lab.DeleteTopic(cleanup, topic); delErr != nil && err == nil {
				err = delErr
			}
		}
	}()
	topic, err := newTopic()
	if err != nil {
		return err
	}
	fmt.Printf("topic %s có 3 partition\n", topic)

	fmt.Println("--- replay: group mới đọc lại toàn bộ log ---")
	for i := 0; i < 6; i++ {
		if err := producer.Produce(ctx, topic, fmt.Sprintf("key-%d", i), fmt.Sprintf("event-%d", i)); err != nil {
			return err
		}
	}
	for _, name := range []string{"group A", "group B"} {
		values, err := lab.ReplayFromBeginning(ctx, topic, newGroup("demo-replay"))
		if err != nil {
			return err
		}
		slices.Sort(values)
		fmt.Printf("%s (mới, FirstOffset) đọc được %d message: %s\n", name, len(values), strings.Join(values, ", "))
	}
	fmt.Println("Kết luận: Kafka không xóa message sau khi đọc, nên mỗi group mới tự đọc lại từ đầu log (trong thời gian retention).")

	// crashScenario: M1 tới, consumer crash giữa xử lý và commit, M2 tới, consumer thay thế đọc tiếp.
	crashScenario := func(order lab.CommitOrder, key string) error {
		topic, err := newTopic()
		if err != nil {
			return err
		}
		groupID := newGroup("demo-crash")
		if err := producer.Produce(ctx, topic, key, key+"-M1"); err != nil {
			return err
		}
		var processed []string
		if _, err := lab.ConsumeOneThenCrash(ctx, topic, groupID, order, func(v string) { processed = append(processed, v) }); err != nil {
			return err
		}
		fmt.Printf("consumer 1 đã xử lý: [%s] rồi crash\n", strings.Join(processed, ", "))
		if err := producer.Produce(ctx, topic, key, key+"-M2"); err != nil {
			return err
		}
		seen, err := lab.ReceiveUntil(ctx, topic, groupID, key+"-M2")
		if err != nil {
			return err
		}
		fmt.Printf("consumer 2 (cùng group) nhận: [%s]\n", strings.Join(seen, ", "))
		return nil
	}

	fmt.Println("--- xử lý xong rồi mới commit, crash trước khi commit (at-least-once) ---")
	if err := crashScenario(lab.ProcessThenCommit, "a"); err != nil {
		return err
	}
	fmt.Println("Kết luận: offset chưa commit nên consumer thay thế đọc lại M1, M1 được xử lý lần hai. Không mất message, nhưng consumer phải idempotent.")

	fmt.Println("--- commit trước rồi mới xử lý, crash trước khi xử lý (at-most-once) ---")
	if err := crashScenario(lab.CommitThenProcess, "b"); err != nil {
		return err
	}
	fmt.Println("Kết luận: offset đã đi qua M1 nên consumer thay thế bắt đầu từ M2, M1 mất mà không ai biết.")
	return nil
}
