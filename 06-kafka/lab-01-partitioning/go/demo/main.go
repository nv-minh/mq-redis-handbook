// Command demo produce message có key vào topic 3 partition và in ra key nào vào partition nào,
// cho thấy bẫy của balancer mặc định (round-robin) trong kafka-go và hành vi của key nil.
package main

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	lab "github.com/nv-minh/mq-redis-handbook/06-kafka/lab-01-partitioning/go"
	"github.com/nv-minh/mq-redis-handbook/internal/testkit"
	"github.com/segmentio/kafka-go"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() (err error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	topic := testkit.UniqueName("demo-partitioning")
	if err := lab.CreateTopic(ctx, topic, 3); err != nil {
		return fmt.Errorf("không tạo được topic (%v): %w. Đã chạy make up chưa?", lab.Brokers(), err)
	}
	defer func() {
		if delErr := lab.DeleteTopic(context.Background(), topic); delErr != nil && err == nil {
			err = delErr
		}
	}()
	fmt.Printf("topic %s có 3 partition, client kafka-go\n", topic)

	keys := []string{"alice", "bob", "carol", "dave", "erin"}
	partitionsOf := func(p *lab.Producer, key string, n int) (string, error) {
		var out []string
		for round := 0; round < n; round++ {
			res, err := p.ProduceKeyed(ctx, topic, key, fmt.Sprintf("%s-%d", key, round))
			if err != nil {
				return "", err
			}
			out = append(out, fmt.Sprint(res.Partition))
		}
		return strings.Join(out, ", "), nil
	}

	fmt.Println("--- Murmur2Balancer đặt tường minh: cùng key luôn vào cùng partition ---")
	murmur := lab.NewProducer(&kafka.Murmur2Balancer{})
	defer func() { _ = murmur.Close() }()
	for _, key := range keys {
		parts, err := partitionsOf(murmur, key, 3)
		if err != nil {
			return err
		}
		fmt.Printf("key %-6s -> partition %s\n", key, parts)
	}
	fmt.Println("Kết luận: Murmur2Balancer khớp hash của Java và kafka-javascript, nên key rơi vào cùng partition như demo TypeScript.")

	fmt.Println("--- balancer mặc định (không đặt Balancer): cùng một key vẫn bị rải ra ---")
	def := lab.NewProducer(nil)
	defer func() { _ = def.Close() }()
	parts, err := partitionsOf(def, "alice", 6)
	if err != nil {
		return err
	}
	fmt.Printf("key alice  -> partition %s\n", parts)
	fmt.Println("Kết luận: kafka-go mặc định round-robin, cùng key KHÔNG nằm trên một partition nếu không đặt Balancer tường minh.")

	fmt.Println("--- key nil với RoundRobin tường minh ---")
	rr := lab.NewProducer(&kafka.RoundRobin{})
	defer func() { _ = rr.Close() }()
	var out []string
	for i := 0; i < 12; i++ {
		res, err := rr.ProduceUnkeyed(ctx, topic, fmt.Sprintf("nil-%d", i))
		if err != nil {
			return err
		}
		out = append(out, fmt.Sprint(res.Partition))
	}
	fmt.Printf("partition của 12 message: %s\n", strings.Join(out, " "))
	fmt.Println("Kết luận: RoundRobin luân phiên qua các partition. Murmur2Balancer chọn ngẫu nhiên cho key nil, Hash luân phiên.")
	return nil
}
