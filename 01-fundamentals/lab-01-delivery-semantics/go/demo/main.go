// Command demo prints what each delivery semantics does to the same six messages.
package main

import (
	"fmt"
	"math/rand"
	"strings"

	lab "github.com/nv-minh/mq-redis-handbook/01-fundamentals/lab-01-delivery-semantics/go"
)

func main() {
	ids := make([]string, 6)
	for i := range ids {
		ids[i] = fmt.Sprintf("order-%d", i+1)
	}
	run(ids, lab.AtMostOnce, 0.4, false)
	run(ids, lab.AtLeastOnce, 0.4, false)
	run(ids, lab.AtLeastOnce, 0.4, true)
}

func run(ids []string, mode lab.Mode, lossRate float64, dedup bool) {
	q := lab.NewQueue(lab.Options{
		Mode:          mode,
		LossRate:      lossRate,
		Rand:          rand.New(rand.NewSource(7)).Float64,
		MaxDeliveries: 4,
	})
	var deliveries, applied []string
	apply := lab.NewIdempotentHandler(func(id string) error {
		applied = append(applied, id)
		return nil
	})
	q.Consume(func(id string) error {
		deliveries = append(deliveries, id)
		if dedup {
			return apply(id)
		}
		applied = append(applied, id)
		return nil
	})
	for _, id := range ids {
		q.Publish(id)
	}
	q.Drain()

	seen := map[string]bool{}
	dups := 0
	for _, id := range applied {
		if seen[id] {
			dups++
		}
		seen[id] = true
	}
	var lost []string
	for _, id := range ids {
		if !seen[id] {
			lost = append(lost, id)
		}
	}
	suffix := ""
	if dedup {
		suffix = ", idempotent consumer"
	}
	fmt.Printf("\n== %s, lossRate=%v%s\n", mode, lossRate, suffix)
	fmt.Printf("published : %d\n", len(ids))
	fmt.Printf("deliveries: %d (%s)\n", len(deliveries), strings.Join(deliveries, " "))
	fmt.Printf("applied   : %d (%s)\n", len(applied), strings.Join(applied, " "))
	if len(lost) == 0 {
		lost = []string{"none"}
	}
	fmt.Printf("never applied: %s\n", strings.Join(lost, " "))
	fmt.Printf("duplicates applied: %d\n", dups)
}
