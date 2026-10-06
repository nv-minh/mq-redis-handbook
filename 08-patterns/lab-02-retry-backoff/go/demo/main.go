// Command demo mô phỏng 1000 client cùng retry sau một sự cố với full jitter backoff.
// PRNG có seed nên mỗi lần chạy in ra đúng cùng một kết quả. Demo không cần broker.
package main

import (
	"fmt"
	"log"
	"math/rand/v2"
	"slices"
	"strings"
	"time"

	lab "github.com/nv-minh/mq-redis-handbook/08-patterns/lab-02-retry-backoff/go"
)

const clients = 1000

var opts = lab.Options{Base: 100 * time.Millisecond, Cap: 5 * time.Second}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func ms(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }

func run() error {
	r := rand.New(rand.NewPCG(2026, 1))
	fmt.Printf("== Full jitter: base %s, cap %s, %d client ==\n", opts.Base, opts.Cap, clients)
	fmt.Println("attempt | cận trên (không jitter) | min     | trung bình | max")
	for attempt := 0; attempt <= 8; attempt++ {
		delays := make([]float64, clients)
		sum := 0.0
		for i := range delays {
			d, err := lab.BackoffDelay(attempt, opts, r.Float64)
			if err != nil {
				return err
			}
			delays[i] = ms(d)
			sum += delays[i]
		}
		bound, err := lab.BackoffBound(attempt, opts)
		if err != nil {
			return err
		}
		fmt.Printf("%7d | %23.0f | %7.0f | %10.0f | %.0f\n",
			attempt, ms(bound), slices.Min(delays), sum/clients, slices.Max(delays))
	}

	const attempt = 5
	bound, err := lab.BackoffBound(attempt, opts)
	if err != nil {
		return err
	}
	fmt.Printf("\n== Phân bố delay của %d client ở attempt %d (cận trên %.0fms) ==\n", clients, attempt, ms(bound))
	fmt.Printf("Không jitter: cả %d client cùng retry đúng lúc %.0fms (thundering herd).\n", clients, ms(bound))
	fmt.Println("Full jitter: các client rải đều trên [0, cận trên), mỗi # là 5 client:")
	var buckets [10]int
	for i := 0; i < clients; i++ {
		d, err := lab.BackoffDelay(attempt, opts, r.Float64)
		if err != nil {
			return err
		}
		buckets[min(9, int(float64(d)/float64(bound)*10))]++
	}
	for i, count := range buckets {
		from := ms(bound) * float64(i) / 10
		to := ms(bound) * float64(i+1) / 10
		fmt.Printf("%5.0f-%5.0fms | %s %d\n", from, to, strings.Repeat("#", (count+2)/5), count)
	}

	fmt.Println("\n== Attempt rất lớn không tràn số ==")
	huge, err := lab.BackoffBound(1100, opts)
	if err != nil {
		return err
	}
	fmt.Printf("attempt 1100: cận trên %.0fms (2^1100 là +Inf khi tính bằng float, nhưng cap vẫn chặn).\n", ms(huge))
	return nil
}
