package main

import (
	"context"
	"flag"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/chowyu12/gmq/pkg/client"
	"github.com/chowyu12/gmq/pkg/log"
	pb "github.com/chowyu12/gmq/proto"
)

var (
	mode        = flag.String("mode", "prod", "Benchmark mode: prod (produce) or cons (consume)")
	topic       = flag.String("topic", "bench-topic", "Target topic")
	concurrency = flag.Int("c", 20, "Concurrency (Goroutines)")
	total       = flag.Int("n", 100000, "Total messages")
	batchSize   = flag.Int("b", 50, "Batch size")
	msgSize     = flag.Int("s", 512, "Message size (Bytes)")
	addr        = flag.String("addr", "localhost:50051", "Broker address")
)

func main() {
	flag.Parse()
	log.Init("warn") // Mute logs

	if *mode == "prod" {
		runProducerBench()
	} else {
		runConsumerBench()
	}
}

func runProducerBench() {
	payload := make([]byte, *msgSize)

	fmt.Printf("=== GMQ Producer Throughput Benchmark ===\n")
	fmt.Printf("Address: %s | Topic: %s\n", *addr, *topic)
	fmt.Printf("Concurrency: %d | Total: %d | Batch: %d | Size: %dB\n",
		*concurrency, *total, *batchSize, *msgSize)

	var count int64
	var claimed int64
	var errCount int64
	start := time.Now()
	var wg sync.WaitGroup

	// Progress reporter
	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			current := atomic.LoadInt64(&count)
			pct := float64(current) / float64(*total) * 100
			elapsed := time.Since(start).Seconds()
			tps := float64(current) / elapsed
			fmt.Printf("Progress: %.2f%% (%d/%d) | Rate: %.2f msg/s\n", pct, current, *total, tps)
			if current >= int64(*total) {
				return
			}
		}
	}()

	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			producer, err := client.NewProducer(&client.ProducerConfig{
				ServerAddr: *addr,
			})
			if err != nil {
				fmt.Printf("Failed to create producer: %v\n", err)
				return
			}
			defer producer.Close()

			for {
				if atomic.AddInt64(&claimed, int64(*batchSize)) > int64(*total) {
					return
				}

				items := make([]*pb.PublishItem, *batchSize)
				for j := 0; j < *batchSize; j++ {
					items[j] = &pb.PublishItem{
						Topic:   *topic,
						Payload: payload,
					}
				}

				_, err := producer.Publish(context.Background(), items)
				if err != nil {
					atomic.AddInt64(&errCount, 1)
				} else {
					atomic.AddInt64(&count, int64(*batchSize))
				}
			}
		}(i)
	}

	wg.Wait()
	duration := time.Since(start)
	actualCount := atomic.LoadInt64(&count)
	tps := float64(actualCount) / duration.Seconds()

	fmt.Printf("\n[Result] Duration: %v\n", duration)
	fmt.Printf("Success: %d | Failures: %d\n", actualCount, errCount)
	fmt.Printf("Avg Throughput: %.2f msg/s\n", tps)
	fmt.Printf("Bandwidth: %.2f MB/s\n", (tps*float64(*msgSize))/1024/1024)
}

func runConsumerBench() {
	fmt.Printf("=== GMQ Consumer Throughput Benchmark ===\n")
	fmt.Printf("Address: %s | Topic: %s | Group: bench-group\n", *addr, *topic)

	var count int64
	var lastCount int64

	// Rate reporter
	go func() {
		ticker := time.NewTicker(time.Second)
		for range ticker.C {
			current := atomic.LoadInt64(&count)
			fmt.Printf("Rate: %d msg/s | Total Received: %d\n", current-lastCount, current)
			lastCount = current
		}
	}()

	var wg sync.WaitGroup
	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			consumer, err := client.NewConsumer(&client.ConsumerConfig{
				ServerAddr:    *addr,
				ConsumerGroup: "bench-group",
				ConsumerID:    fmt.Sprintf("bench-cons-%d", id),
				Topic:         *topic,
			})
			if err != nil {
				fmt.Printf("Failed to create consumer [%d]: %v\n", id, err)
				return
			}
			defer consumer.Close()

			for {
				mctx, err := consumer.Receive(context.Background(), 5*time.Second)
				if err != nil {
					if err == context.DeadlineExceeded {
						continue 
					}
					fmt.Printf("Consumer error [%d]: %v\n", id, err)
					break
				}
				if mctx == nil {
					continue
				}
				atomic.AddInt64(&count, int64(len(mctx.Messages())))
				_ = mctx.Ack()
			}
		}(i)
	}
	wg.Wait()
}
