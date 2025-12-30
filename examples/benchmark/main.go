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
	mode        = flag.String("mode", "prod", "压测模式: prod (生产) 或 cons (消费)")
	topic       = flag.String("topic", "bench-topic", "压测 Topic")
	concurrency = flag.Int("c", 20, "并发数 (Goroutines)")
	total       = flag.Int("n", 100000, "消息总数")
	batchSize   = flag.Int("b", 50, "每批发送的消息数")
	msgSize     = flag.Int("s", 512, "消息大小 (Bytes)")
	addr        = flag.String("addr", "localhost:50051", "Broker 地址")
)

func main() {
	flag.Parse()
	log.Init("warn") // 屏蔽大量日志

	if *mode == "prod" {
		runProducerBench()
	} else {
		runConsumerBench()
	}
}

func runProducerBench() {
	payload := make([]byte, *msgSize)
	producer, err := client.NewProducer(&client.ProducerConfig{ServerAddr: *addr})
	if err != nil {
		fmt.Printf("创建生产者失败: %v\n", err)
		return
	}
	defer producer.Close()

	fmt.Printf("=== GMQ 生产吞吐量压测 ===\n")
	fmt.Printf("地址: %s | Topic: %s\n", *addr, *topic)
	fmt.Printf("并发: %d | 总量: %d | 批大小: %d | 消息大小: %dB\n",
		*concurrency, *total, *batchSize, *msgSize)

	var count int64
	var errCount int64
	start := time.Now()
	var wg sync.WaitGroup

	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				if atomic.LoadInt64(&count) >= int64(*total) {
					return
				}

				items := make([]*pb.PublishItem, *batchSize)
				for j := 0; j < *batchSize; j++ {
					items[j] = &pb.PublishItem{
						Topic:   *topic,
						Payload: payload,
						Qos:     pb.QoS_QOS_AT_MOST_ONCE,
					}
				}

				_, err := producer.Publish(context.Background(), items)
				if err != nil {
					atomic.AddInt64(&errCount, 1)
				} else {
					atomic.AddInt64(&count, int64(*batchSize))
				}
			}
		}()
	}

	wg.Wait()
	duration := time.Since(start)
	actualCount := atomic.LoadInt64(&count)
	tps := float64(actualCount) / duration.Seconds()

	fmt.Printf("\n[结果] 耗时: %v\n", duration)
	fmt.Printf("成功发送: %d | 失败次数: %d\n", actualCount, errCount)
	fmt.Printf("平均吞吐量: %.2f msg/s\n", tps)
	fmt.Printf("数据带宽: %.2f MB/s\n", (tps*float64(*msgSize))/1024/1024)
}

func runConsumerBench() {
	fmt.Printf("=== GMQ 消费吞吐量压测 ===\n")
	fmt.Printf("地址: %s | Topic: %s | 消费组: bench-group\n", *addr, *topic)

	var count int64
	var lastCount int64

	// 启动定时器打印每秒消费速率
	go func() {
		ticker := time.NewTicker(time.Second)
		for range ticker.C {
			current := atomic.LoadInt64(&count)
			fmt.Printf("当前消费速率: %d msg/s | 总接收: %d\n", current-lastCount, current)
			lastCount = current
		}
	}()

	var wg sync.WaitGroup
	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			consumer, _ := client.NewConsumer(&client.ConsumerConfig{
				ServerAddr:     *addr,
				ConsumerGroup:  "bench-group",
				ConsumerID:     fmt.Sprintf("bench-cons-%d", id),
				Topic:          *topic,
				PullIntervalMs: 10, // 降低拉取间隔至 10ms
			})

			for {
				mctx, err := consumer.Receive(context.Background())
				if err != nil {
					break
				}
				atomic.AddInt64(&count, int64(len(mctx.Messages())))
				// 直接使用便捷的批量 Ack
				_ = mctx.Ack()
			}
		}(i)
	}
	wg.Wait()
}
