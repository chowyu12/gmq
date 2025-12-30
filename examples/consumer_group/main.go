package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/chowyu12/gmq/pkg/client"
	pb "github.com/chowyu12/gmq/proto"
)

func main() {
	fmt.Println("=== GMQ 消费组示例 ===")
	fmt.Println("启动多个消费者组成消费组，演示负载均衡...")

	consumerGroup := "demo-consumer-group"
	topic := "test-topic"

	// 启动 3 个消费者
	var wg sync.WaitGroup
	for i := 1; i <= 3; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			startConsumer(id, consumerGroup, topic)
		}(i)
		time.Sleep(500 * time.Millisecond) // 错开启动时间
	}

	// 等待退出信号
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	fmt.Println("\n正在关闭所有消费者...")
}

func startConsumer(id int, consumerGroup, topic string) {
	consumerID := fmt.Sprintf("consumer-%d", id)

	var messageCount int
	var mu sync.Mutex

	consumer, err := client.NewConsumer(&client.ConsumerConfig{
		ServerAddr:    "localhost:50051",
		ConsumerGroup: consumerGroup,
		ConsumerID:    consumerID,
		MessageHandler: func(msg *pb.ConsumeMessage) error {
			mu.Lock()
			messageCount++
			count := messageCount
			mu.Unlock()

			fmt.Printf("[%s] 收到第 %d 条消息 | msgID=%s | partition=%d | 内容=%s\n",
				consumerID, count, msg.MessageId[:8], msg.PartitionId, string(msg.Payload))

			// 模拟处理时间
			time.Sleep(200 * time.Millisecond)
			return nil
		},
		ErrorHandler: func(err error) {
			fmt.Printf("[%s] 错误: %v\n", consumerID, err)
		},
	})
	if err != nil {
		fmt.Printf("创建消费者 %s 失败: %v\n", consumerID, err)
		return
	}
	defer consumer.Close()

	// 订阅 topic
	err = consumer.Subscribe(
		context.Background(),
		topic,
		client.WithSubscribeQoS(pb.QoS_QOS_AT_MOST_ONCE),
	)
	if err != nil {
		fmt.Printf("[%s] 订阅失败: %v\n", consumerID, err)
		return
	}

	fmt.Printf("[%s] 已启动，等待消息...\n", consumerID)

	// 保持运行
	select {}
}
