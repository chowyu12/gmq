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
	"github.com/chowyu12/gmq/pkg/log"
	pb "github.com/chowyu12/gmq/proto"
)

func main() {
	log.Init("debug")
	log.Info("=== GMQ 消费组示例 ===")
	log.Info("启动多个消费者组成消费组，演示负载均衡...")

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

	log.Info("正在关闭所有消费者...")
}

func startConsumer(id int, consumerGroup, topic string) {
	consumerID := fmt.Sprintf("consumer-%d", id)

	var messageCount int
	var mu sync.Mutex

	var consumer *client.Consumer
	var err error

	consumer, err = client.NewConsumer(&client.ConsumerConfig{
		ServerAddr:    "localhost:50051",
		ConsumerGroup: consumerGroup,
		ConsumerID:    consumerID,
		MessageHandler: func(ctx client.MessageContext) error {
			msgs := ctx.Messages()
			mu.Lock()
			messageCount += len(msgs)
			currentTotal := messageCount
			mu.Unlock()

			log.Info(">>> 收到消息批次",
				"consumer", consumerID,
				"batchSize", len(msgs),
				"totalReceived", currentTotal)

			// 模拟批量业务处理
			for _, msg := range msgs {
				log.Debug("处理消息", "msgID", msg.MessageId, "payload", string(msg.Payload))
			}

			// 显式批量确认
			if err := consumer.Ack(context.Background(), msgs); err != nil {
				log.Error("批量确认失败", "error", err)
				return err
			}
			log.Info("<<< 批次确认成功", "consumer", consumerID, "count", len(msgs))
			return nil
		},
		ErrorHandler: func(err error) {
			log.Error("消费者错误", "consumer", consumerID, "error", err)
		},
	})
	if err != nil {
		log.Error("创建消费者失败", "consumer", consumerID, "error", err)
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
		log.Error("订阅失败", "consumer", consumerID, "error", err)
		return
	}

	log.Info("消费者已启动，等待消息...", "consumer", consumerID)

	// 保持运行
	select {}
}
