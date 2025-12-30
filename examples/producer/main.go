package main

import (
	"context"
	"fmt"
	"time"

	"github.com/chowyu12/gmq/pkg/client"
	pb "github.com/chowyu12/gmq/proto"
)

func main() {
	// 创建生产者客户端
	producer, err := client.NewProducer(&client.ProducerConfig{
		ServerAddr: "localhost:50051",
	})
	if err != nil {
		fmt.Printf("创建生产者失败: %v\n", err)
		return
	}
	defer producer.Close()

	fmt.Println("=== GMQ 生产者示例 ===")

	// 发布 QoS 0 消息
	fmt.Println("\n1. 发布 QoS 0 消息...")
	for i := 0; i < 5; i++ {
		payload := []byte(fmt.Sprintf("QoS 0 消息 #%d - %s", i, time.Now().Format(time.RFC3339)))
		resp, err := producer.Publish(
			context.Background(),
			"test-topic",
			payload,
			client.WithQoS(pb.QoS_QOS_AT_MOST_ONCE),
			client.WithPartitionKey(fmt.Sprintf("key-%d", i%2)),
			client.WithProperties(map[string]string{
				"source": "producer-example",
				"index":  fmt.Sprintf("%d", i),
			}),
		)
		if err != nil {
			fmt.Printf("发布消息失败: %v\n", err)
			continue
		}
		fmt.Printf("✓ 消息已发布: msgID=%s, partition=%d, topic=%s\n",
			resp.MessageId, resp.PartitionId, resp.Topic)
		time.Sleep(500 * time.Millisecond)
	}

	// 发布 QoS 1 消息
	fmt.Println("\n2. 发布 QoS 1 消息...")
	for i := 0; i < 3; i++ {
		payload := []byte(fmt.Sprintf("QoS 1 消息 #%d - %s", i, time.Now().Format(time.RFC3339)))
		resp, err := producer.Publish(
			context.Background(),
			"test-topic",
			payload,
			client.WithQoS(pb.QoS_QOS_AT_LEAST_ONCE),
			client.WithPartitionID(1), // 指定分区
			client.WithProperties(map[string]string{
				"source":   "producer-example",
				"priority": "high",
			}),
		)
		if err != nil {
			fmt.Printf("发布消息失败: %v\n", err)
			continue
		}
		fmt.Printf("✓ 消息已发布 (QoS 1): msgID=%s, partition=%d\n",
			resp.MessageId, resp.PartitionId)
		time.Sleep(500 * time.Millisecond)
	}

	// 发布到不同 topic
	fmt.Println("\n3. 发布到多个 topic...")
	topics := []string{"orders", "payments", "notifications"}
	for _, topic := range topics {
		payload := []byte(fmt.Sprintf("消息到 %s - %s", topic, time.Now().Format(time.RFC3339)))
		resp, err := producer.Publish(
			context.Background(),
			topic,
			payload,
			client.WithQoS(pb.QoS_QOS_AT_MOST_ONCE),
		)
		if err != nil {
			fmt.Printf("发布消息到 %s 失败: %v\n", topic, err)
			continue
		}
		fmt.Printf("✓ 消息已发布到 %s: msgID=%s, partition=%d\n",
			topic, resp.MessageId, resp.PartitionId)
		time.Sleep(300 * time.Millisecond)
	}

	fmt.Println("\n生产者示例完成！")
	time.Sleep(2 * time.Second)
}
