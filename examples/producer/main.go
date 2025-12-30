package main

import (
	"context"
	"fmt"
	"time"

	"github.com/chowyu12/gmq/pkg/client"
	"github.com/chowyu12/gmq/pkg/log"
	pb "github.com/chowyu12/gmq/proto"
)

func main() {
	log.Init("info")
	// 创建生产者客户端
	producer, err := client.NewProducer(&client.ProducerConfig{
		ServerAddr: "localhost:50051",
	})
	if err != nil {
		log.Error("创建生产者失败", "error", err)
		return
	}
	defer producer.Close()

	log.Info("=== GMQ 生产者示例 ===")

	// 发布 QoS 0 消息
	log.Info("1. 发布 QoS 0 消息...")
	for i := 0; i < 20; i++ { // 增加消息数量
		payload := []byte(fmt.Sprintf("QoS 0 消息 #%d - %s", i, time.Now().Format(time.RFC3339)))
		resp, err := producer.Publish(
			context.Background(),
			"test-topic",
			payload,
			client.WithQoS(pb.QoS_QOS_AT_MOST_ONCE),
			client.WithPartitionKey(fmt.Sprintf("key-%d", i)), // 每个消息使用不同的 key 以均匀分布分区
			client.WithProperties(map[string]string{
				"source": "producer-example",
				"index":  fmt.Sprintf("%d", i),
			}),
		)
		if err != nil {
			log.Error("发布消息失败", "error", err)
			continue
		}
		log.Info("消息已发布", "msgID", resp.MessageId, "partition", resp.PartitionId, "topic", resp.Topic)
		time.Sleep(500 * time.Millisecond)
	}

	// 发布 QoS 1 消息
	log.Info("2. 发布 QoS 1 消息...")
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
			log.Error("发布消息失败", "error", err)
			continue
		}
		log.Info("消息已发布 (QoS 1)", "msgID", resp.MessageId, "partition", resp.PartitionId)
		time.Sleep(500 * time.Millisecond)
	}

	// 发布到不同 topic
	log.Info("3. 发布到多个 topic...")
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
			log.Error("发布消息失败", "topic", topic, "error", err)
			continue
		}
		log.Info("消息已发布", "topic", topic, "msgID", resp.MessageId, "partition", resp.PartitionId)
		time.Sleep(300 * time.Millisecond)
	}

	log.Info("生产者示例完成！")
	time.Sleep(2 * time.Second)
}
