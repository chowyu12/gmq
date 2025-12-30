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
	log.Init("debug")
	// 创建生产者客户端
	producer, err := client.NewProducer(&client.ProducerConfig{
		ServerAddr: "localhost:50051",
	})
	if err != nil {
		log.Error("创建生产者失败", "error", err)
		return
	}
	defer producer.Close()

	log.Info("=== GMQ 生产者批量发送示例 ===")

	// 1. 批量发布 QoS 0 消息 (同一 Topic)
	log.Info("1. 批量发布 QoS 0 消息...")
	var qos0Items []*pb.PublishItem
	for i := 0; i < 10; i++ {
		qos0Items = append(qos0Items, &pb.PublishItem{
			Topic:        "test-topic",
			Payload:      []byte(fmt.Sprintf("批量 QoS 0 消息 #%d", i)),
			Qos:          pb.QoS_QOS_AT_MOST_ONCE,
			PartitionKey: fmt.Sprintf("key-%d", i),
		})
	}
	resp, err := producer.Publish(context.Background(), qos0Items)
	if err != nil {
		log.Error("批量发布失败", "error", err)
	} else {
		log.Info("批量发布成功", "results", len(resp.Results))
	}

	// 2. 跨 Topic 批量发布
	log.Info("2. 跨 Topic 批量发布消息...")
	mixedItems := []*pb.PublishItem{
		{Topic: "orders", Payload: []byte("订单消息"), Qos: pb.QoS_QOS_AT_LEAST_ONCE},
		{Topic: "payments", Payload: []byte("支付消息"), Qos: pb.QoS_QOS_AT_LEAST_ONCE},
		{Topic: "notifications", Payload: []byte("通知消息"), Qos: pb.QoS_QOS_AT_MOST_ONCE},
	}
	resp, err = producer.Publish(context.Background(), mixedItems)
	if err != nil {
		log.Error("混合批量发布失败", "error", err)
	} else {
		for _, res := range resp.Results {
			log.Info("消息已发布", "topic", res.Topic, "msgID", res.MessageId, "success", res.Success)
		}
	}

	log.Info("生产者示例完成！")
	time.Sleep(1 * time.Second)
}
