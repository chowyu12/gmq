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
	// Create producer client
	producer, err := client.NewProducer(&client.ProducerConfig{
		ServerAddr: "localhost:50051",
	})
	if err != nil {
		log.Error("Failed to create producer", "error", err)
		return
	}
	defer producer.Close()

	log.Info("=== GMQ Producer Batch Send Example ===")

	// 1. Batch publish messages (Same Topic)
	log.Info("1. Batch publishing messages...")
	var items []*pb.PublishItem
	for i := 0; i < 10; i++ {
		items = append(items, &pb.PublishItem{
			Topic:        "test-topic",
			Payload:      []byte(fmt.Sprintf("Batch message #%d", i)),
			PartitionKey: fmt.Sprintf("key-%d", i),
		})
	}
	resp, err := producer.Publish(context.Background(), items)
	if err != nil {
		log.Error("Batch publish failed", "error", err)
	} else {
		log.Info("Batch publish success", "results", len(resp.Results))
	}

	// 2. Cross Topic batch publish
	log.Info("2. Cross Topic batch publishing...")
	mixedItems := []*pb.PublishItem{
		{Topic: "orders", Payload: []byte("Order message")},
		{Topic: "payments", Payload: []byte("Payment message")},
		{Topic: "notifications", Payload: []byte("Notification message")},
	}
	resp, err = producer.Publish(context.Background(), mixedItems)
	if err != nil {
		log.Error("Mixed batch publish failed", "error", err)
	} else {
		for _, res := range resp.Results {
			log.Info("Message published", "topic", res.Topic, "msgID", res.MessageId, "success", res.Success)
		}
	}

	log.Info("Producer example completed!")
	time.Sleep(1 * time.Second)
}
