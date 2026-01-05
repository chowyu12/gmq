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
)

func main() {
	log.Init("debug")
	log.Info("=== GMQ Consumer Group Example ===")
	log.Info("Starting multiple consumers in a group to demonstrate load balancing...")

	consumerGroup := "demo-consumer-group"
	topic := "test-topic"

	// Start 3 consumers
	var wg sync.WaitGroup
	for i := 1; i <= 3; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			startConsumer(id, consumerGroup, topic)
		}(i)
		time.Sleep(500 * time.Millisecond) // Stagger start times
	}

	// Wait for exit signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	log.Info("Shutting down all consumers...")
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
		Topic:         topic,
		ErrorHandler: func(err error) {
			log.Error("Consumer error", "consumerID", consumerID, "error", err)
		},
	})
	if err != nil {
		log.Error("Failed to create consumer", "consumerID", consumerID, "error", err)
		return
	}
	defer consumer.Close()

	log.Info("Consumer started, entering receive loop...", "consumerID", consumerID, "topic", topic)

	for {
		mctx, err := consumer.Receive(context.Background(), 10*time.Second)
		if err != nil && err != context.DeadlineExceeded {
			log.Error("Failed to receive messages", "consumerID", consumerID, "error", err)
			break
		}

		if mctx == nil {
			continue
		}

		msgs := mctx.Messages()
		mu.Lock()
		messageCount += len(msgs)
		currentTotal := messageCount
		mu.Unlock()

		log.Info(">>> Received message batch",
			"consumer", consumerID,
			"batchSize", len(msgs),
			"totalReceived", currentTotal)

		// Simulate business processing
		for _, msg := range msgs {
			log.Debug("Processing message", "payload", string(msg.Payload))
		}

		// Explicit batch acknowledgment
		if err := mctx.Ack(); err != nil {
			log.Error("Failed to acknowledge batch", "consumerID", consumerID, "error", err)
		} else {
			log.Info("<<< Batch acknowledged successfully", "consumerID", consumerID, "count", len(msgs))
		}
	}
}
