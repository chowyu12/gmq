package storage

import (
	"context"
	"time"
)

// Message structure (internal use)
type Message struct {
	ID             string            `json:"id"`
	Topic          string            `json:"topic"`
	PartitionID    int32             `json:"partition_id"`
	Offset         int64             `json:"offset"`
	Payload        []byte            `json:"payload"`
	Properties     map[string]string `json:"properties"`
	Timestamp      int64             `json:"timestamp"`
	ExpiresAt      int64             `json:"expires_at,omitempty"`
	QoS            int32             `json:"qos"`
	ProducerID     string            `json:"producer_id,omitempty"`
	SequenceNumber int64             `json:"sequence_number,omitempty"`
	Key            string            `json:"key,omitempty"`
}

// Partition structure
type Partition struct {
	ID            int32     `json:"id"`
	Topic         string    `json:"topic"`
	NextOffset    int64     `json:"next_offset"`
	MessageCount  int64     `json:"message_count"`
	CreatedAt     time.Time `json:"created_at"`
	LastWriteTime time.Time `json:"last_write_time"`
}

// ConsumerState represents consumer state
type ConsumerState struct {
	ConsumerId         string  `json:"id"`
	ConsumerGroup      string  `json:"consumer_group"`
	Topic              string  `json:"topic"`
	AssignedPartitions []int32 `json:"assigned_partitions"`
	LastHeartbeat      int64   `json:"last_heartbeat"`
}

// Storage unified storage interface
type Storage interface {
	// --- Message operations ---
	WriteMessages(ctx context.Context, msgs []*Message) ([]int64, error)
	ReadMessages(ctx context.Context, topic string, partitionID int32, offset int64, limit int) ([]*Message, error)
	CreatePartition(ctx context.Context, topic string, partitionID int32) error
	GetPartition(ctx context.Context, topic string, partitionID int32) (*Partition, error)
	ListPartitions(ctx context.Context, topic string) ([]*Partition, error)
	
	// --- Offset management ---
	UpdateOffset(ctx context.Context, consumerGroup, topic string, partitionID int32, offset int64) error
	GetOffset(ctx context.Context, consumerGroup, topic string, partitionID int32) (int64, error)
	FetchMessages(ctx context.Context, consumerGroup, topic string, partitionID int32, limit int) ([]*Message, error)
	
	// --- TTL management ---
	SetTTL(ctx context.Context, topic string, ttl time.Duration) error
	
	// --- State management ---
	SaveConsumer(ctx context.Context, state *ConsumerState) error
	GetConsumers(ctx context.Context, group, topic string) ([]*ConsumerState, error)
	DeleteConsumer(ctx context.Context, id, group, topic string) error
	UpdateAssignment(ctx context.Context, group, topic string, assignment map[int32]string) error
	GetAssignment(ctx context.Context, group, topic string) (map[int32]string, error)

	Close() error
}
