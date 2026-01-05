package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/chowyu12/gmq/pkg/log"
	"github.com/redis/go-redis/v9"
)

// RedisStorage implements storage using Redis/DragonflyDB
type RedisStorage struct {
	client *redis.Client
}

func NewRedisStorage(addr string, password string, db int) (*RedisStorage, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
		// Force RESP2 protocol because DragonflyDB does not support some RESP3
		// maintenance subcommands (like CLIENT MAINT_NOTIFICATIONS) yet.
		Protocol: 2,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to DragonflyDB: %w", err)
	}

	return &RedisStorage{client: client}, nil
}

// --- Message operations (using Redis Streams) ---

func (r *RedisStorage) streamKey(topic string, partitionID int32) string {
	return fmt.Sprintf("gmq:stream:%s:%d", topic, partitionID)
}

// Helper function: encode Redis ID string (timestamp-seq) to int64
func encodeOffset(id string) int64 {
	parts := strings.Split(id, "-")
	if len(parts) != 2 {
		return 0
	}
	ts, _ := strconv.ParseInt(parts[0], 10, 64)
	seq, _ := strconv.ParseInt(parts[1], 10, 64)
	// High 44 bits: millisecond timestamp, low 20 bits: sequence number
	// This ensures offset is monotonically increasing and preserves Redis ID information
	return (ts << 20) | (seq & 0xFFFFF)
}

// Helper function: decode int64 offset back to Redis ID string
func decodeOffset(offset int64) string {
	if offset <= 0 {
		return "-"
	}
	ts := offset >> 20
	seq := offset & 0xFFFFF
	return fmt.Sprintf("%d-%d", ts, seq)
}

func (r *RedisStorage) WriteMessages(ctx context.Context, msgs []*Message) ([]int64, error) {
	if len(msgs) == 0 {
		return nil, nil
	}

	// 1. Get TTL configuration for all topics (batch optimization)
	// This logic can be further optimized, currently using simple loop or cache
	topicTTLs := make(map[string]float64)
	for _, msg := range msgs {
		if _, ok := topicTTLs[msg.Topic]; !ok {
			ttlKey := fmt.Sprintf("gmq:meta:ttl:%s", msg.Topic)
			ttlVal, _ := r.client.Get(ctx, ttlKey).Float64()
			topicTTLs[msg.Topic] = ttlVal
		}
	}

	// 2. Prepare Pipeline
	pipe := r.client.Pipeline()
	results := make([]*redis.StringCmd, len(msgs))

	for i, msg := range msgs {
		// Idempotency check (SetNX in batch operations is complex, still processing per message)
		if msg.ProducerID != "" && msg.SequenceNumber > 0 {
			dedupKey := fmt.Sprintf("gmq:dedup:%s:%s:%d", msg.Topic, msg.ProducerID, msg.SequenceNumber)
			// Note: SetNX in Pipeline cannot immediately determine result to skip subsequent steps
			// Strict implementation requires Lua script or two-phase commit, simplified here assuming all messages in Pipeline are new
			pipe.SetNX(ctx, dedupKey, "1", 1*time.Hour)
		}

		if ttl := topicTTLs[msg.Topic]; ttl > 0 {
			msg.ExpiresAt = time.Now().Add(time.Duration(ttl) * time.Second).Unix()
		}

		key := r.streamKey(msg.Topic, msg.PartitionID)
		data, _ := json.Marshal(msg)
		args := &redis.XAddArgs{
			Stream: key,
			Values: map[string]interface{}{"data": data},
			// Automatically prune stream to prevent OOM.
			// Keep last 1,000,000 messages per partition to support large benchmark runs.
			// Approx: true makes it much more efficient.
			MaxLen: 1000000,
			Approx: true,
		}
		results[i] = pipe.XAdd(ctx, args)
	}

	// Execute Pipeline
	_, err := pipe.Exec(ctx)
	if err != nil && err != redis.Nil {
		return nil, err
	}

	// 3. Parse results
	offsets := make([]int64, len(msgs))
	for i, resCmd := range results {
		res, err := resCmd.Result()
		if err != nil {
			log.WithContext(ctx).Error("Failed to write message item in batch", "index", i, "error", err)
			continue
		}
		offsets[i] = encodeOffset(res)
	}

	return offsets, nil
}

// FetchMessages reads messages using XREADGROUP command.
// It automatically manages consumer group creation and utilizes Redis kernel for consumption progress.
func (r *RedisStorage) FetchMessages(ctx context.Context, group, topic string, consumerID string, partitionID int32, limit int) ([]*Message, error) {
	// Limit batch size to prevent excessive memory usage
	const maxBatchSize = 500
	if limit > maxBatchSize {
		limit = maxBatchSize
	}
	if limit <= 0 {
		limit = 100
	}

	key := r.streamKey(topic, partitionID)

	// 1. Try to create Consumer Group if it doesn't exist
	// MKSTREAM parameter ensures an empty stream is created if it doesn't exist
	// "0" indicates consuming from the beginning for new groups
	err := r.client.XGroupCreateMkStream(ctx, key, group, "0").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return nil, fmt.Errorf("failed to create consumer group: %w", err)
	}

	// 2. Read messages using XREADGROUP
	// ">" indicates reading messages never delivered to other consumers
	streams, err := r.client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    group,
		Consumer: consumerID,
		Streams:  []string{key, ">"},
		Count:    int64(limit),
		Block:    0, // Non-blocking read
	}).Result()

	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	var messages []*Message
	if len(streams) > 0 {
		for _, xmsg := range streams[0].Messages {
			var msg Message
			if data, ok := xmsg.Values["data"].(string); ok {
				if err := json.Unmarshal([]byte(data), &msg); err != nil {
					log.WithContext(ctx).Error("Failed to unmarshal message data", "id", xmsg.ID, "error", err)
					continue
				}
				// Use encoded full ID as Offset returned to client
				msg.Offset = encodeOffset(xmsg.ID)
				messages = append(messages, &msg)
			}
		}
	}

	return messages, nil
}

// AcknowledgeMessages acknowledges processed messages and removes them from PEL (Pending Entries List).
func (r *RedisStorage) AcknowledgeMessages(ctx context.Context, group, topic string, partitionID int32, offsets []int64) (int64, error) {
	if len(offsets) == 0 {
		return 0, nil
	}

	key := r.streamKey(topic, partitionID)
	ids := make([]string, len(offsets))
	for i, offset := range offsets {
		ids[i] = decodeOffset(offset)
	}

	// Execute XACK
	count, err := r.client.XAck(ctx, key, group, ids...).Result()
	if err != nil {
		return 0, err
	}

	return count, nil
}

// --- TTL management ---

func (r *RedisStorage) SetTTL(ctx context.Context, topic string, ttl time.Duration) error {
	// Redis Stream expiration is usually implemented via XADD MAXLEN or setting Key EXPIRE
	// Here we record metadata, actual cleanup can be done automatically via MAXLEN during writes
	key := fmt.Sprintf("gmq:meta:ttl:%s", topic)
	return r.client.Set(ctx, key, ttl.Seconds(), 0).Err()
}

func (r *RedisStorage) CreatePartition(ctx context.Context, topic string, partitionID int32) error {
	// Redis Stream is automatically created on first XADD, here we can do some metadata recording
	key := fmt.Sprintf("gmq:meta:partitions:%s", topic)
	return r.client.SAdd(ctx, key, partitionID).Err()
}

func (r *RedisStorage) ListPartitions(ctx context.Context, topic string) ([]*Partition, error) {
	key := fmt.Sprintf("gmq:meta:partitions:%s", topic)
	pids, err := r.client.SMembers(ctx, key).Result()
	if err != nil {
		return nil, err
	}

	var partitions []*Partition
	for _, pidStr := range pids {
		pid, _ := strconv.ParseInt(pidStr, 10, 32)
		partitions = append(partitions, &Partition{
			ID:    int32(pid),
			Topic: topic,
		})
	}
	return partitions, nil
}

func (r *RedisStorage) GetPartition(ctx context.Context, topic string, partitionID int32) (*Partition, error) {
	key := r.streamKey(topic, partitionID)
	info, err := r.client.XInfoStream(ctx, key).Result()
	if err != nil {
		return nil, err
	}

	return &Partition{
		ID:           partitionID,
		Topic:        topic,
		MessageCount: info.Length,
	}, nil
}

// --- State management ---

func (r *RedisStorage) SaveConsumer(ctx context.Context, state *ConsumerState) error {
	key := fmt.Sprintf("gmq:state:consumers:%s:%s", state.ConsumerGroup, state.Topic)
	data, _ := json.Marshal(state)
	return r.client.HSet(ctx, key, state.ConsumerId, data).Err()
}

func (r *RedisStorage) GetConsumers(ctx context.Context, group, topic string) ([]*ConsumerState, error) {
	key := fmt.Sprintf("gmq:state:consumers:%s:%s", group, topic)
	vals, err := r.client.HGetAll(ctx, key).Result()
	if err != nil {
		return nil, err
	}

	var states []*ConsumerState
	for _, val := range vals {
		var state ConsumerState
		json.Unmarshal([]byte(val), &state)
		states = append(states, &state)
	}
	return states, nil
}

func (r *RedisStorage) DeleteConsumer(ctx context.Context, id, group, topic string) error {
	key := fmt.Sprintf("gmq:state:consumers:%s:%s", group, topic)
	return r.client.HDel(ctx, key, id).Err()
}

func (r *RedisStorage) UpdateAssignment(ctx context.Context, group, topic string, assignment map[int32]string) error {
	key := fmt.Sprintf("gmq:state:assign:%s:%s", group, topic)

	// Convert map format
	fields := make(map[string]interface{})
	for k, v := range assignment {
		fields[strconv.Itoa(int(k))] = v
	}

	return r.client.HMSet(ctx, key, fields).Err()
}

func (r *RedisStorage) GetAssignment(ctx context.Context, group, topic string) (map[int32]string, error) {
	key := fmt.Sprintf("gmq:state:assign:%s:%s", group, topic)
	vals, err := r.client.HGetAll(ctx, key).Result()
	if err != nil {
		return nil, err
	}

	assignment := make(map[int32]string)
	for k, v := range vals {
		pid, _ := strconv.Atoi(k)
		assignment[int32(pid)] = v
	}
	return assignment, nil
}

func (r *RedisStorage) Close() error {
	return r.client.Close()
}
