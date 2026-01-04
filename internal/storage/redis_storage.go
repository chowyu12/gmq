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

func (r *RedisStorage) ReadMessages(ctx context.Context, topic string, partitionID int32, offset int64, limit int) ([]*Message, error) {
	key := r.streamKey(topic, partitionID)

	// Redis Stream ID format is "timestamp-sequence"
	// If offset == 0, we start reading from the smallest ID "-"
	// If offset > 0, we need to read after that ID, so use "(" + ID syntax
	startID := decodeOffset(offset)
	if offset > 0 {
		startID = "(" + startID // Exclude current offset, read next message
	}

	res, err := r.client.XRangeN(ctx, key, startID, "+", int64(limit)).Result()
	if err != nil {
		return nil, err
	}

	var messages []*Message
	for _, xmsg := range res {
		var msg Message
		if data, ok := xmsg.Values["data"].(string); ok {
			json.Unmarshal([]byte(data), &msg)
			// Use encoded full ID as Offset returned to client
			msg.Offset = encodeOffset(xmsg.ID)
			messages = append(messages, &msg)
		}
	}
	return messages, nil
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

// --- Offset management ---

func (r *RedisStorage) UpdateOffset(ctx context.Context, group, topic string, partitionID int32, offset int64) error {
	key := fmt.Sprintf("gmq:offsets:%s:%s", group, topic)
	return r.client.HSet(ctx, key, strconv.Itoa(int(partitionID)), offset).Err()
}

func (r *RedisStorage) GetOffset(ctx context.Context, group, topic string, partitionID int32) (int64, error) {
	key := fmt.Sprintf("gmq:offsets:%s:%s", group, topic)
	val, err := r.client.HGet(ctx, key, strconv.Itoa(int(partitionID))).Result()
	if err == redis.Nil {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(val, 10, 64)
}

// FetchMessages atomically reads and updates offset, then reads messages.
// It uses a distributed lock to prevent concurrent pulls on the same partition,
// eliminating race conditions and avoiding Lua memory limits.
func (r *RedisStorage) FetchMessages(ctx context.Context, group, topic string, partitionID int32, limit int) ([]*Message, error) {
	// Limit batch size to prevent excessive memory usage in Go layer
	const maxBatchSize = 500
	if limit > maxBatchSize {
		limit = maxBatchSize
	}
	if limit <= 0 {
		limit = 100
	}

	// 1. Acquire distributed lock for this specific partition pull
	// This prevents multiple brokers from pulling the same messages simultaneously.
	lockKey := fmt.Sprintf("gmq:lock:fetch:%s:%s:%d", group, topic, partitionID)
	// Lock timeout 5s is plenty for a single batch pull
	ok, err := r.client.SetNX(ctx, lockKey, "1", 5*time.Second).Result()
	if err != nil {
		return nil, err
	}
	if !ok {
		// If locked, another broker is currently fetching for this partition.
		// Return empty to allow the caller to retry or back off.
		return nil, nil
	}
	// Ensure lock is released regardless of success or failure
	defer r.client.Del(ctx, lockKey)

	// 2. Get the current offset from storage
	currentOffset, err := r.GetOffset(ctx, group, topic, partitionID)
	if err != nil {
		return nil, err
	}

	// 3. Read messages starting from currentOffset + 1
	msgs, err := r.ReadMessages(ctx, topic, partitionID, currentOffset, limit)
	if err != nil {
		return nil, err
	}

	// 4. If messages were found, update the offset immediately
	if len(msgs) > 0 {
		lastOffset := msgs[len(msgs)-1].Offset
		if err := r.UpdateOffset(ctx, group, topic, partitionID, lastOffset); err != nil {
			// Log error but still return messages; next pull will retry from old offset (at-least-once)
			log.WithContext(ctx).Error("Failed to update offset after fetch", "group", group, "topic", topic, "error", err)
		}
	}

	return msgs, nil
}

// --- TTL management ---

func (r *RedisStorage) SetTTL(ctx context.Context, topic string, ttl time.Duration) error {
	// Redis Stream expiration is usually implemented via XADD MAXLEN or setting Key EXPIRE
	// Here we record metadata, actual cleanup can be done automatically via MAXLEN during writes
	key := fmt.Sprintf("gmq:meta:ttl:%s", topic)
	return r.client.Set(ctx, key, ttl.Seconds(), 0).Err()
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
