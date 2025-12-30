package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisStorage 基于 Redis/DragonflyDB 的存储实现
type RedisStorage struct {
	client *redis.Client
}

func NewRedisStorage(addr string, password string, db int) (*RedisStorage, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("连接 DragonflyDB 失败: %w", err)
	}

	return &RedisStorage{client: client}, nil
}

// --- 消息操作 (使用 Redis Streams) ---

func (r *RedisStorage) streamKey(topic string, partitionID int32) string {
	return fmt.Sprintf("gmq:stream:%s:%d", topic, partitionID)
}

func (r *RedisStorage) WriteMessage(ctx context.Context, msg *Message) (int64, error) {
	key := r.streamKey(msg.Topic, msg.PartitionID)
	
	// 获取 TTL 配置
	ttlKey := fmt.Sprintf("gmq:meta:ttl:%s", msg.Topic)
	ttlVal, _ := r.client.Get(ctx, ttlKey).Float64()
	if ttlVal > 0 {
		msg.ExpiresAt = time.Now().Add(time.Duration(ttlVal) * time.Second).Unix()
	}

	data, _ := json.Marshal(msg)

	args := &redis.XAddArgs{
		Stream: key,
		Values: map[string]interface{}{"data": data},
	}

	// 如果有 TTL，可以考虑限制 Stream 长度（简单粗暴但有效）
	// 或者在读取时过滤。Redis Stream 暂不支持原生消息级 TTL
	// 这里我们主要依赖读取时的逻辑过滤
	
	res, err := r.client.XAdd(ctx, args).Result()
	if err != nil {
		return 0, err
	}

	parts := strings.Split(res, "-")
	offset, _ := strconv.ParseInt(parts[0], 10, 64)
	return offset, nil
}

func (r *RedisStorage) ReadMessages(ctx context.Context, topic string, partitionID int32, offset int64, limit int) ([]*Message, error) {
	key := r.streamKey(topic, partitionID)
	
	// 使用 XRANGE 读取消息
	start := strconv.FormatInt(offset, 10)
	if offset == 0 {
		start = "-"
	}

	res, err := r.client.XRangeN(ctx, key, start, "+", int64(limit)).Result()
	if err != nil {
		return nil, err
	}

	var messages []*Message
	for _, xmsg := range res {
		var msg Message
		if data, ok := xmsg.Values["data"].(string); ok {
			json.Unmarshal([]byte(data), &msg)
			// 更新真实的 Stream ID 作为 Offset
			parts := strings.Split(xmsg.ID, "-")
			msg.Offset, _ = strconv.ParseInt(parts[0], 10, 64)
			messages = append(messages, &msg)
		}
	}
	return messages, nil
}

func (r *RedisStorage) CreatePartition(ctx context.Context, topic string, partitionID int32) error {
	// Redis Stream 在第一次 XADD 时自动创建，这里可以做一些元数据记录
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

// --- 偏移量管理 ---

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

// --- TTL 管理 ---

func (r *RedisStorage) SetTTL(ctx context.Context, topic string, ttl time.Duration) error {
	// Redis Stream 的过期通常通过 XADD 的 MAXLEN 实现，或者设置 Key 的 EXPIRE
	// 这里记录元数据，实际清理可以在写入时通过 MAXLEN 自动完成
	key := fmt.Sprintf("gmq:meta:ttl:%s", topic)
	return r.client.Set(ctx, key, ttl.Seconds(), 0).Err()
}

// --- 状态管理 ---

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
	
	// 转换 map 格式
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
