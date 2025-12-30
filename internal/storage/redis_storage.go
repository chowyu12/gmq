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

// 辅助函数：将 Redis ID 字符串 (timestamp-seq) 编码为 int64
func encodeOffset(id string) int64 {
	parts := strings.Split(id, "-")
	if len(parts) != 2 {
		return 0
	}
	ts, _ := strconv.ParseInt(parts[0], 10, 64)
	seq, _ := strconv.ParseInt(parts[1], 10, 64)
	// 高 44 位毫秒级时间戳，低 20 位序列号
	// 这样可以确保 offset 是单调递增且不丢失 Redis ID 信息的
	return (ts << 20) | (seq & 0xFFFFF)
}

// 辅助函数：将 int64 编码的 Offset 还原为 Redis ID 字符串
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

	// 1. 获取所有 Topic 的 TTL 配置 (批量获取优化)
	// 这里的获取逻辑可以进一步优化，目前先简单循环或缓存
	topicTTLs := make(map[string]float64)
	for _, msg := range msgs {
		if _, ok := topicTTLs[msg.Topic]; !ok {
			ttlKey := fmt.Sprintf("gmq:meta:ttl:%s", msg.Topic)
			ttlVal, _ := r.client.Get(ctx, ttlKey).Float64()
			topicTTLs[msg.Topic] = ttlVal
		}
	}

	// 2. 准备 Pipeline
	pipe := r.client.Pipeline()
	results := make([]*redis.StringCmd, len(msgs))

	for i, msg := range msgs {
		// 幂等性检查 (批量操作中 SetNX 比较复杂，这里仍然按条处理)
		if msg.ProducerID != "" && msg.SequenceNumber > 0 {
			dedupKey := fmt.Sprintf("gmq:dedup:%s:%s:%d", msg.Topic, msg.ProducerID, msg.SequenceNumber)
			// 注意：Pipeline 中的 SetNX 无法立即判断结果来跳过后续步骤
			// 严格实现需要 Lua 脚本或两阶段提交，这里简化处理，假设 Pipeline 中都是新消息
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
		}
		results[i] = pipe.XAdd(ctx, args)
	}

	// 执行 Pipeline
	_, err := pipe.Exec(ctx)
	if err != nil && err != redis.Nil {
		return nil, err
	}

	// 3. 解析结果
	offsets := make([]int64, len(msgs))
	for i, resCmd := range results {
		res, err := resCmd.Result()
		if err != nil {
			log.WithContext(ctx).Error("批量写入消息项失败", "index", i, "error", err)
			continue
		}
		offsets[i] = encodeOffset(res)
	}

	return offsets, nil
}

func (r *RedisStorage) ReadMessages(ctx context.Context, topic string, partitionID int32, offset int64, limit int) ([]*Message, error) {
	key := r.streamKey(topic, partitionID)
	
	// 使用高精度解码后的 ID 作为起点
	// 此时 offset 已经是下一条消息的预期起点
	start := decodeOffset(offset)

	res, err := r.client.XRangeN(ctx, key, start, "+", int64(limit)).Result()
	if err != nil {
		return nil, err
	}

	var messages []*Message
	for _, xmsg := range res {
		var msg Message
		if data, ok := xmsg.Values["data"].(string); ok {
			json.Unmarshal([]byte(data), &msg)
			// 使用编码后的完整 ID 作为 Offset 返回给客户端
			msg.Offset = encodeOffset(xmsg.ID)
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
	err := r.client.HSet(ctx, key, strconv.Itoa(int(partitionID)), offset).Err()
	if err != nil {
		return err
	}

	// 触发物理清理：删除该分区中所有已被所有消费组确认的消息
	return r.trimStream(ctx, topic, partitionID)
}

// trimStream 自动修剪 Stream，删除已被确认的消息
func (r *RedisStorage) trimStream(ctx context.Context, topic string, partitionID int32) error {
	streamKey := r.streamKey(topic, partitionID)

	// 1. 获取订阅该 Topic 的所有消费组
	// 注意：这里简化处理，在实际生产中可能需要一个专门的元数据 Key 来记录所有消费组
	// 这里我们通过遍历 offset 相关的 keys 来模拟查找 (gmq:offsets:*)
	iter := r.client.Scan(ctx, 0, "gmq:offsets:*", 0).Iterator()
	var minOffset int64 = -1

	for iter.Next(ctx) {
		key := iter.Val() // 格式: gmq:offsets:group:topic
		if !strings.HasSuffix(key, ":"+topic) {
			continue
		}

		// 获取该组在该分区的 offset
		val, err := r.client.HGet(ctx, key, strconv.Itoa(int(partitionID))).Result()
		if err == nil {
			offset, _ := strconv.ParseInt(val, 10, 64)
			if minOffset == -1 || offset < minOffset {
				minOffset = offset
			}
		}
	}

	// 2. 如果找到了最小已确认 offset，执行修剪
	// Redis Stream ID 格式是 "timestamp-seq"，这里的 offset 对应的是 timestamp 部分
	if minOffset > 0 {
		trimID := fmt.Sprintf("%d-0", minOffset)
		log.WithContext(ctx).Debug("修剪消息流", "topic", topic, "partition", partitionID, "minID", trimID)
		return r.client.XTrimMinID(ctx, streamKey, trimID).Err()
	}

	return nil
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
