package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// FileStorage 基于文件系统的存储实现
type FileStorage struct {
	baseDir    string
	partitions map[string]*partitionStorage
	mu         sync.RWMutex
	offsets    map[string]int64 // key: consumerGroup:topic:partition
	offsetMu   sync.RWMutex
	ttls       map[string]time.Duration // Topic -> TTL
	ttlMu      sync.RWMutex
	stateStore *SQLStateStorage // 状态存储
}

type partitionStorage struct {
	topic       string
	partitionID int32
	file        *os.File
	offset      int64
	mu          sync.Mutex
}

// NewFileStorage 创建文件存储
func NewFileStorage(baseDir string) (*FileStorage, error) {
	if err := os.MkdirAll(baseDir, 0755); err != nil {
		return nil, fmt.Errorf("创建存储目录失败: %w", err)
	}

	// 初始化 SQLite 状态存储
	stateDbPath := filepath.Join(baseDir, "state.db")
	stateStore, err := NewSQLStateStorage(stateDbPath)
	if err != nil {
		return nil, fmt.Errorf("初始化状态存储失败: %w", err)
	}

	fs := &FileStorage{
		baseDir:    baseDir,
		partitions: make(map[string]*partitionStorage),
		offsets:    make(map[string]int64),
		ttls:       make(map[string]time.Duration),
		stateStore: stateStore,
	}

	// 加载已有分区
	if err := fs.loadPartitions(); err != nil {
		return nil, fmt.Errorf("加载分区失败: %w", err)
	}

	// 启动清理协程
	go fs.gcLoop()

	return fs, nil
}

func (fs *FileStorage) loadPartitions() error {
	return filepath.Walk(fs.baseDir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return err
		}

		if filepath.Ext(path) == ".log" {
			// 解析文件名获取 topic 和 partition
			// 文件名格式: topic_partition_0.log
			rel, _ := filepath.Rel(fs.baseDir, path)
			dir := filepath.Dir(rel)
			
			var topic string
			var partitionID int32
			_, err := fmt.Sscanf(filepath.Base(path), "partition_%d.log", &partitionID)
			if err != nil {
				return nil
			}
			topic = dir

			key := fs.partitionKey(topic, partitionID)
			file, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND, 0644)
			if err != nil {
				return err
			}

			// 计算当前偏移量
			stat, _ := file.Stat()
			offset := fs.calculateOffset(file)

			fs.partitions[key] = &partitionStorage{
				topic:       topic,
				partitionID: partitionID,
				file:        file,
				offset:      offset,
			}

			fmt.Printf("加载分区: topic=%s, partition=%d, offset=%d, size=%d\n", 
				topic, partitionID, offset, stat.Size())
		}
		return nil
	})
}

func (fs *FileStorage) calculateOffset(file *os.File) int64 {
	var offset int64
	file.Seek(0, io.SeekStart)
	decoder := json.NewDecoder(file)
	
	for {
		var msg Message
		if err := decoder.Decode(&msg); err != nil {
			break
		}
		offset++
	}
	
	file.Seek(0, io.SeekEnd)
	return offset
}

func (fs *FileStorage) partitionKey(topic string, partitionID int32) string {
	return fmt.Sprintf("%s:%d", topic, partitionID)
}

func (fs *FileStorage) WriteMessage(ctx context.Context, msg *Message) (int64, error) {
	key := fs.partitionKey(msg.Topic, msg.PartitionID)

	fs.mu.RLock()
	ps, exists := fs.partitions[key]
	fs.mu.RUnlock()

	if !exists {
		if err := fs.CreatePartition(ctx, msg.Topic, msg.PartitionID); err != nil {
			return 0, err
		}
		fs.mu.RLock()
		ps = fs.partitions[key]
		fs.mu.RUnlock()
	}

	ps.mu.Lock()
	defer ps.mu.Unlock()

	// 设置过期时间
	fs.ttlMu.RLock()
	if ttl, ok := fs.ttls[msg.Topic]; ok {
		msg.ExpiresAt = time.Now().Add(ttl).Unix()
	}
	fs.ttlMu.RUnlock()

	// 设置偏移量
	msg.Offset = ps.offset
	ps.offset++

	// 序列化并写入
	data, err := json.Marshal(msg)
	if err != nil {
		return 0, fmt.Errorf("序列化消息失败: %w", err)
	}

	data = append(data, '\n')
	if _, err := ps.file.Write(data); err != nil {
		return 0, fmt.Errorf("写入消息失败: %w", err)
	}

	// 立即刷盘
	if err := ps.file.Sync(); err != nil {
		return 0, fmt.Errorf("刷盘失败: %w", err)
	}

	return msg.Offset, nil
}

func (fs *FileStorage) ReadMessages(ctx context.Context, topic string, partitionID int32, offset int64, limit int) ([]*Message, error) {
	key := fs.partitionKey(topic, partitionID)
	
	fs.mu.RLock()
	ps, exists := fs.partitions[key]
	fs.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("分区不存在: topic=%s, partition=%d", topic, partitionID)
	}

	ps.mu.Lock()
	defer ps.mu.Unlock()

	// 从头读取文件
	if _, err := ps.file.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}

	var messages []*Message
	decoder := json.NewDecoder(ps.file)
	currentOffset := int64(0)

	for {
		var msg Message
		if err := decoder.Decode(&msg); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		// 过滤过期消息
		if msg.ExpiresAt > 0 && time.Now().Unix() > msg.ExpiresAt {
			currentOffset++
			continue
		}

		if currentOffset >= offset {
			messages = append(messages, &msg)
			if len(messages) >= limit {
				break
			}
		}
		currentOffset++
	}

	// 恢复文件位置
	ps.file.Seek(0, io.SeekEnd)

	return messages, nil
}

func (fs *FileStorage) GetPartition(ctx context.Context, topic string, partitionID int32) (*Partition, error) {
	key := fs.partitionKey(topic, partitionID)
	
	fs.mu.RLock()
	ps, exists := fs.partitions[key]
	fs.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("分区不存在: topic=%s, partition=%d", topic, partitionID)
	}

	ps.mu.Lock()
	defer ps.mu.Unlock()

	return &Partition{
		ID:         partitionID,
		Topic:      topic,
		NextOffset: ps.offset,
	}, nil
}

func (fs *FileStorage) CreatePartition(ctx context.Context, topic string, partitionID int32) error {
	key := fs.partitionKey(topic, partitionID)
	
	fs.mu.Lock()
	defer fs.mu.Unlock()

	if _, exists := fs.partitions[key]; exists {
		return nil // 已存在
	}

	// 创建目录
	topicDir := filepath.Join(fs.baseDir, topic)
	if err := os.MkdirAll(topicDir, 0755); err != nil {
		return fmt.Errorf("创建 topic 目录失败: %w", err)
	}

	// 创建分区文件
	filePath := filepath.Join(topicDir, fmt.Sprintf("partition_%d.log", partitionID))
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("创建分区文件失败: %w", err)
	}

	fs.partitions[key] = &partitionStorage{
		topic:       topic,
		partitionID: partitionID,
		file:        file,
		offset:      0,
	}

	fmt.Printf("创建分区: topic=%s, partition=%d\n", topic, partitionID)
	return nil
}

func (fs *FileStorage) ListPartitions(ctx context.Context, topic string) ([]*Partition, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	var partitions []*Partition
	prefix := topic + ":"
	for key, ps := range fs.partitions {
		if topic == "" || (len(key) > len(prefix) && key[:len(prefix)] == prefix) {
			ps.mu.Lock()
			partitions = append(partitions, &Partition{
				ID:         ps.partitionID,
				Topic:      ps.topic,
				NextOffset: ps.offset,
			})
			ps.mu.Unlock()
		}
	}
	return partitions, nil
}

func (fs *FileStorage) offsetKey(consumerGroup, topic string, partitionID int32) string {
	return fmt.Sprintf("%s:%s:%d", consumerGroup, topic, partitionID)
}

func (fs *FileStorage) UpdateOffset(ctx context.Context, consumerGroup, topic string, partitionID int32, offset int64) error {
	key := fs.offsetKey(consumerGroup, topic, partitionID)
	
	fs.offsetMu.Lock()
	defer fs.offsetMu.Unlock()
	
	fs.offsets[key] = offset
	
	// 持久化偏移量到文件
	offsetDir := filepath.Join(fs.baseDir, "_offsets", consumerGroup, topic)
	if err := os.MkdirAll(offsetDir, 0755); err != nil {
		return err
	}
	
	offsetFile := filepath.Join(offsetDir, fmt.Sprintf("partition_%d.offset", partitionID))
	return os.WriteFile(offsetFile, []byte(fmt.Sprintf("%d", offset)), 0644)
}

func (fs *FileStorage) GetOffset(ctx context.Context, consumerGroup, topic string, partitionID int32) (int64, error) {
	key := fs.offsetKey(consumerGroup, topic, partitionID)
	
	fs.offsetMu.RLock()
	offset, exists := fs.offsets[key]
	fs.offsetMu.RUnlock()
	
	if exists {
		return offset, nil
	}
	
	// 从文件加载
	offsetFile := filepath.Join(fs.baseDir, "_offsets", consumerGroup, topic, 
		fmt.Sprintf("partition_%d.offset", partitionID))
	
	data, err := os.ReadFile(offsetFile)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil // 默认从头开始
		}
		return 0, err
	}
	
	var loadedOffset int64
	fmt.Sscanf(string(data), "%d", &loadedOffset)
	
	fs.offsetMu.Lock()
	fs.offsets[key] = loadedOffset
	fs.offsetMu.Unlock()
	
	return loadedOffset, nil
}

func (fs *FileStorage) SetTTL(ctx context.Context, topic string, ttl time.Duration) error {
	fs.ttlMu.Lock()
	defer fs.ttlMu.Unlock()
	fs.ttls[topic] = ttl
	fmt.Printf("设置 Topic TTL: topic=%s, ttl=%v\n", topic, ttl)
	return nil
}

// --- 状态管理方法实现 ---

func (fs *FileStorage) SaveConsumer(ctx context.Context, state *ConsumerState) error {
	return fs.stateStore.SaveConsumer(ctx, state.ConsumerId, state.ConsumerGroup, state.Topic, state.AssignedPartitions, state.LastHeartbeat)
}

func (fs *FileStorage) GetConsumers(ctx context.Context, group, topic string) ([]*ConsumerState, error) {
	results, err := fs.stateStore.GetConsumers(ctx, group, topic)
	if err != nil {
		return nil, err
	}

	states := make([]*ConsumerState, len(results))
	for i, r := range results {
		states[i] = &ConsumerState{
			ConsumerId:         r["id"].(string),
			ConsumerGroup:      r["consumer_group"].(string),
			Topic:              r["topic"].(string),
			AssignedPartitions: r["assigned_partitions"].([]int32),
			LastHeartbeat:      r["last_heartbeat"].(int64),
		}
	}
	return states, nil
}

func (fs *FileStorage) DeleteConsumer(ctx context.Context, id, group, topic string) error {
	return fs.stateStore.DeleteConsumer(ctx, id, group, topic)
}

func (fs *FileStorage) UpdateAssignment(ctx context.Context, group, topic string, assignment map[int32]string) error {
	return fs.stateStore.UpdateAssignment(ctx, group, topic, assignment)
}

func (fs *FileStorage) GetAssignment(ctx context.Context, group, topic string) (map[int32]string, error) {
	return fs.stateStore.GetAssignment(ctx, group, topic)
}

// gcLoop 定期清理过期消息
func (fs *FileStorage) gcLoop() {
	ticker := time.NewTicker(1 * time.Hour)
	for range ticker.C {
		fs.performGC()
	}
}

func (fs *FileStorage) performGC() {
	fs.mu.RLock()
	partitions := make([]*partitionStorage, 0, len(fs.partitions))
	for _, ps := range fs.partitions {
		partitions = append(partitions, ps)
	}
	fs.mu.RUnlock()

	for _, ps := range partitions {
		fs.gcPartition(ps)
	}
}

func (fs *FileStorage) gcPartition(ps *partitionStorage) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	tempPath := ps.file.Name() + ".gc"
	tempFile, err := os.OpenFile(tempPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return
	}
	defer tempFile.Close()

	ps.file.Seek(0, io.SeekStart)
	decoder := json.NewDecoder(ps.file)
	writer := json.NewEncoder(tempFile)
	
	now := time.Now().Unix()
	
	for {
		var msg Message
		if err := decoder.Decode(&msg); err != nil {
			break
		}
		
		if msg.ExpiresAt == 0 || msg.ExpiresAt > now {
			writer.Encode(msg)
		}
	}

	oldPath := ps.file.Name()
	ps.file.Close()
	os.Remove(oldPath)
	os.Rename(tempPath, oldPath)
	
	newFile, _ := os.OpenFile(oldPath, os.O_RDWR|os.O_APPEND, 0644)
	ps.file = newFile
}

func (fs *FileStorage) Close() error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	if fs.stateStore != nil {
		fs.stateStore.Close()
	}

	for _, ps := range fs.partitions {
		if err := ps.file.Close(); err != nil {
			return err
		}
	}

	return nil
}
