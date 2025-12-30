package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sync"

	_ "modernc.org/sqlite"
)

// SQLStateStorage 基于 SQLite 的状态存储实现
type SQLStateStorage struct {
	db *sql.DB
	mu sync.Mutex
}

// NewSQLStateStorage 创建一个新的状态存储
func NewSQLStateStorage(dbPath string) (*SQLStateStorage, error) {
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("打开数据库失败: %w", err)
	}

	// 初始化表结构
	queries := []string{
		`CREATE TABLE IF NOT EXISTS consumers (
			id TEXT PRIMARY KEY,
			consumer_group TEXT,
			topic TEXT,
			assigned_partitions TEXT,
			last_heartbeat INTEGER
		)`,
		`CREATE TABLE IF NOT EXISTS group_assignments (
			consumer_group TEXT,
			topic TEXT,
			partition_id INTEGER,
			consumer_id TEXT,
			PRIMARY KEY(consumer_group, topic, partition_id)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_group_topic ON consumers(consumer_group, topic)`,
	}

	for _, q := range queries {
		if _, err := db.Exec(q); err != nil {
			return nil, fmt.Errorf("执行初始化 SQL 失败: %w", err)
		}
	}

	return &SQLStateStorage{db: db}, nil
}

// SaveConsumer 保存或更新消费者信息
func (s *SQLStateStorage) SaveConsumer(ctx context.Context, id, group, topic string, partitions []int32, heartbeat int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	partsData, _ := json.Marshal(partitions)
	query := `INSERT OR REPLACE INTO consumers (id, consumer_group, topic, assigned_partitions, last_heartbeat) 
			  VALUES (?, ?, ?, ?, ?)`
	
	_, err := s.db.ExecContext(ctx, query, id, group, topic, string(partsData), heartbeat)
	return err
}

// GetConsumers 获取指定消费组和 topic 的所有消费者
func (s *SQLStateStorage) GetConsumers(ctx context.Context, group, topic string) ([]map[string]interface{}, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	query := `SELECT id, consumer_group, topic, assigned_partitions, last_heartbeat FROM consumers 
			  WHERE consumer_group = ? AND topic = ?`
	
	rows, err := s.db.QueryContext(ctx, query, group, topic)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var consumers []map[string]interface{}
	for rows.Next() {
		var id, g, t, partsStr string
		var heartbeat int64
		if err := rows.Scan(&id, &g, &t, &partsStr, &heartbeat); err != nil {
			return nil, err
		}

		var partitions []int32
		json.Unmarshal([]byte(partsStr), &partitions)

		consumers = append(consumers, map[string]interface{}{
			"id":                  id,
			"consumer_group":      g,
			"topic":               t,
			"assigned_partitions": partitions,
			"last_heartbeat":      heartbeat,
		})
	}
	return consumers, nil
}

// DeleteConsumer 删除消费者
func (s *SQLStateStorage) DeleteConsumer(ctx context.Context, id, group, topic string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	// 1. 删除消费者主表
	_, err = tx.ExecContext(ctx, "DELETE FROM consumers WHERE id = ?", id)
	if err != nil {
		tx.Rollback()
		return err
	}

	// 2. 清理分配表中的该消费者
	_, err = tx.ExecContext(ctx, "UPDATE group_assignments SET consumer_id = '' WHERE consumer_id = ?", id)
	if err != nil {
		tx.Rollback()
		return err
	}

	return tx.Commit()
}

// UpdateAssignment 更新分区分配关系
func (s *SQLStateStorage) UpdateAssignment(ctx context.Context, group, topic string, assignment map[int32]string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	query := `INSERT OR REPLACE INTO group_assignments (consumer_group, topic, partition_id, consumer_id) 
			  VALUES (?, ?, ?, ?)`
	
	for partID, consumerID := range assignment {
		if _, err := tx.ExecContext(ctx, query, group, topic, partID, consumerID); err != nil {
			tx.Rollback()
			return err
		}
	}

	return tx.Commit()
}

// GetAssignment 获取当前分配关系
func (s *SQLStateStorage) GetAssignment(ctx context.Context, group, topic string) (map[int32]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	query := `SELECT partition_id, consumer_id FROM group_assignments WHERE consumer_group = ? AND topic = ?`
	rows, err := s.db.QueryContext(ctx, query, group, topic)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	assignment := make(map[int32]string)
	for rows.Next() {
		var partID int32
		var consumerID string
		if err := rows.Scan(&partID, &consumerID); err != nil {
			return nil, err
		}
		assignment[partID] = consumerID
	}
	return assignment, nil
}

// Close 关闭数据库
func (s *SQLStateStorage) Close() error {
	return s.db.Close()
}
