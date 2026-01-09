package main

import (
	"context"
	"embed"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/chowyu12/gmq/pkg/log"
	pb "github.com/chowyu12/gmq/proto"
	storagepb "github.com/chowyu12/gmq/proto/storage"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

//go:embed templates/index.html
var templateFS embed.FS

var (
	addr        = flag.String("addr", ":8080", "Admin dashboard HTTP server address")
	brokerAddr  = flag.String("broker", "localhost:50051", "Broker service gRPC address")
	storageAddr = flag.String("storage", "localhost:50052", "Storage service gRPC address")
	redisAddr   = flag.String("redis-addr", "localhost:6379", "Redis address")
	redisPass   = flag.String("redis-pass", "", "Redis password")
	logLevel    = flag.String("log-level", "info", "Log level")
)

type AdminServer struct {
	brokerClient  pb.GMQServiceClient
	storageClient storagepb.StorageServiceClient
	redisClient   *redis.Client
}

func NewAdminServer(brokerAddr, storageAddr, redisAddr, redisPass string) (*AdminServer, error) {
	brokerConn, err := grpc.Dial(brokerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to broker: %w", err)
	}

	storageConn, err := grpc.Dial(storageAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		brokerConn.Close()
		return nil, fmt.Errorf("failed to connect to storage: %w", err)
	}

	redisClient := redis.NewClient(&redis.Options{
		Addr:     redisAddr,
		Password: redisPass,
		DB:       0,
		Protocol: 2,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := redisClient.Ping(ctx).Err(); err != nil {
		brokerConn.Close()
		storageConn.Close()
		return nil, fmt.Errorf("failed to connect to redis: %w", err)
	}

	return &AdminServer{
		brokerClient:  pb.NewGMQServiceClient(brokerConn),
		storageClient: storagepb.NewStorageServiceClient(storageConn),
		redisClient:   redisClient,
	}, nil
}

// API Response structures
type APIResponse struct {
	Success bool        `json:"success"`
	Data    interface{} `json:"data,omitempty"`
	Error   string      `json:"error,omitempty"`
}

type TopicInfo struct {
	Topic      string              `json:"topic"`
	Partitions []PartitionInfoResp `json:"partitions"`
}

type PartitionInfoResp struct {
	ID           int32 `json:"id"`
	MessageCount int64 `json:"message_count"`
	NextOffset   int64 `json:"next_offset"`
}

type ConsumerGroupInfo struct {
	Group      string                 `json:"group"`
	Topic      string                 `json:"topic"`
	Consumers  []ConsumerInfoResp     `json:"consumers"`
	Assignment map[string]interface{} `json:"assignment"`
}

type ConsumerInfoResp struct {
	ID                 string  `json:"id"`
	AssignedPartitions []int32 `json:"assigned_partitions"`
	LastHeartbeat      int64   `json:"last_heartbeat"`
}

type MessageResp struct {
	Offset      int64  `json:"offset"`
	Payload     string `json:"payload"`
	PayloadSize int    `json:"payload_size"`
	Timestamp   int64  `json:"timestamp"`
}

// HTTP Handlers
func (s *AdminServer) handleTopics(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		s.listTopics(w, r)
	} else if r.Method == http.MethodPost {
		s.createTopic(w, r)
	} else {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *AdminServer) listTopics(w http.ResponseWriter, r *http.Request) {
	ctx := context.Background()
	topicSet := make(map[string]bool)

	// 1. Scan explicit partition metadata keys (gmq:meta:partitions:{topic})
	iter1 := s.redisClient.Scan(ctx, 0, "gmq:meta:partitions:*", 0).Iterator()
	for iter1.Next(ctx) {
		key := iter1.Val()
		parts := strings.Split(key, ":")
		if len(parts) >= 4 {
			topic := strings.Join(parts[3:], ":")
			topicSet[topic] = true
		}
	}

	// 2. Scan actual Stream data keys (gmq:stream:{topic}:{partition})
	// This ensures topics automatically created by producers are also visible
	iter2 := s.redisClient.Scan(ctx, 0, "gmq:stream:*", 0).Iterator()
	for iter2.Next(ctx) {
		key := iter2.Val()
		parts := strings.Split(key, ":")
		if len(parts) >= 4 {
			// Key format is gmq:stream:topic:partitionid
			// Extract everything between 'stream' and the last part (partitionid)
			topic := strings.Join(parts[2:len(parts)-1], ":")
			topicSet[topic] = true
		}
	}

	if err := iter1.Err(); err != nil {
		json.NewEncoder(w).Encode(APIResponse{Success: false, Error: err.Error()})
		return
	}
	if err := iter2.Err(); err != nil {
		json.NewEncoder(w).Encode(APIResponse{Success: false, Error: err.Error()})
		return
	}

	var result []TopicInfo
	for topic := range topicSet {
		// Try to get partitions from storage first
		resp, err := s.storageClient.ListPartitions(ctx, &storagepb.ListPartitionsRequest{Topic: topic})

		var pids []int32
		if err == nil && resp.Success && len(resp.Partitions) > 0 {
			for _, p := range resp.Partitions {
				pids = append(pids, p.Id)
			}
		} else {
			// If storage doesn't know (auto-created), probe Redis directly for partitions
			pIter := s.redisClient.Scan(ctx, 0, fmt.Sprintf("gmq:stream:%s:*", topic), 0).Iterator()
			for pIter.Next(ctx) {
				pKey := pIter.Val()
				pParts := strings.Split(pKey, ":")
				if pid, err := strconv.Atoi(pParts[len(pParts)-1]); err == nil {
					pids = append(pids, int32(pid))
				}
			}
		}

		var partitions []PartitionInfoResp
		for _, pid := range pids {
			// Get detailed partition info from storage
			partResp, err := s.storageClient.GetPartition(ctx, &storagepb.GetPartitionRequest{
				Topic:       topic,
				PartitionId: pid,
			})

			if err == nil && partResp.Success {
				partitions = append(partitions, PartitionInfoResp{
					ID:           pid,
					MessageCount: partResp.Partition.MessageCount,
					NextOffset:   partResp.Partition.NextOffset,
				})
			} else {
				// Fallback if storage fails to provide info (e.g. key exists but metadata doesn't)
				partitions = append(partitions, PartitionInfoResp{
					ID: pid,
				})
			}
		}

		result = append(result, TopicInfo{
			Topic:      topic,
			Partitions: partitions,
		})
	}

	json.NewEncoder(w).Encode(APIResponse{Success: true, Data: result})
}

func (s *AdminServer) createTopic(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Topic      string `json:"topic"`
		Partitions int32  `json:"partitions"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		json.NewEncoder(w).Encode(APIResponse{Success: false, Error: err.Error()})
		return
	}

	if req.Partitions <= 0 {
		req.Partitions = 4
	}

	ctx := context.Background()
	for i := int32(0); i < req.Partitions; i++ {
		_, err := s.storageClient.CreatePartition(ctx, &storagepb.CreatePartitionRequest{
			Topic:       req.Topic,
			PartitionId: i,
		})
		if err != nil {
			json.NewEncoder(w).Encode(APIResponse{Success: false, Error: err.Error()})
			return
		}
	}

	json.NewEncoder(w).Encode(APIResponse{Success: true, Data: map[string]string{"topic": req.Topic}})
}

func (s *AdminServer) handleConsumerGroups(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	topic := r.URL.Query().Get("topic")
	if topic == "" {
		json.NewEncoder(w).Encode(APIResponse{Success: false, Error: "topic parameter required"})
		return
	}

	ctx := context.Background()

	// Scan Redis for all consumer groups for this topic
	// Pattern: gmq:state:consumers:{group}:{topic}
	pattern := fmt.Sprintf("gmq:state:consumers:*:%s", topic)
	iter := s.redisClient.Scan(ctx, 0, pattern, 0).Iterator()

	groupSet := make(map[string]bool)
	for iter.Next(ctx) {
		key := iter.Val()
		// Extract group name from key: gmq:state:consumers:{group}:{topic}
		parts := strings.Split(key, ":")
		if len(parts) >= 4 {
			group := parts[3]
			groupSet[group] = true
		}
	}
	if err := iter.Err(); err != nil {
		json.NewEncoder(w).Encode(APIResponse{Success: false, Error: err.Error()})
		return
	}

	var result []ConsumerGroupInfo
	for group := range groupSet {
		consumersResp, _ := s.storageClient.GetConsumers(ctx, &storagepb.GetConsumersRequest{
			ConsumerGroup: group,
			Topic:         topic,
		})

		assignmentResp, _ := s.storageClient.GetGroupAssignment(ctx, &storagepb.GetGroupAssignmentRequest{
			ConsumerGroup: group,
			Topic:         topic,
		})

		consumers := make([]ConsumerInfoResp, 0)
		if consumersResp != nil && consumersResp.Success {
			for _, c := range consumersResp.Consumers {
				consumers = append(consumers, ConsumerInfoResp{
					ID:                 c.ConsumerId,
					AssignedPartitions: c.AssignedPartitions,
					LastHeartbeat:      c.LastHeartbeat,
				})
			}
		}

		assignment := make(map[string]interface{})
		if assignmentResp != nil && assignmentResp.Success {
			for k, v := range assignmentResp.PartitionAssignment {
				assignment[strconv.Itoa(int(k))] = v
			}
		}

		// Calculate consumer lag for each partition
		partitionsResp, _ := s.storageClient.ListPartitions(ctx, &storagepb.ListPartitionsRequest{Topic: topic})
		if partitionsResp != nil && partitionsResp.Success {
			for _, p := range partitionsResp.Partitions {
				// Get pending messages count for this partition and group
				streamKey := fmt.Sprintf("gmq:stream:%s:%d", topic, p.Id)
				pending, _ := s.redisClient.XPendingExt(ctx, &redis.XPendingExtArgs{
					Stream: streamKey,
					Group:  group,
					Start:  "-",
					End:    "+",
					Count:  1000,
				}).Result()

				if len(pending) > 0 {
					lagKey := fmt.Sprintf("%s:%d", group, p.Id)
					assignment[lagKey+"_lag"] = len(pending)
				}
			}
		}

		result = append(result, ConsumerGroupInfo{
			Group:      group,
			Topic:      topic,
			Consumers:  consumers,
			Assignment: assignment,
		})
	}

	json.NewEncoder(w).Encode(APIResponse{Success: true, Data: result})
}

func (s *AdminServer) handleSendMessage(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Topic        string `json:"topic"`
		Payload      string `json:"payload"`
		PartitionID  int32  `json:"partition_id,omitempty"`
		PartitionKey string `json:"partition_key,omitempty"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		json.NewEncoder(w).Encode(APIResponse{Success: false, Error: err.Error()})
		return
	}

	// Create a producer connection to send message
	// For simplicity, we'll use a direct gRPC call
	// In production, you might want to maintain a connection pool

	ctx := context.Background()
	stream, err := s.brokerClient.Stream(ctx)
	if err != nil {
		json.NewEncoder(w).Encode(APIResponse{Success: false, Error: err.Error()})
		return
	}
	defer stream.CloseSend()

	// Send publish request
	pubReq := &pb.StreamMessage{
		Type: pb.MessageType_MESSAGE_TYPE_PUBLISH_REQUEST,
		Payload: &pb.StreamMessage_PublishReq{
			PublishReq: &pb.PublishRequest{
				RequestId: fmt.Sprintf("admin-%d", time.Now().UnixNano()),
				Items: []*pb.PublishItem{
					{
						Topic:        req.Topic,
						Payload:      []byte(req.Payload),
						PartitionId:  req.PartitionID,
						PartitionKey: req.PartitionKey,
					},
				},
			},
		},
	}

	if err := stream.Send(pubReq); err != nil {
		json.NewEncoder(w).Encode(APIResponse{Success: false, Error: err.Error()})
		return
	}

	// Wait for response
	resp, err := stream.Recv()
	if err != nil {
		json.NewEncoder(w).Encode(APIResponse{Success: false, Error: err.Error()})
		return
	}

	if resp.Type == pb.MessageType_MESSAGE_TYPE_PUBLISH_RESPONSE {
		pubResp := resp.GetPublishResp()
		json.NewEncoder(w).Encode(APIResponse{
			Success: true,
			Data: map[string]interface{}{
				"request_id": pubResp.RequestId,
				"results":    pubResp.Results,
			},
		})
	} else {
		json.NewEncoder(w).Encode(APIResponse{Success: false, Error: "unexpected response type"})
	}
}

// Helper function: encode Redis ID string (timestamp-seq) to int64
func encodeOffset(id string) int64 {
	parts := strings.Split(id, "-")
	if len(parts) != 2 {
		return 0
	}
	ts, _ := strconv.ParseInt(parts[0], 10, 64)
	seq, _ := strconv.ParseInt(parts[1], 10, 64)
	// Use 48 bits for timestamp, 16 bits for sequence (65535 msgs/ms)
	return (ts << 16) | (seq & 0xFFFF)
}

// Helper function: decode int64 offset back to Redis ID string
func decodeOffset(offset int64) string {
	if offset <= 0 {
		return "-"
	}
	ts := offset >> 16
	seq := offset & 0xFFFF
	return fmt.Sprintf("%d-%d", ts, seq)
}

func (s *AdminServer) handleMessages(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	topic := r.URL.Query().Get("topic")
	partitionIDStr := r.URL.Query().Get("partition_id")
	limitStr := r.URL.Query().Get("limit")
	startOffsetStr := r.URL.Query().Get("start_offset")

	limit := 100
	if limitStr != "" {
		if l, err := strconv.Atoi(limitStr); err == nil && l > 0 && l <= 1000 {
			limit = l
		}
	}

	startOffset := int64(0)
	if startOffsetStr != "" {
		if so, err := strconv.ParseInt(startOffsetStr, 10, 64); err == nil {
			startOffset = so
		}
	}

	ctx := context.Background()
	var result []MessageResp

	// Determine topics and partitions to scan
	var topics []string
	if topic != "" {
		topics = []string{topic}
	} else {
		// Scan all topics if none specified
		iter := s.redisClient.Scan(ctx, 0, "gmq:stream:*", 0).Iterator()
		topicSet := make(map[string]bool)
		for iter.Next(ctx) {
			parts := strings.Split(iter.Val(), ":")
			if len(parts) >= 4 {
				t := strings.Join(parts[2:len(parts)-1], ":")
				topicSet[t] = true
			}
		}
		for t := range topicSet {
			topics = append(topics, t)
		}
	}

	for _, t := range topics {
		var pids []int32
		if partitionIDStr != "" {
			if pid, err := strconv.Atoi(partitionIDStr); err == nil {
				pids = []int32{int32(pid)}
			}
		} else {
			// Scan all partitions for this topic
			pIter := s.redisClient.Scan(ctx, 0, fmt.Sprintf("gmq:stream:%s:*", t), 0).Iterator()
			for pIter.Next(ctx) {
				parts := strings.Split(pIter.Val(), ":")
				if pid, err := strconv.Atoi(parts[len(parts)-1]); err == nil {
					pids = append(pids, int32(pid))
				}
			}
		}

		for _, pid := range pids {
			streamKey := fmt.Sprintf("gmq:stream:%s:%d", t, pid)

			var messages []redis.XMessage
			var err error

			if startOffset <= 0 {
				// Get latest messages (Reverse scan from end)
				messages, err = s.redisClient.XRevRangeN(ctx, streamKey, "+", "-", int64(limit)).Result()
			} else {
				// Get messages older than startOffset (Reverse scan from startOffset-1)
				// Redis ID is "ts-seq". To get older, we use "(" + ID
				messages, err = s.redisClient.XRevRangeN(ctx, streamKey, "("+decodeOffset(startOffset), "-", int64(limit)).Result()
			}

			if err != nil {
				continue
			}

			for _, msg := range messages {
				offset := encodeOffset(msg.ID)
				payload := ""
				if data, ok := msg.Values["d"].(string); ok {
					payload = data
				}

				parts := strings.Split(msg.ID, "-")
				ts, _ := strconv.ParseInt(parts[0], 10, 64)

				result = append(result, MessageResp{
					Offset:      offset,
					Payload:     payload,
					PayloadSize: len(payload),
					Timestamp:   ts,
				})
			}
		}
	}

	// Sort results by timestamp/offset descending (globally across partitions)
	sort.Slice(result, func(i, j int) bool {
		return result[i].Offset > result[j].Offset
	})

	// Apply limit (since we might have multiple partitions)
	if len(result) > limit {
		result = result[:limit]
	}

	json.NewEncoder(w).Encode(APIResponse{Success: true, Data: result})
}

func (s *AdminServer) handleDeleteTopic(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Topic string `json:"topic"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		json.NewEncoder(w).Encode(APIResponse{Success: false, Error: err.Error()})
		return
	}

	if req.Topic == "" {
		json.NewEncoder(w).Encode(APIResponse{Success: false, Error: "topic is required"})
		return
	}

	ctx := context.Background()

	// 1. Find all related keys in Redis
	// Streams: gmq:stream:{topic}:*
	// Metadata: gmq:meta:partitions:{topic}, gmq:meta:ttl:{topic}
	// Consumer Groups: gmq:state:consumers:*:{topic}, gmq:state:assign:*:{topic}
	patterns := []string{
		fmt.Sprintf("gmq:stream:%s:*", req.Topic),
		fmt.Sprintf("gmq:meta:partitions:%s", req.Topic),
		fmt.Sprintf("gmq:meta:ttl:%s", req.Topic),
		fmt.Sprintf("gmq:state:consumers:*:%s", req.Topic),
		fmt.Sprintf("gmq:state:assign:%s:*", req.Topic),
	}

	var allKeys []string
	for _, pattern := range patterns {
		iter := s.redisClient.Scan(ctx, 0, pattern, 0).Iterator()
		for iter.Next(ctx) {
			allKeys = append(allKeys, iter.Val())
		}
	}

	if len(allKeys) > 0 {
		if err := s.redisClient.Del(ctx, allKeys...).Err(); err != nil {
			json.NewEncoder(w).Encode(APIResponse{Success: false, Error: "failed to delete keys: " + err.Error()})
			return
		}
	}

	// Also cleanup storage-service metadata if needed
	// (Actually our storage-service is mainly Redis-based, so Del is enough)

	json.NewEncoder(w).Encode(APIResponse{Success: true, Data: map[string]int{"deleted_keys": len(allKeys)}})
}

func (s *AdminServer) handleDashboard(w http.ResponseWriter, r *http.Request) {
	content, err := templateFS.ReadFile("templates/index.html")
	if err != nil {
		http.Error(w, "Failed to read template", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html")
	w.Write(content)
}

func main() {
	flag.Parse()
	log.Init(*logLevel)

	admin, err := NewAdminServer(*brokerAddr, *storageAddr, *redisAddr, *redisPass)
	if err != nil {
		log.Error("Failed to initialize admin server", "error", err)
		os.Exit(1)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", admin.handleDashboard)
	mux.HandleFunc("/api/topics", admin.handleTopics)
	mux.HandleFunc("/api/topics/delete", admin.handleDeleteTopic)
	mux.HandleFunc("/api/consumer-groups", admin.handleConsumerGroups)
	mux.HandleFunc("/api/send-message", admin.handleSendMessage)
	mux.HandleFunc("/api/messages", admin.handleMessages)

	server := &http.Server{
		Addr:    *addr,
		Handler: mux,
	}

	go func() {
		log.Info("Admin dashboard started", "addr", *addr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("HTTP server error", "error", err)
			os.Exit(1)
		}
	}()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	log.Info("Shutting down admin dashboard...")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	server.Shutdown(ctx)
	log.Info("Admin dashboard stopped")
}
