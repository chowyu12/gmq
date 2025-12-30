package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/chowyu12/gmq/internal/storage"
	pb "github.com/chowyu12/gmq/proto/storage"
	"google.golang.org/grpc"
)

var (
	addr      = flag.String("addr", ":50052", "Storage服务监听地址")
	engine    = flag.String("engine", "file", "存储引擎: file 或 redis")
	dataDir   = flag.String("data", "./storage-data", "文件引擎数据目录")
	redisAddr = flag.String("redis-addr", "localhost:6379", "Redis/Dragonfly 地址")
	redisPass = flag.String("redis-pass", "", "Redis 密码")
)

// StorageServer Storage服务实现
type StorageServer struct {
	pb.UnimplementedStorageServiceServer
	store storage.Storage
}

func NewStorageServer(store storage.Storage) *StorageServer {
	return &StorageServer{
		store: store,
	}
}

// --- 消息接口实现 ---

func (s *StorageServer) WriteMessage(ctx context.Context, req *pb.WriteMessageRequest) (*pb.WriteMessageResponse, error) {
	msg := req.Message
	internalMsg := &storage.Message{
		ID:          msg.Id,
		Topic:       msg.Topic,
		PartitionID: msg.PartitionId,
		Payload:     msg.Payload,
		Properties:  msg.Properties,
		Timestamp:   msg.Timestamp,
		QoS:         msg.Qos,
	}

	offset, err := s.store.WriteMessage(ctx, internalMsg)
	if err != nil {
		return &pb.WriteMessageResponse{Success: false, ErrorMessage: err.Error()}, nil
	}

	return &pb.WriteMessageResponse{Success: true, Offset: offset}, nil
}

func (s *StorageServer) ReadMessages(ctx context.Context, req *pb.ReadMessagesRequest) (*pb.ReadMessagesResponse, error) {
	messages, err := s.store.ReadMessages(ctx, req.Topic, req.PartitionId, req.Offset, int(req.Limit))
	if err != nil {
		return &pb.ReadMessagesResponse{Success: false, ErrorMessage: err.Error()}, nil
	}

	pbMessages := make([]*pb.Message, len(messages))
	for i, msg := range messages {
		pbMessages[i] = &pb.Message{
			Id:          msg.ID,
			Topic:       msg.Topic,
			PartitionId: msg.PartitionID,
			Offset:      msg.Offset,
			Payload:     msg.Payload,
			Properties:  msg.Properties,
			Timestamp:   msg.Timestamp,
			Qos:         msg.QoS,
		}
	}

	return &pb.ReadMessagesResponse{Messages: pbMessages, Success: true}, nil
}

func (s *StorageServer) CreatePartition(ctx context.Context, req *pb.CreatePartitionRequest) (*pb.CreatePartitionResponse, error) {
	err := s.store.CreatePartition(ctx, req.Topic, req.PartitionId)
	if err != nil {
		return &pb.CreatePartitionResponse{Success: false, ErrorMessage: err.Error()}, nil
	}
	return &pb.CreatePartitionResponse{Success: true}, nil
}

func (s *StorageServer) GetPartition(ctx context.Context, req *pb.GetPartitionRequest) (*pb.GetPartitionResponse, error) {
	p, err := s.store.GetPartition(ctx, req.Topic, req.PartitionId)
	if err != nil {
		return &pb.GetPartitionResponse{Success: false, ErrorMessage: err.Error()}, nil
	}

	return &pb.GetPartitionResponse{
		Partition: &pb.PartitionInfo{
			Id:           p.ID,
			Topic:        p.Topic,
			NextOffset:   p.NextOffset,
			MessageCount: p.MessageCount,
		},
		Success: true,
	}, nil
}

func (s *StorageServer) UpdateOffset(ctx context.Context, req *pb.UpdateOffsetRequest) (*pb.UpdateOffsetResponse, error) {
	err := s.store.UpdateOffset(ctx, req.ConsumerGroup, req.Topic, req.PartitionId, req.Offset)
	if err != nil {
		return &pb.UpdateOffsetResponse{Success: false, ErrorMessage: err.Error()}, nil
	}
	return &pb.UpdateOffsetResponse{Success: true}, nil
}

func (s *StorageServer) GetOffset(ctx context.Context, req *pb.GetOffsetRequest) (*pb.GetOffsetResponse, error) {
	offset, err := s.store.GetOffset(ctx, req.ConsumerGroup, req.Topic, req.PartitionId)
	if err != nil {
		return &pb.GetOffsetResponse{Success: false, ErrorMessage: err.Error()}, nil
	}
	return &pb.GetOffsetResponse{Offset: offset, Success: true}, nil
}

func (s *StorageServer) ListPartitions(ctx context.Context, req *pb.ListPartitionsRequest) (*pb.ListPartitionsResponse, error) {
	partitions, err := s.store.ListPartitions(ctx, req.Topic)
	if err != nil {
		return &pb.ListPartitionsResponse{Success: false, ErrorMessage: err.Error()}, nil
	}

	pbPartitions := make([]*pb.PartitionInfo, len(partitions))
	for i, p := range partitions {
		pbPartitions[i] = &pb.PartitionInfo{
			Id:         p.ID,
			Topic:      p.Topic,
			NextOffset: p.NextOffset,
		}
	}
	return &pb.ListPartitionsResponse{Partitions: pbPartitions, Success: true}, nil
}

func (s *StorageServer) SetTTL(ctx context.Context, req *pb.SetTTLRequest) (*pb.SetTTLResponse, error) {
	err := s.store.SetTTL(ctx, req.Topic, time.Duration(req.TtlSeconds)*time.Second)
	if err != nil {
		return &pb.SetTTLResponse{Success: false, ErrorMessage: err.Error()}, nil
	}
	return &pb.SetTTLResponse{Success: true}, nil
}

// --- 状态管理接口实现 ---

func (s *StorageServer) SaveConsumer(ctx context.Context, req *pb.SaveConsumerRequest) (*pb.SaveConsumerResponse, error) {
	c := req.Consumer
	err := s.store.SaveConsumer(ctx, &storage.ConsumerState{
		ConsumerId:         c.ConsumerId,
		ConsumerGroup:      c.ConsumerGroup,
		Topic:              c.Topic,
		AssignedPartitions: c.AssignedPartitions,
		LastHeartbeat:      c.LastHeartbeat,
	})
	if err != nil {
		return &pb.SaveConsumerResponse{Success: false, ErrorMessage: err.Error()}, nil
	}
	return &pb.SaveConsumerResponse{Success: true}, nil
}

func (s *StorageServer) GetConsumers(ctx context.Context, req *pb.GetConsumersRequest) (*pb.GetConsumersResponse, error) {
	consumers, err := s.store.GetConsumers(ctx, req.ConsumerGroup, req.Topic)
	if err != nil {
		return &pb.GetConsumersResponse{Success: false, ErrorMessage: err.Error()}, nil
	}

	pbConsumers := make([]*pb.ConsumerState, len(consumers))
	for i, c := range consumers {
		pbConsumers[i] = &pb.ConsumerState{
			ConsumerId:         c.ConsumerId,
			ConsumerGroup:      c.ConsumerGroup,
			Topic:              c.Topic,
			AssignedPartitions: c.AssignedPartitions,
			LastHeartbeat:      c.LastHeartbeat,
		}
	}
	return &pb.GetConsumersResponse{Consumers: pbConsumers, Success: true}, nil
}

func (s *StorageServer) DeleteConsumer(ctx context.Context, req *pb.DeleteConsumerRequest) (*pb.DeleteConsumerResponse, error) {
	err := s.store.DeleteConsumer(ctx, req.ConsumerId, req.ConsumerGroup, req.Topic)
	if err != nil {
		return &pb.DeleteConsumerResponse{Success: false, ErrorMessage: err.Error()}, nil
	}
	return &pb.DeleteConsumerResponse{Success: true}, nil
}

func (s *StorageServer) UpdateGroupAssignment(ctx context.Context, req *pb.UpdateGroupAssignmentRequest) (*pb.UpdateGroupAssignmentResponse, error) {
	err := s.store.UpdateAssignment(ctx, req.ConsumerGroup, req.Topic, req.PartitionAssignment)
	if err != nil {
		return &pb.UpdateGroupAssignmentResponse{Success: false, ErrorMessage: err.Error()}, nil
	}
	return &pb.UpdateGroupAssignmentResponse{Success: true}, nil
}

func (s *StorageServer) GetGroupAssignment(ctx context.Context, req *pb.GetGroupAssignmentRequest) (*pb.GetGroupAssignmentResponse, error) {
	assignment, err := s.store.GetAssignment(ctx, req.ConsumerGroup, req.Topic)
	if err != nil {
		return &pb.GetGroupAssignmentResponse{Success: false, ErrorMessage: err.Error()}, nil
	}
	return &pb.GetGroupAssignmentResponse{PartitionAssignment: assignment, Success: true}, nil
}

func main() {
	flag.Parse()

	fmt.Printf("Storage Service 启动中 (Engine: %s)...\n", *engine)

	var store storage.Storage
	var err error

	if *engine == "redis" {
		store, err = storage.NewRedisStorage(*redisAddr, *redisPass, 0)
		if err != nil {
			fmt.Printf("创建 Redis 存储失败: %v\n", err)
			os.Exit(1)
		}
	} else {
		// 默认使用文件引擎
		// 文件引擎目前需要同时包含 FileStorage 和 SQLStateStorage 的逻辑
		// 为了简化，我们暂时保留旧的 FileStorage 实现，但它需要适配新的统一接口
		// 实际上我们应该重构 FileStorage 使其实现 storage.Storage 接口
		// 这里假设 FileStorage 已经通过某种方式合并了逻辑或实现了接口
		store, err = storage.NewFileStorage(*dataDir)
		if err != nil {
			fmt.Printf("创建文件存储失败: %v\n", err)
			os.Exit(1)
		}
	}
	defer store.Close()

	// 创建 gRPC 服务器
	grpcServer := grpc.NewServer()
	storageServer := NewStorageServer(store)
	pb.RegisterStorageServiceServer(grpcServer, storageServer)

	// 监听端口
	listener, err := net.Listen("tcp", *addr)
	if err != nil {
		fmt.Printf("监听端口失败: %v\n", err)
		os.Exit(1)
	}

	go func() {
		fmt.Printf("Storage Service 已在 %s 启动\n", *addr)
		if err := grpcServer.Serve(listener); err != nil {
			fmt.Printf("服务器错误: %v\n", err)
			os.Exit(1)
		}
	}()

	// 等待退出信号
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	fmt.Println("\n正在关闭 Storage Service...")
	grpcServer.GracefulStop()
	fmt.Println("Storage Service 已关闭")
}
