package main

import (
	"context"
	"flag"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/chowyu12/gmq/internal/storage"
	"github.com/chowyu12/gmq/pkg/log"
	pb "github.com/chowyu12/gmq/proto/storage"
	"google.golang.org/grpc"
)

var (
	addr      = flag.String("addr", ":50052", "Storage 服务监听地址")
	redisAddr = flag.String("redis-addr", "localhost:6379", "Redis/DragonflyDB 地址")
	redisPass = flag.String("redis-pass", "", "Redis 密码")
	logLevel  = flag.String("log-level", "info", "日志级别: debug, info, warn, error")
)

// StorageServer 存储服务实现
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

func (s *StorageServer) WriteMessages(ctx context.Context, req *pb.WriteMessagesRequest) (*pb.WriteMessagesResponse, error) {
	internalMsgs := make([]*storage.Message, len(req.Messages))
	for i, msg := range req.Messages {
		internalMsgs[i] = &storage.Message{
			ID:             msg.Id,
			Topic:          msg.Topic,
			PartitionID:    msg.PartitionId,
			Payload:        msg.Payload,
			Properties:     msg.Properties,
			Timestamp:      msg.Timestamp,
			QoS:            msg.Qos,
			ProducerID:     msg.ProducerId,
			SequenceNumber: msg.SequenceNumber,
		}
	}

	offsets, err := s.store.WriteMessages(ctx, internalMsgs)
	if err != nil {
		log.WithContext(ctx).Error("批量写入消息失败", "error", err)
		return &pb.WriteMessagesResponse{Success: false, ErrorMessage: err.Error()}, nil
	}

	return &pb.WriteMessagesResponse{Success: true, Offsets: offsets}, nil
}

func (s *StorageServer) ReadMessages(ctx context.Context, req *pb.ReadMessagesRequest) (*pb.ReadMessagesResponse, error) {
	messages, err := s.store.ReadMessages(ctx, req.Topic, req.PartitionId, req.Offset, int(req.Limit))
	if err != nil {
		log.WithContext(ctx).Error("读取消息失败", "topic", req.Topic, "error", err)
		return &pb.ReadMessagesResponse{Success: false, ErrorMessage: err.Error()}, nil
	}

	pbMessages := make([]*pb.Message, len(messages))
	for i, msg := range messages {
		pbMessages[i] = &pb.Message{
			Id:             msg.ID,
			Topic:          msg.Topic,
			PartitionId:    msg.PartitionID,
			Offset:         msg.Offset,
			Payload:        msg.Payload,
			Properties:     msg.Properties,
			Timestamp:      msg.Timestamp,
			Qos:            msg.QoS,
			ProducerId:     msg.ProducerID,
			SequenceNumber: msg.SequenceNumber,
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
	log.Init(*logLevel)

	log.Info("Storage Service 启动中", "engine", "Redis/DragonflyDB")

	store, err := storage.NewRedisStorage(*redisAddr, *redisPass, 0)
	if err != nil {
		log.Error("初始化存储失败", "error", err)
		os.Exit(1)
	}
	defer store.Close()

	grpcServer := grpc.NewServer()
	storageServer := NewStorageServer(store)
	pb.RegisterStorageServiceServer(grpcServer, storageServer)

	listener, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Error("监听端口失败", "error", err)
		os.Exit(1)
	}

	go func() {
		log.Info("gRPC 服务启动", "addr", *addr)
		if err := grpcServer.Serve(listener); err != nil {
			log.Error("服务运行异常", "error", err)
			os.Exit(1)
		}
	}()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	log.Info("正在优雅关闭服务...")
	grpcServer.GracefulStop()
	log.Info("服务已关闭")
}
