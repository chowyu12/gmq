package main

import (
	"context"
	"flag"
	"fmt"
	"hash/fnv"
	"io"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/chowyu12/gmq/pkg/log"
	pb "github.com/chowyu12/gmq/proto"
	storagepb "github.com/chowyu12/gmq/proto/storage"
	"github.com/rs/xid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	addr              = flag.String("addr", ":50051", "Broker 服务监听地址")
	storageAddr       = flag.String("storage", "localhost:50052", "Storage 服务地址")
	defaultPartitions = flag.Int("partitions", 4, "默认分区数")
	logLevel          = flag.String("log-level", "info", "日志级别: debug, info, warn, error")
)

// BrokerServer 统一代理服务，集成连接管理、分发逻辑和负载均衡
type BrokerServer struct {
	pb.UnimplementedGMQServiceServer
	storageClient storagepb.StorageServiceClient
	partitions    int32
	mu            sync.RWMutex
}

func NewBrokerServer(client storagepb.StorageServiceClient, partitions int32) *BrokerServer {
	return &BrokerServer{
		storageClient: client,
		partitions:    partitions,
	}
}

// rebalance 重新分配分区
func (s *BrokerServer) rebalance(ctx context.Context, consumerGroup, topic string) error {
	consumersResp, err := s.storageClient.GetConsumers(ctx, &storagepb.GetConsumersRequest{
		ConsumerGroup: consumerGroup,
		Topic:         topic,
	})
	if err != nil || !consumersResp.Success {
		return fmt.Errorf("获取消费者列表失败: %v", err)
	}

	if len(consumersResp.Consumers) == 0 {
		return nil
	}

	// 简单的平均分配算法
	assignment := make(map[int32]string)
	for i := int32(0); i < s.partitions; i++ {
		consumerIndex := int(i) % len(consumersResp.Consumers)
		targetConsumerID := consumersResp.Consumers[consumerIndex].ConsumerId
		assignment[i] = targetConsumerID
	}

	_, err = s.storageClient.UpdateGroupAssignment(ctx, &storagepb.UpdateGroupAssignmentRequest{
		ConsumerGroup:       consumerGroup,
		Topic:               topic,
		PartitionAssignment: assignment,
	})
	if err == nil {
		log.WithContext(ctx).Info("重平衡完成", "topic", topic, "group", consumerGroup, "assignment", assignment)
	}
	return err
}

// CreateTopic 实现手动创建 Topic 接口
func (s *BrokerServer) CreateTopic(ctx context.Context, req *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {
	partitions := req.Partitions
	if partitions <= 0 {
		partitions = 3
	}

	for i := int32(0); i < partitions; i++ {
		_, err := s.storageClient.CreatePartition(ctx, &storagepb.CreatePartitionRequest{
			Topic:       req.Topic,
			PartitionId: i,
		})
		if err != nil {
			return &pb.CreateTopicResponse{Success: false, ErrorMessage: err.Error()}, nil
		}
	}

	if req.TtlSeconds > 0 {
		_, err := s.storageClient.SetTTL(ctx, &storagepb.SetTTLRequest{
			Topic:      req.Topic,
			TtlSeconds: req.TtlSeconds,
		})
		if err != nil {
			log.WithContext(ctx).Error("设置 Topic TTL 失败", "topic", req.Topic, "error", err)
		}
	}

	return &pb.CreateTopicResponse{Success: true}, nil
}

// ensureTopicExists 确保 Topic 存在
func (s *BrokerServer) ensureTopicExists(ctx context.Context, topic string) error {
	resp, err := s.storageClient.ListPartitions(ctx, &storagepb.ListPartitionsRequest{Topic: topic})
	if err == nil && resp.Success && len(resp.Partitions) > 0 {
		return nil
	}

	for i := int32(0); i < 3; i++ {
		_, err := s.storageClient.CreatePartition(ctx, &storagepb.CreatePartitionRequest{
			Topic:       topic,
			PartitionId: i,
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// Stream 处理客户端双向流连接
func (s *BrokerServer) Stream(stream pb.GMQService_StreamServer) error {
	ctx := stream.Context()
	streamID := xid.New().String()

	log.WithContext(ctx).Info("新客户端流连接", "streamID", streamID)
	defer log.WithContext(ctx).Info("客户端流断开", "streamID", streamID)

	var (
		consumerID       string
		consumerGroup    string
		subscribedTopics []string
		pullInterval     = 100 * time.Millisecond
	)

	messageChan := make(chan *pb.ConsumeMessage, 100)
	stopChan := make(chan struct{})
	defer close(stopChan)

	// 1. 下行推送协程
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-stopChan:
				return
			case msg := <-messageChan:
				streamMsg := &pb.StreamMessage{
					Type: pb.MessageType_MESSAGE_TYPE_CONSUME_MESSAGE,
					Payload: &pb.StreamMessage_ConsumeMsg{
						ConsumeMsg: msg,
					},
				}
				if err := stream.Send(streamMsg); err != nil {
					log.WithContext(ctx).Error("推送消息失败", "streamID", streamID, "error", err)
					return
				}
			}
		}
	}()

	// 2. 轮询拉取协程
	pullTicker := time.NewTicker(pullInterval)
	defer pullTicker.Stop()

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-stopChan:
				return
			case <-pullTicker.C:
				if consumerID == "" || consumerGroup == "" {
					continue
				}

				for _, topic := range subscribedTopics {
					assignResp, err := s.storageClient.GetGroupAssignment(ctx, &storagepb.GetGroupAssignmentRequest{
						ConsumerGroup: consumerGroup,
						Topic:         topic,
					})
					if err != nil || !assignResp.Success {
						continue
					}

					var myPartitions []int32
					for partID, assignedConsumerID := range assignResp.PartitionAssignment {
						if assignedConsumerID == consumerID {
							myPartitions = append(myPartitions, partID)
						}
					}

					for _, partID := range myPartitions {
						offsetResp, err := s.storageClient.GetOffset(ctx, &storagepb.GetOffsetRequest{
							ConsumerGroup: consumerGroup,
							Topic:         topic,
							PartitionId:   partID,
						})
						if err != nil || !offsetResp.Success {
							continue
						}

						resp, err := s.storageClient.ReadMessages(ctx, &storagepb.ReadMessagesRequest{
							Topic:       topic,
							PartitionId: partID,
							Offset:      offsetResp.Offset,
							Limit:       20, // 批量拉取
						})
						if err != nil || !resp.Success || len(resp.Messages) == 0 {
							continue
						}

						batch := &pb.ConsumeMessage{
							Items: make([]*pb.MessageItem, len(resp.Messages)),
						}
						for i, m := range resp.Messages {
							batch.Items[i] = &pb.MessageItem{
								MessageId:   m.Id,
								Topic:       m.Topic,
								PartitionId: m.PartitionId,
								Offset:      m.Offset,
								Payload:     m.Payload,
								Properties:  m.Properties,
								Timestamp:   m.Timestamp,
								Qos:         pb.QoS(m.Qos),
							}
						}

						select {
						case messageChan <- batch:
						case <-ctx.Done():
							return
						case <-stopChan:
							return
						}
					}
				}
			}
		}
	}()

	// 3. 上行接收循环
	for {
		msg, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		switch msg.Type {
		case pb.MessageType_MESSAGE_TYPE_PUBLISH_REQUEST:
			s.handlePublish(ctx, stream, msg.GetPublishReq())

		case pb.MessageType_MESSAGE_TYPE_SUBSCRIBE_REQUEST:
			req := msg.GetSubscribeReq()
			consumerID = req.ConsumerId
			consumerGroup = req.ConsumerGroup
			subscribedTopics = append(subscribedTopics, req.Topic)
			if req.PullIntervalMs > 0 {
				pullInterval = time.Duration(req.PullIntervalMs) * time.Millisecond
				pullTicker.Reset(pullInterval)
			}
			s.handleSubscribe(ctx, stream, req)

		case pb.MessageType_MESSAGE_TYPE_ACK_REQUEST:
			s.handleAck(ctx, stream, msg.GetAckReq())

		case pb.MessageType_MESSAGE_TYPE_HEARTBEAT_REQUEST:
			s.handleHeartbeat(ctx, stream, msg.GetHeartbeatReq())
		}
	}

	// 4. 断开连接清理
	if consumerID != "" {
		for _, topic := range subscribedTopics {
			s.storageClient.DeleteConsumer(context.Background(), &storagepb.DeleteConsumerRequest{
				ConsumerId:    consumerID,
				ConsumerGroup: consumerGroup,
				Topic:         topic,
			})
			s.rebalance(context.Background(), consumerGroup, topic)
		}
	}

	return nil
}

func (s *BrokerServer) handlePublish(ctx context.Context, stream pb.GMQService_StreamServer, req *pb.PublishRequest) {
	if len(req.Items) == 0 {
		return
	}

	storageMsgs := make([]*storagepb.Message, len(req.Items))
	results := make([]*pb.PublishResult, len(req.Items))

	for i, item := range req.Items {
		if err := s.ensureTopicExists(ctx, item.Topic); err != nil {
			log.WithContext(ctx).Error("自动创建 Topic 失败", "topic", item.Topic, "error", err)
		}

		partitionID := item.PartitionId
		if partitionID < 0 {
			hash := fnv.New32a()
			key := item.PartitionKey
			if key == "" {
				key = xid.New().String()
			}
			hash.Write([]byte(key))
			partitionID = int32(hash.Sum32() % uint32(s.partitions))
		}

		msgID := xid.New().String()
		storageMsgs[i] = &storagepb.Message{
			Id:             msgID,
			Topic:          item.Topic,
			PartitionId:    partitionID,
			Payload:        item.Payload,
			Properties:     item.Properties,
			Timestamp:      time.Now().Unix(),
			Qos:            int32(item.Qos),
			ProducerId:     item.ProducerId,
			SequenceNumber: item.SequenceNumber,
		}
		results[i] = &pb.PublishResult{
			MessageId:   msgID,
			Topic:       item.Topic,
			PartitionId: partitionID,
		}
	}

	storageResp, err := s.storageClient.WriteMessages(ctx, &storagepb.WriteMessagesRequest{
		Messages: storageMsgs,
	})

	resp := &pb.PublishResponse{RequestId: req.RequestId, Results: results}
	if err != nil || !storageResp.Success {
		for _, r := range results {
			r.Success = false
			if err != nil {
				r.ErrorMessage = err.Error()
			} else {
				r.ErrorMessage = storageResp.ErrorMessage
			}
		}
	} else {
		for _, r := range results {
			r.Success = true
			// 注意：这里由于是批量写入，offset 可能需要对应返回，目前先简化处理
		}
	}

	stream.Send(&pb.StreamMessage{
		Type:    pb.MessageType_MESSAGE_TYPE_PUBLISH_RESPONSE,
		Payload: &pb.StreamMessage_PublishResp{PublishResp: resp},
	})
}

func (s *BrokerServer) handleSubscribe(ctx context.Context, stream pb.GMQService_StreamServer, req *pb.SubscribeRequest) {
	_, err := s.storageClient.SaveConsumer(ctx, &storagepb.SaveConsumerRequest{
		Consumer: &storagepb.ConsumerState{
			ConsumerId:         req.ConsumerId,
			ConsumerGroup:      req.ConsumerGroup,
			Topic:              req.Topic,
			AssignedPartitions: req.Partitions,
			LastHeartbeat:      time.Now().Unix(),
		},
	})

	if err == nil {
		s.rebalance(ctx, req.ConsumerGroup, req.Topic)
	}

	resp := &pb.SubscribeResponse{RequestId: req.RequestId}
	if err != nil {
		resp.Success = false
		resp.ErrorMessage = err.Error()
	} else {
		resp.Success = true
		resp.AssignedPartitions = req.Partitions
	}

	stream.Send(&pb.StreamMessage{
		Type:    pb.MessageType_MESSAGE_TYPE_SUBSCRIBE_RESPONSE,
		Payload: &pb.StreamMessage_SubscribeResp{SubscribeResp: resp},
	})
}

func (s *BrokerServer) handleAck(ctx context.Context, stream pb.GMQService_StreamServer, req *pb.AckRequest) {
	var lastErr error
	for _, item := range req.Items {
		_, err := s.storageClient.UpdateOffset(ctx, &storagepb.UpdateOffsetRequest{
			ConsumerGroup: req.ConsumerGroup,
			Topic:         item.Topic,
			PartitionId:   item.PartitionId,
			Offset:        item.Offset + 1,
		})
		if err != nil {
			lastErr = err
		}
	}

	resp := &pb.AckResponse{RequestId: req.RequestId, Success: lastErr == nil}
	if lastErr != nil {
		resp.ErrorMessage = lastErr.Error()
	}

	stream.Send(&pb.StreamMessage{
		Type:    pb.MessageType_MESSAGE_TYPE_ACK_RESPONSE,
		Payload: &pb.StreamMessage_AckResp{AckResp: resp},
	})
}

func (s *BrokerServer) handleHeartbeat(ctx context.Context, stream pb.GMQService_StreamServer, req *pb.HeartbeatRequest) {
	s.storageClient.SaveConsumer(ctx, &storagepb.SaveConsumerRequest{
		Consumer: &storagepb.ConsumerState{
			ConsumerId:    req.ConsumerId,
			ConsumerGroup: req.ConsumerGroup,
			LastHeartbeat: time.Now().Unix(),
		},
	})

	stream.Send(&pb.StreamMessage{
		Type:    pb.MessageType_MESSAGE_TYPE_HEARTBEAT_RESPONSE,
		Payload: &pb.StreamMessage_HeartbeatResp{HeartbeatResp: &pb.HeartbeatResponse{Success: true}},
	})
}

func main() {
	flag.Parse()
	log.Init(*logLevel)

	log.Info("Broker Service 启动中", "addr", *addr, "storage", *storageAddr)

	conn, err := grpc.Dial(*storageAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Error("连接 Storage 失败", "error", err)
		os.Exit(1)
	}
	defer conn.Close()

	storageClient := storagepb.NewStorageServiceClient(conn)

	grpcServer := grpc.NewServer()
	brokerServer := NewBrokerServer(storageClient, int32(*defaultPartitions))
	pb.RegisterGMQServiceServer(grpcServer, brokerServer)

	listener, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Error("监听端口失败", "error", err)
		os.Exit(1)
	}

	go func() {
		if err := grpcServer.Serve(listener); err != nil {
			log.Error("服务运行异常", "error", err)
			os.Exit(1)
		}
	}()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	log.Info("正在优雅关闭 Broker...")
	grpcServer.GracefulStop()
	log.Info("服务已关闭")
}
