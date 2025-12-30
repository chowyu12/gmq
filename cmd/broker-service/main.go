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

	pb "github.com/chowyu12/gmq/proto"
	storagepb "github.com/chowyu12/gmq/proto/storage"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	addr              = flag.String("addr", ":50051", "Broker服务监听地址")
	storageAddr       = flag.String("storage", "localhost:50052", "Storage服务地址")
	defaultPartitions = flag.Int("partitions", 4, "默认分区数")
)

// BrokerServer 合并后的服务，集成了 Gateway 和 Dispatcher 功能
type BrokerServer struct {
	pb.UnimplementedGMQServiceServer
	storageClient storagepb.StorageServiceClient
	partitions    int32

	// 本地缓存，用于快速分发，但状态持久化在 Storage
	mu sync.RWMutex
}

func NewBrokerServer(client storagepb.StorageServiceClient, partitions int32) *BrokerServer {
	return &BrokerServer{
		storageClient: client,
		partitions:    partitions,
	}
}

// rebalance 重新分配分区的辅助方法
func (s *BrokerServer) rebalance(ctx context.Context, consumerGroup, topic string) error {
	// 1. 获取所有活跃消费者
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

	// 2. 简单的平均分配算法
	assignment := make(map[int32]string)
	for i := int32(0); i < s.partitions; i++ {
		consumerIndex := int(i) % len(consumersResp.Consumers)
		targetConsumerID := consumersResp.Consumers[consumerIndex].ConsumerId
		assignment[i] = targetConsumerID
	}

	// 3. 更新到 Storage
	_, err = s.storageClient.UpdateGroupAssignment(ctx, &storagepb.UpdateGroupAssignmentRequest{
		ConsumerGroup:       consumerGroup,
		Topic:               topic,
		PartitionAssignment: assignment,
	})
	return err
}

// CreateTopic 实现手动创建 Topic 接口
func (s *BrokerServer) CreateTopic(ctx context.Context, req *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {
	partitions := req.Partitions
	if partitions <= 0 {
		partitions = 3 // 默认 3 个分区
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

	// 如果指定了 TTL，同步设置
	if req.TtlSeconds > 0 {
		_, err := s.storageClient.SetTTL(ctx, &storagepb.SetTTLRequest{
			Topic:      req.Topic,
			TtlSeconds: req.TtlSeconds,
		})
		if err != nil {
			fmt.Printf("设置 Topic TTL 失败: %v\n", err)
		}
	}

	return &pb.CreateTopicResponse{Success: true}, nil
}

// ensureTopicExists 确保 Topic 存在，不存在则创建默认分区
func (s *BrokerServer) ensureTopicExists(ctx context.Context, topic string) error {
	resp, err := s.storageClient.ListPartitions(ctx, &storagepb.ListPartitionsRequest{Topic: topic})
	if err == nil && resp.Success && len(resp.Partitions) > 0 {
		return nil
	}

	// 不存在，创建默认 3 个分区
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
	clientID := fmt.Sprintf("broker_client_%d", time.Now().UnixNano())

	fmt.Printf("客户端连接: %s\n", clientID)
	defer fmt.Printf("客户端断开: %s\n", clientID)

	var (
		consumerID       string
		consumerGroup    string
		subscribedTopics []string
		pullInterval     = 100 * time.Millisecond // 默认 100ms
	)

	// 消息发送通道
	messageChan := make(chan *pb.ConsumeMessage, 100)
	stopChan := make(chan struct{})
	defer close(stopChan)

	// 内存中的发送进度，防止重复推送 (key: topic-partition)
	sendingOffsets := make(map[string]int64)

	// 1. 消息发送协程 (Gateway 职责)
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
					fmt.Printf("发送消息失败: %v\n", err)
					return
				}
			}
		}
	}()

	// 2. 消息轮询拉取协程 (Dispatcher 职责)
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
				// 如果订阅时修改了 pullInterval，这里需要更新 Ticker
				// 简化实现：Ticker 在流生命周期内保持 Subscribe 时的设定
				if consumerID == "" || consumerGroup == "" {
					continue
				}

				for _, topic := range subscribedTopics {
					// 1. 获取当前消费者的分配方案
					assignResp, err := s.storageClient.GetGroupAssignment(ctx, &storagepb.GetGroupAssignmentRequest{
						ConsumerGroup: consumerGroup,
						Topic:         topic,
					})
					if err != nil || !assignResp.Success {
						continue
					}

					// 2. 筛选出属于当前消费者的分区
					var myPartitions []int32
					for partID, assignedConsumerID := range assignResp.PartitionAssignment {
						if assignedConsumerID == consumerID {
							myPartitions = append(myPartitions, partID)
						}
					}

					// 3. 为每个分配的分区拉取消息
					for _, partID := range myPartitions {
						key := fmt.Sprintf("%s-%d", topic, partID)

						// 如果内存中没有发送进度，则从 Storage 获取当前已确认的偏移量作为起点
						if _, exists := sendingOffsets[key]; !exists {
							offsetResp, err := s.storageClient.GetOffset(ctx, &storagepb.GetOffsetRequest{
								ConsumerGroup: consumerGroup,
								Topic:         topic,
								PartitionId:   partID,
							})
							if err != nil || !offsetResp.Success {
								continue
							}
							sendingOffsets[key] = offsetResp.Offset
						}

						resp, err := s.storageClient.ReadMessages(ctx, &storagepb.ReadMessagesRequest{
							Topic:       topic,
							PartitionId: partID,
							Offset:      sendingOffsets[key],
							Limit:       10,
						})
						if err != nil || !resp.Success || len(resp.Messages) == 0 {
							continue
						}

						// 更新内存发送进度：下一次从这一批的最后一条消息之后开始拉取
						sendingOffsets[key] = resp.Messages[len(resp.Messages)-1].Offset + 1

						for _, m := range resp.Messages {
							select {
							case messageChan <- &pb.ConsumeMessage{
								MessageId:   m.Id,
								Topic:       m.Topic,
								PartitionId: m.PartitionId,
								Offset:      m.Offset,
								Payload:     m.Payload,
								Properties:  m.Properties,
								Timestamp:   m.Timestamp,
								Qos:         pb.QoS(m.Qos),
							}:
							case <-ctx.Done():
								return
							case <-stopChan:
								return
							}
						}
					}
				}
			}
		}
	}()

	// 3. 处理接收消息
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
			if req.ClientId != "" {
				clientID = req.ClientId
			}
			if req.PullIntervalMs > 0 {
				pullInterval = time.Duration(req.PullIntervalMs) * time.Millisecond
				pullTicker.Reset(pullInterval) // 动态更新拉取间隔
			}
			s.handleSubscribe(ctx, stream, req)

		case pb.MessageType_MESSAGE_TYPE_ACK_REQUEST:
			s.handleAck(ctx, stream, msg.GetAckReq())

		case pb.MessageType_MESSAGE_TYPE_HEARTBEAT_REQUEST:
			s.handleHeartbeat(ctx, stream, msg.GetHeartbeatReq())
		}
	}

	// 4. 清理：通知 Storage 消费者下线
	if consumerID != "" {
		for _, topic := range subscribedTopics {
			s.storageClient.DeleteConsumer(context.Background(), &storagepb.DeleteConsumerRequest{
				ConsumerId:    consumerID,
				ConsumerGroup: consumerGroup,
				Topic:         topic,
			})
			// 触发重平衡
			s.rebalance(context.Background(), consumerGroup, topic)
		}
	}

	return nil
}

func (s *BrokerServer) handlePublish(ctx context.Context, stream pb.GMQService_StreamServer, req *pb.PublishRequest) {
	// 确保 Topic 存在
	if err := s.ensureTopicExists(ctx, req.Topic); err != nil {
		fmt.Printf("自动创建 Topic 失败: %v\n", err)
	}

	partitionID := req.PartitionId
	if partitionID < 0 {
		hash := fnv.New32a()
		key := req.PartitionKey
		if key == "" {
			key = uuid.New().String()
		}
		hash.Write([]byte(key))
		partitionID = int32(hash.Sum32() % uint32(s.partitions))
	}

	msgID := uuid.New().String()
	storageResp, err := s.storageClient.WriteMessage(ctx, &storagepb.WriteMessageRequest{
		Message: &storagepb.Message{
			Id:          msgID,
			Topic:       req.Topic,
			PartitionId: partitionID,
			Payload:     req.Payload,
			Properties:  req.Properties,
			Timestamp:   time.Now().Unix(),
			Qos:         int32(req.Qos),
		},
	})

	resp := &pb.PublishResponse{RequestId: req.RequestId, Topic: req.Topic}
	if err != nil || !storageResp.Success {
		resp.Success = false
		if err != nil {
			resp.ErrorMessage = err.Error()
		} else {
			resp.ErrorMessage = storageResp.ErrorMessage
		}
	} else {
		resp.Success = true
		resp.MessageId = msgID
		resp.PartitionId = partitionID
	}

	stream.Send(&pb.StreamMessage{
		Type:    pb.MessageType_MESSAGE_TYPE_PUBLISH_RESPONSE,
		Payload: &pb.StreamMessage_PublishResp{PublishResp: resp},
	})
}

func (s *BrokerServer) handleSubscribe(ctx context.Context, stream pb.GMQService_StreamServer, req *pb.SubscribeRequest) {
	// 1. 持久化消费者状态到 Storage
	_, err := s.storageClient.SaveConsumer(ctx, &storagepb.SaveConsumerRequest{
		Consumer: &storagepb.ConsumerState{
			ConsumerId:         req.ConsumerId,
			ConsumerGroup:      req.ConsumerGroup,
			Topic:              req.Topic,
			AssignedPartitions: req.Partitions,
			LastHeartbeat:      time.Now().Unix(),
		},
	})

	// 2. 触发重平衡
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
	_, err := s.storageClient.UpdateOffset(ctx, &storagepb.UpdateOffsetRequest{
		ConsumerGroup: req.ConsumerGroup,
		Topic:         req.Topic,
		PartitionId:   req.PartitionId,
		Offset:        req.Offset + 1, // 确认为下一条消息的偏移量
	})

	resp := &pb.AckResponse{RequestId: req.RequestId, Success: err == nil}
	if err != nil {
		resp.ErrorMessage = err.Error()
	}

	stream.Send(&pb.StreamMessage{
		Type:    pb.MessageType_MESSAGE_TYPE_ACK_RESPONSE,
		Payload: &pb.StreamMessage_AckResp{AckResp: resp},
	})
}

func (s *BrokerServer) handleHeartbeat(ctx context.Context, stream pb.GMQService_StreamServer, req *pb.HeartbeatRequest) {
	// 更新心跳状态到 Storage
	s.storageClient.SaveConsumer(ctx, &storagepb.SaveConsumerRequest{
		Consumer: &storagepb.ConsumerState{
			ConsumerId:    req.ConsumerId,
			ConsumerGroup: req.ConsumerGroup,
			LastHeartbeat: time.Now().Unix(),
		},
	})

	// 实际生产环境这里可能会根据心跳超时情况触发 rebalance
	// 简化实现：心跳仅更新时间

	stream.Send(&pb.StreamMessage{
		Type:    pb.MessageType_MESSAGE_TYPE_HEARTBEAT_RESPONSE,
		Payload: &pb.StreamMessage_HeartbeatResp{HeartbeatResp: &pb.HeartbeatResponse{Success: true}},
	})
}

func main() {
	flag.Parse()

	fmt.Printf("Broker Service (Gateway + Dispatcher) 启动中...\n")
	fmt.Printf("监听地址: %s\n", *addr)
	fmt.Printf("Storage地址: %s\n", *storageAddr)

	conn, err := grpc.Dial(*storageAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Printf("连接 Storage Service 失败: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	storageClient := storagepb.NewStorageServiceClient(conn)

	grpcServer := grpc.NewServer()
	brokerServer := NewBrokerServer(storageClient, int32(*defaultPartitions))
	pb.RegisterGMQServiceServer(grpcServer, brokerServer)

	listener, err := net.Listen("tcp", *addr)
	if err != nil {
		fmt.Printf("监听端口失败: %v\n", err)
		os.Exit(1)
	}

	go func() {
		if err := grpcServer.Serve(listener); err != nil {
			fmt.Printf("服务器错误: %v\n", err)
			os.Exit(1)
		}
	}()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	fmt.Println("\n正在关闭 Broker Service...")
	grpcServer.GracefulStop()
}
