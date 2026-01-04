package main

import (
	"context"
	"flag"
	"hash/fnv"
	"io"
	"net"
	"os"
	"os/signal"
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
	addr              = flag.String("addr", ":50051", "Broker 地址")
	storageAddr       = flag.String("storage", "localhost:50052", "Storage 地址")
	defaultPartitions = flag.Int("partitions", 4, "默认分区数")
	logLevel          = flag.String("log-level", "info", "日志级别")
)

type BrokerServer struct {
	pb.UnimplementedGMQServiceServer
	storageClient storagepb.StorageServiceClient
	partitions    int32
}

func NewBrokerServer(client storagepb.StorageServiceClient, partitions int32) *BrokerServer {
	return &BrokerServer{storageClient: client, partitions: partitions}
}

func (s *BrokerServer) Stream(stream pb.GMQService_StreamServer) error {
	ctx := stream.Context()
	for {
		msg, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}

		switch msg.Type {
		case pb.MessageType_MESSAGE_TYPE_PUBLISH_REQUEST:
			s.handlePublish(ctx, stream, msg.GetPublishReq())
		case pb.MessageType_MESSAGE_TYPE_SUBSCRIBE_REQUEST:
			s.handleSubscribe(ctx, stream, msg.GetSubscribeReq())
		case pb.MessageType_MESSAGE_TYPE_ACK_REQUEST:
			s.handleAck(ctx, stream, msg.GetAckReq())
		case pb.MessageType_MESSAGE_TYPE_PULL_REQUEST:
			s.handlePull(ctx, stream, msg.GetPullReq())
		case pb.MessageType_MESSAGE_TYPE_HEARTBEAT_REQUEST:
			s.handleHeartbeat(ctx, stream, msg.GetHeartbeatReq())
		}
	}
}

func (s *BrokerServer) handlePull(ctx context.Context, stream pb.GMQService_StreamServer, req *pb.PullRequest) {
	assignResp, err := s.storageClient.GetGroupAssignment(ctx, &storagepb.GetGroupAssignmentRequest{
		ConsumerGroup: req.ConsumerGroup, Topic: req.Topic,
	})
	if err != nil || !assignResp.Success {
		return
	}

	var myPartitions []int32
	for partID, assignedID := range assignResp.PartitionAssignment {
		if assignedID == req.ConsumerId {
			myPartitions = append(myPartitions, partID)
		}
	}
	if len(myPartitions) == 0 {
		return
	}

	timeout := time.After(1 * time.Second)
	for {
		var allItems []*pb.MessageItem
		for _, partID := range myPartitions {
			// 直接原子拉取：获取进度、读取、更新进度三合一
			resp, err := s.storageClient.FetchMessages(ctx, &storagepb.FetchMessagesRequest{
				Topic:         req.Topic,
				PartitionId:   partID,
				ConsumerGroup: req.ConsumerGroup,
				Limit:         req.Limit,
			})

			if err == nil && len(resp.Messages) > 0 {
				for _, m := range resp.Messages {
					allItems = append(allItems, &pb.MessageItem{
						MessageId:   m.Id,
						Topic:       m.Topic,
						PartitionId: m.PartitionId,
						Offset:      m.Offset,
						Payload:     m.Payload,
						Properties:  m.Properties,
						Timestamp:   m.Timestamp,
						Qos:         pb.QoS(m.Qos),
					})
				}
			}
		}

		if len(allItems) > 0 {
			stream.Send(&pb.StreamMessage{
				Type: pb.MessageType_MESSAGE_TYPE_CONSUME_MESSAGE,
				Payload: &pb.StreamMessage_ConsumeMsg{
					ConsumeMsg: &pb.ConsumeMessage{Items: allItems},
				},
			})
			return
		}

		select {
		case <-timeout:
			return
		case <-ctx.Done():
			return
		case <-time.After(20 * time.Millisecond):
			continue
		}
	}
}

func (s *BrokerServer) handleAck(ctx context.Context, stream pb.GMQService_StreamServer, req *pb.AckRequest) {
	// 在 Fetch-and-Update 架构下，Ack 用于保证 At-Least-Once
	// 这里目前主要做返回响应
	stream.Send(&pb.StreamMessage{
		Type: pb.MessageType_MESSAGE_TYPE_ACK_RESPONSE,
		Payload: &pb.StreamMessage_AckResp{
			AckResp: &pb.AckResponse{RequestId: req.RequestId, Success: true},
		},
	})
}

func (s *BrokerServer) handlePublish(ctx context.Context, stream pb.GMQService_StreamServer, req *pb.PublishRequest) {
	storageMsgs := make([]*storagepb.Message, len(req.Items))
	results := make([]*pb.PublishResult, len(req.Items))
	for i, item := range req.Items {
		msgID := xid.New().String()
		partID := item.PartitionId
		if partID < 0 {
			partID = int32(fnv.New32a().Sum32() % uint32(s.partitions))
		}
		storageMsgs[i] = &storagepb.Message{
			Id:          msgID,
			Topic:       item.Topic,
			PartitionId: partID,
			Payload:     item.Payload,
			Timestamp:   time.Now().Unix(),
		}
		results[i] = &pb.PublishResult{
			MessageId:   msgID,
			Topic:       item.Topic,
			PartitionId: partID,
			Success:     true,
		}
	}
	s.storageClient.WriteMessages(ctx, &storagepb.WriteMessagesRequest{Messages: storageMsgs})
	stream.Send(&pb.StreamMessage{
		Type: pb.MessageType_MESSAGE_TYPE_PUBLISH_RESPONSE,
		Payload: &pb.StreamMessage_PublishResp{
			PublishResp: &pb.PublishResponse{RequestId: req.RequestId, Results: results},
		},
	})
}

func (s *BrokerServer) handleSubscribe(ctx context.Context, stream pb.GMQService_StreamServer, req *pb.SubscribeRequest) {
	s.storageClient.SaveConsumer(ctx, &storagepb.SaveConsumerRequest{
		Consumer: &storagepb.ConsumerState{
			ConsumerId:    req.ConsumerId,
			ConsumerGroup: req.ConsumerGroup,
			Topic:         req.Topic,
			LastHeartbeat: time.Now().Unix(),
		},
	})
	s.rebalance(ctx, req.ConsumerGroup, req.Topic)
	stream.Send(&pb.StreamMessage{
		Type: pb.MessageType_MESSAGE_TYPE_SUBSCRIBE_RESPONSE,
		Payload: &pb.StreamMessage_SubscribeResp{
			SubscribeResp: &pb.SubscribeResponse{RequestId: req.RequestId, Success: true},
		},
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
		Type: pb.MessageType_MESSAGE_TYPE_HEARTBEAT_RESPONSE,
		Payload: &pb.StreamMessage_HeartbeatResp{
			HeartbeatResp: &pb.HeartbeatResponse{Success: true},
		},
	})
}

func (s *BrokerServer) rebalance(ctx context.Context, group, topic string) {
	resp, _ := s.storageClient.GetConsumers(ctx, &storagepb.GetConsumersRequest{
		ConsumerGroup: group,
		Topic:         topic,
	})
	if resp == nil || len(resp.Consumers) == 0 {
		return
	}
	assign := make(map[int32]string)
	for i := int32(0); i < s.partitions; i++ {
		assign[i] = resp.Consumers[int(i)%len(resp.Consumers)].ConsumerId
	}
	s.storageClient.UpdateGroupAssignment(ctx, &storagepb.UpdateGroupAssignmentRequest{
		ConsumerGroup:       group,
		Topic:               topic,
		PartitionAssignment: assign,
	})
}

func main() {
	flag.Parse()
	log.Init(*logLevel)
	conn, _ := grpc.Dial(*storageAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	storage := storagepb.NewStorageServiceClient(conn)
	srv := grpc.NewServer()
	pb.RegisterGMQServiceServer(srv, NewBrokerServer(storage, int32(*defaultPartitions)))
	l, _ := net.Listen("tcp", *addr)
	log.Info("Broker 启动", "addr", *addr)
	go srv.Serve(l)
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	<-c
	srv.GracefulStop()
}
