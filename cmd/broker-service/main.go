package main

import (
	"context"
	"flag"
	"hash/fnv"
	"io"
	"math/rand"
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
	addr              = flag.String("addr", ":50051", "Broker service address")
	storageAddr       = flag.String("storage", "localhost:50052", "Storage service address")
	defaultPartitions = flag.Int("partitions", 4, "Default number of partitions")
	logLevel          = flag.String("log-level", "info", "Log level")
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
	// Session 级状态，一旦 Subscribe 成功即固定
	var (
		sessionConsumerID    string
		sessionConsumerGroup string
		sessionTopic         string
	)

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
			req := msg.GetSubscribeReq()
			// Handshake: initialize Session
			sessionConsumerID = req.ConsumerId
			sessionConsumerGroup = req.ConsumerGroup
			sessionTopic = req.Topic
			s.handleSubscribe(ctx, stream, req)

		case pb.MessageType_MESSAGE_TYPE_ACK_REQUEST:
			req := msg.GetAckReq()
			cid, cgrp := req.ConsumerId, req.ConsumerGroup
			if cid == "" {
				cid = sessionConsumerID
			}
			if cgrp == "" {
				cgrp = sessionConsumerGroup
			}
			s.handleAck(ctx, stream, cid, cgrp, req)

		case pb.MessageType_MESSAGE_TYPE_PULL_REQUEST:
			req := msg.GetPullReq()
			// Since sessions are initialized and proto is cleaned up, use session params
			s.handlePull(ctx, stream, sessionConsumerID, sessionConsumerGroup, sessionTopic, req.Limit)

		case pb.MessageType_MESSAGE_TYPE_HEARTBEAT_REQUEST:
			req := msg.GetHeartbeatReq()
			cid, cgrp := req.ConsumerId, req.ConsumerGroup
			if cid == "" {
				cid = sessionConsumerID
			}
			if cgrp == "" {
				cgrp = sessionConsumerGroup
			}
			s.handleHeartbeat(ctx, stream, cid, cgrp, req)
		}
	}
}

func (s *BrokerServer) handlePull(ctx context.Context, stream pb.GMQService_StreamServer, consumerID, group, topic string, limit int32) {
	if consumerID == "" || group == "" || topic == "" {
		return
	}

	assignResp, err := s.storageClient.GetGroupAssignment(ctx, &storagepb.GetGroupAssignmentRequest{
		ConsumerGroup: group, Topic: topic,
	})
	if err != nil || !assignResp.Success {
		return
	}

	var myPartitions []int32
	for partID, assignedID := range assignResp.PartitionAssignment {
		if assignedID == consumerID {
			myPartitions = append(myPartitions, partID)
		}
	}

	if len(myPartitions) == 0 {
		return
	}

	// Wait up to 2 seconds for data
	timeout := time.After(2 * time.Second)
	for {
		var allItems []*pb.MessageItem
		for _, partID := range myPartitions {
			// Each FetchMessages already has a small block (e.g. 200ms) in Redis
			resp, err := s.storageClient.FetchMessages(ctx, &storagepb.FetchMessagesRequest{
				Topic:         topic,
				PartitionId:   partID,
				ConsumerGroup: group,
				ConsumerId:    consumerID,
				Limit:         limit,
			})

			if err != nil || resp == nil || len(resp.Messages) == 0 {
				continue
			}

			for _, m := range resp.Messages {
				allItems = append(allItems, &pb.MessageItem{
					MessageId:   m.Id,
					Topic:       m.Topic,
					PartitionId: m.PartitionId,
					Offset:      m.Offset,
					Payload:     m.Payload,
					Properties:  m.Properties,
					Timestamp:   m.Timestamp,
					Key:         m.Key,
				})
			}
		}

		if len(allItems) > 0 {
			if err := stream.Send(&pb.StreamMessage{
				Type: pb.MessageType_MESSAGE_TYPE_CONSUME_MESSAGE,
				Payload: &pb.StreamMessage_ConsumeMsg{
					ConsumeMsg: &pb.ConsumeMessage{Items: allItems},
				},
			}); err != nil {
				log.WithContext(ctx).Error("Failed to push consumed messages", "error", err)
			}
			return
		}

		select {
		case <-timeout:
			return
		case <-ctx.Done():
			return
		default:
			// If no partitions have data, yield momentarily
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func (s *BrokerServer) handleAck(ctx context.Context, stream pb.GMQService_StreamServer, consumerID, group string, req *pb.AckRequest) {
	if consumerID == "" || group == "" {
		return
	}

	// Group AckItems by Topic and Partition to call Storage service efficiently
	ackGroups := make(map[string]map[int32][]int64)
	for _, item := range req.Items {
		if _, ok := ackGroups[item.Topic]; !ok {
			ackGroups[item.Topic] = make(map[int32][]int64)
		}
		ackGroups[item.Topic][item.PartitionId] = append(ackGroups[item.Topic][item.PartitionId], item.Offset)
	}

	for topic, partitions := range ackGroups {
		for partID, offsets := range partitions {
			s.storageClient.AcknowledgeMessages(ctx, &storagepb.AcknowledgeMessagesRequest{
				Topic:         topic,
				PartitionId:   partID,
				ConsumerGroup: group,
				Offsets:       offsets,
			})
		}
	}

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
			if item.PartitionKey != "" {
				h := fnv.New32a()
				h.Write([]byte(item.PartitionKey))
				partID = int32(h.Sum32() % uint32(s.partitions))
			} else {
				partID = rand.Int31n(s.partitions)
			}
		}

		storageMsgs[i] = &storagepb.Message{
			Id:             msgID,
			Topic:          item.Topic,
			PartitionId:    partID,
			Payload:        item.Payload,
			Timestamp:      time.Now().Unix(),
			Key:            item.PartitionKey,
			ProducerId:     item.ProducerId,
			SequenceNumber: item.SequenceNumber,
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
	if req.ConsumerId == "" || req.ConsumerGroup == "" {
		return
	}
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

func (s *BrokerServer) handleHeartbeat(ctx context.Context, stream pb.GMQService_StreamServer, consumerID, group string, req *pb.HeartbeatRequest) {
	if consumerID == "" || group == "" {
		return
	}
	s.storageClient.SaveConsumer(ctx, &storagepb.SaveConsumerRequest{
		Consumer: &storagepb.ConsumerState{
			ConsumerId:    consumerID,
			ConsumerGroup: group,
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
	now := time.Now().Unix()
	var activeConsumers []*storagepb.ConsumerState
	for _, c := range resp.Consumers {
		if now-c.LastHeartbeat < 30 {
			activeConsumers = append(activeConsumers, c)
		}
	}
	if len(activeConsumers) == 0 {
		return
	}
	assign := make(map[int32]string)
	for i := int32(0); i < s.partitions; i++ {
		assign[i] = activeConsumers[int(i)%len(activeConsumers)].ConsumerId
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
	rand.Seed(time.Now().UnixNano())
	conn, _ := grpc.Dial(*storageAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	storage := storagepb.NewStorageServiceClient(conn)
	srv := grpc.NewServer()
	pb.RegisterGMQServiceServer(srv, NewBrokerServer(storage, int32(*defaultPartitions)))
	l, _ := net.Listen("tcp", *addr)
	log.Info("Broker started", "addr", *addr)
	go srv.Serve(l)
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	<-c
	srv.GracefulStop()
}
