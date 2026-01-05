package main

import (
	"context"
	"flag"
	"fmt"
	"hash/fnv"
	"io"
	"math/rand"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/chowyu12/gmq/pkg/log"
	pb "github.com/chowyu12/gmq/proto"
	storagepb "github.com/chowyu12/gmq/proto/storage"
	"github.com/redis/go-redis/v9"
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
	redisClient   *redis.Client // Redis client for pub/sub notifications

	// Global Signal Hub: one Redis subscription for all consumers
	listenersMu sync.RWMutex
	listeners   map[string]map[chan struct{}]struct{}
}

func NewBrokerServer(client storagepb.StorageServiceClient, partitions int32, redisAddr string) *BrokerServer {
	rdb := redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})
	s := &BrokerServer{
		storageClient: client,
		partitions:    int32(partitions),
		redisClient:   rdb,
		listeners:     make(map[string]map[chan struct{}]struct{}),
	}
	go s.startSignalLoop()
	return s
}

// startSignalLoop runs a single background goroutine to listen for ALL partition signals
func (s *BrokerServer) startSignalLoop() {
	ctx := context.Background()
	// Use PSubscribe to catch all partition signals with one connection
	pubsub := s.redisClient.PSubscribe(ctx, "gmq:signal:*")
	defer pubsub.Close()

	ch := pubsub.Channel()
	for msg := range ch {
		s.listenersMu.RLock()
		// msg.Channel is the actual channel name like gmq:signal:topic:0
		if nodeListeners, ok := s.listeners[msg.Channel]; ok {
			for listener := range nodeListeners {
				select {
				case listener <- struct{}{}:
				default:
					// Listener is busy, skip to avoid blocking the hub
				}
			}
		}
		s.listenersMu.RUnlock()
	}
}

func (s *BrokerServer) Stream(stream pb.GMQService_StreamServer) error {
	ctx := stream.Context()
	// Session state, fixed once Subscribe is successful
	var (
		sessionConsumerID    string
		sessionConsumerGroup string
		sessionTopic         string
		sessionSignalCh      chan struct{}
		sessionChannels      []string
	)

	// Clean up session registration on exit
	defer func() {
		if sessionSignalCh != nil {
			s.unregisterSession(sessionSignalCh, sessionChannels)
		}
	}()

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

			sessionConsumerID = req.ConsumerId
			sessionConsumerGroup = req.ConsumerGroup
			sessionTopic = req.Topic
			if sessionSignalCh == nil {
				sessionSignalCh = make(chan struct{}, 100) // Large buffer to avoid drops
			}

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
			// Use the persistent session signal channel
			s.handlePull(ctx, stream, sessionConsumerID, sessionConsumerGroup, sessionTopic, req.Limit, sessionSignalCh)

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

func (s *BrokerServer) registerSession(ch chan struct{}, channels []string) {
	s.listenersMu.Lock()
	defer s.listenersMu.Unlock()
	for _, c := range channels {
		if s.listeners[c] == nil {
			s.listeners[c] = make(map[chan struct{}]struct{})
		}
		s.listeners[c][ch] = struct{}{}
	}
}

func (s *BrokerServer) unregisterSession(ch chan struct{}, channels []string) {
	s.listenersMu.Lock()
	defer s.listenersMu.Unlock()
	for _, c := range channels {
		if s.listeners[c] != nil {
			delete(s.listeners[c], ch)
			if len(s.listeners[c]) == 0 {
				delete(s.listeners, c)
			}
		}
	}
}

func (s *BrokerServer) getConsumerChannels(ctx context.Context, group, topic, consumerID string) []string {
	assignResp, err := s.storageClient.GetGroupAssignment(ctx, &storagepb.GetGroupAssignmentRequest{
		ConsumerGroup: group, Topic: topic,
	})
	if err != nil || !assignResp.Success {
		return nil
	}

	var channels []string
	for partID, assignedID := range assignResp.PartitionAssignment {
		if assignedID == consumerID {
			channels = append(channels, fmt.Sprintf("gmq:signal:%s:%d", topic, partID))
		}
	}
	return channels
}

func (s *BrokerServer) handlePull(ctx context.Context, stream pb.GMQService_StreamServer, consumerID, group, topic string, limit int32, signalCh chan struct{}) {
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
	var currentChannels []string
	for partID, assignedID := range assignResp.PartitionAssignment {
		if assignedID == consumerID {
			myPartitions = append(myPartitions, partID)
			currentChannels = append(currentChannels, fmt.Sprintf("gmq:signal:%s:%d", topic, partID))
		}
	}

	if len(myPartitions) == 0 {
		// If no partitions assigned yet, wait a bit and return
		time.Sleep(100 * time.Millisecond)
		return
	}

	// Register current partitions to receive signals
	s.registerSession(signalCh, currentChannels)
	defer s.unregisterSession(signalCh, currentChannels)

	// Try regular fetch
	var allItems []*pb.MessageItem
	for _, partID := range myPartitions {
		resp, err := s.storageClient.FetchMessages(ctx, &storagepb.FetchMessagesRequest{
			Topic:         topic,
			PartitionId:   partID,
			ConsumerGroup: group,
			ConsumerId:    consumerID,
			Limit:         limit,
		})
		if err == nil && resp != nil && len(resp.Messages) > 0 {
			for _, m := range resp.Messages {
				allItems = append(allItems, &pb.MessageItem{
					Topic:       m.Topic,
					PartitionId: m.PartitionId,
					Offset:      m.Offset,
					Payload:     m.Payload,
				})
			}
		}
	}

	if len(allItems) > 0 {
		s.sendConsumeMessage(stream, allItems)
		return
	}

	// Try XCLAIM
	for _, partID := range myPartitions {
		claimResp, _ := s.storageClient.ClaimMessages(ctx, &storagepb.ClaimMessagesRequest{
			Topic: topic, PartitionId: partID, ConsumerGroup: group, ConsumerId: consumerID, MinIdleTimeMs: 10000, Limit: limit,
		})
		if claimResp != nil && len(claimResp.Messages) > 0 {
			var items []*pb.MessageItem
			for _, m := range claimResp.Messages {
				items = append(items, &pb.MessageItem{
					Topic:       m.Topic,
					PartitionId: m.PartitionId,
					Offset:      m.Offset,
					Payload:     m.Payload,
				})
			}
			s.sendConsumeMessage(stream, items)
			return
		}
	}

	// Block until a signal arrives or short timeout
	// Shortening timeout to 1s to keep the stream responsive to other requests (Acks, etc.)
	select {
	case <-signalCh:
	case <-ctx.Done():
	case <-time.After(1 * time.Second):
	}
}

func (s *BrokerServer) sendConsumeMessage(stream pb.GMQService_StreamServer, items []*pb.MessageItem) {
	stream.Send(&pb.StreamMessage{
		Type: pb.MessageType_MESSAGE_TYPE_CONSUME_MESSAGE,
		Payload: &pb.StreamMessage_ConsumeMsg{
			ConsumeMsg: &pb.ConsumeMessage{Items: items},
		},
	})
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
			Topic:       item.Topic,
			PartitionId: partID,
			Payload:     item.Payload,
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
	// Pass Redis address for pub/sub notifications
	brokerServer := NewBrokerServer(storage, int32(*defaultPartitions), "localhost:6379")
	pb.RegisterGMQServiceServer(srv, brokerServer)
	l, _ := net.Listen("tcp", *addr)
	log.Info("Broker started", "addr", *addr)
	go srv.Serve(l)
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	<-c
	srv.GracefulStop()
}
