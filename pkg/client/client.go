package client

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	pb "github.com/chowyu12/gmq/proto"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// MessageHandler 消息处理函数
type MessageHandler func(msg *pb.ConsumeMessage) error

// ErrorHandler 错误处理函数
type ErrorHandler func(err error)

// baseClient 内部共享的基础客户端
type baseClient struct {
	conn   *grpc.ClientConn
	client pb.GMQServiceClient
	stream pb.GMQService_StreamClient
	ctx    context.Context
	cancel context.CancelFunc
	mu     sync.RWMutex

	// 共享状态
	pendingRequests map[string]chan *pb.StreamMessage
	requestMu       sync.RWMutex
	errorHandler    ErrorHandler

	// 身份信息
	consumerID    string
	consumerGroup string
	clientID      string
}

func newBaseClient(addr string, errHandler ErrorHandler) (*baseClient, error) {
	conn, err := grpc.Dial(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
		grpc.WithTimeout(5*time.Second))
	if err != nil {
		return nil, fmt.Errorf("连接服务器失败: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	bc := &baseClient{
		conn:            conn,
		client:          pb.NewGMQServiceClient(conn),
		ctx:             ctx,
		cancel:          cancel,
		errorHandler:    errHandler,
		pendingRequests: make(map[string]chan *pb.StreamMessage),
	}

	stream, err := bc.client.Stream(ctx)
	if err != nil {
		conn.Close()
		cancel()
		return nil, fmt.Errorf("建立流连接失败: %w", err)
	}
	bc.stream = stream

	return bc, nil
}

func (c *baseClient) Close() error {
	c.cancel()
	c.mu.Lock()
	if c.stream != nil {
		c.stream.CloseSend()
	}
	c.mu.Unlock()

	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

func (c *baseClient) receiveLoop(msgHandler MessageHandler) {
	for {
		select {
		case <-c.ctx.Done():
			return
		default:
			msg, err := c.stream.Recv()
			if err != nil {
				if err == io.EOF {
					return
				}
				if c.errorHandler != nil {
					c.errorHandler(err)
				}
				return
			}
			c.handleStreamMessage(msg, msgHandler)
		}
	}
}

func (c *baseClient) handleStreamMessage(msg *pb.StreamMessage, msgHandler MessageHandler) {
	switch msg.Type {
	case pb.MessageType_MESSAGE_TYPE_CONSUME_MESSAGE:
		if msgHandler != nil {
			consumeMsg := msg.GetConsumeMsg()
			if err := msgHandler(consumeMsg); err == nil && consumeMsg.Qos == pb.QoS_QOS_AT_LEAST_ONCE {
				// 基础 Ack 逻辑可以放在这里或由 Consumer 处理
			}
		}
	case pb.MessageType_MESSAGE_TYPE_PUBLISH_RESPONSE,
		pb.MessageType_MESSAGE_TYPE_SUBSCRIBE_RESPONSE,
		pb.MessageType_MESSAGE_TYPE_ACK_RESPONSE:
		var reqID string
		if resp := msg.GetPublishResp(); resp != nil {
			reqID = resp.RequestId
		} else if resp := msg.GetSubscribeResp(); resp != nil {
			reqID = resp.RequestId
		} else if resp := msg.GetAckResp(); resp != nil {
			reqID = resp.RequestId
		}
		if reqID != "" {
			c.requestMu.RLock()
			ch, ok := c.pendingRequests[reqID]
			c.requestMu.RUnlock()
			if ok {
				select {
				case ch <- msg:
				default:
				}
			}
		}
	case pb.MessageType_MESSAGE_TYPE_ERROR_RESPONSE:
		if c.errorHandler != nil {
			c.errorHandler(fmt.Errorf("服务器错误: %s", msg.GetErrorResp().Message))
		}
	}
}

func (c *baseClient) heartbeatLoop() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			c.mu.Lock()
			c.stream.Send(&pb.StreamMessage{
				Type: pb.MessageType_MESSAGE_TYPE_HEARTBEAT_REQUEST,
				Payload: &pb.StreamMessage_HeartbeatReq{
					HeartbeatReq: &pb.HeartbeatRequest{
						ConsumerId:    c.consumerID,
						ConsumerGroup: c.consumerGroup,
					},
				},
			})
			c.mu.Unlock()
		}
	}
}

// Producer 生产者客户端
type Producer struct {
	*baseClient
}

type ProducerConfig struct {
	ServerAddr   string
	ClientID     string
	ErrorHandler ErrorHandler
}

// NewProducer 创建生产者
func NewProducer(cfg *ProducerConfig) (*Producer, error) {
	bc, err := newBaseClient(cfg.ServerAddr, cfg.ErrorHandler)
	if err != nil {
		return nil, err
	}
	if cfg.ClientID == "" {
		bc.clientID = "p-" + uuid.New().String()[:8]
	} else {
		bc.clientID = cfg.ClientID
	}

	p := &Producer{baseClient: bc}
	go p.receiveLoop(nil)
	return p, nil
}

func (p *Producer) Publish(ctx context.Context, topic string, payload []byte, opts ...PublishOption) (*pb.PublishResponse, error) {
	req := &pb.PublishRequest{
		RequestId: uuid.New().String(),
		Topic:     topic,
		Payload:   payload,
		Qos:       pb.QoS_QOS_AT_MOST_ONCE,
	}
	for _, opt := range opts {
		opt(req)
	}

	respChan := make(chan *pb.StreamMessage, 1)
	p.requestMu.Lock()
	p.pendingRequests[req.RequestId] = respChan
	p.requestMu.Unlock()
	defer func() {
		p.requestMu.Lock()
		delete(p.pendingRequests, req.RequestId)
		p.requestMu.Unlock()
	}()

	p.mu.Lock()
	err := p.stream.Send(&pb.StreamMessage{
		Type: pb.MessageType_MESSAGE_TYPE_PUBLISH_REQUEST,
		Payload: &pb.StreamMessage_PublishReq{
			PublishReq: req,
		},
	})
	p.mu.Unlock()

	if err != nil {
		return nil, err
	}

	select {
	case resp := <-respChan:
		return resp.GetPublishResp(), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(10 * time.Second):
		return nil, fmt.Errorf("等待发布响应超时")
	}
}

func (p *Producer) CreateTopic(ctx context.Context, topic string, partitions int32, ttlSeconds int64) error {
	resp, err := p.client.CreateTopic(ctx, &pb.CreateTopicRequest{
		Topic:      topic,
		Partitions: partitions,
		TtlSeconds: ttlSeconds,
	})
	if err != nil {
		return err
	}
	if !resp.Success {
		return fmt.Errorf("%s", resp.ErrorMessage)
	}
	return nil
}

// Consumer 消费者客户端
type Consumer struct {
	*baseClient
	messageHandler MessageHandler
	pullIntervalMs int32
	msgChan        chan *pb.ConsumeMessage // 内部消息队列
	workerWg       sync.WaitGroup
}

type ConsumerConfig struct {
	ServerAddr     string
	ConsumerGroup  string // 必填
	ConsumerID     string
	ClientID       string
	PullIntervalMs int32
	PrefetchCount  int // 预取消息数量，默认 100
	MessageHandler MessageHandler
	ErrorHandler   ErrorHandler
}

// NewConsumer 创建消费者
func NewConsumer(cfg *ConsumerConfig) (*Consumer, error) {
	if cfg.ConsumerGroup == "" {
		return nil, fmt.Errorf("消费者必须提供 ConsumerGroup 配置")
	}

	bc, err := newBaseClient(cfg.ServerAddr, cfg.ErrorHandler)
	if err != nil {
		return nil, err
	}

	bc.consumerGroup = cfg.ConsumerGroup
	if cfg.ConsumerID == "" {
		bc.consumerID = "c-" + uuid.New().String()[:8]
	} else {
		bc.consumerID = cfg.ConsumerID
	}
	if cfg.ClientID == "" {
		bc.clientID = "cli-" + uuid.New().String()[:8]
	} else {
		bc.clientID = cfg.ClientID
	}

	if cfg.PrefetchCount <= 0 {
		cfg.PrefetchCount = 100
	}

	c := &Consumer{
		baseClient:     bc,
		messageHandler: cfg.MessageHandler,
		pullIntervalMs: cfg.PullIntervalMs,
		msgChan:        make(chan *pb.ConsumeMessage, cfg.PrefetchCount),
	}

	// 启动接收循环 (将消息放入 msgChan)
	go c.receiveLoop(c.enqueueMessage)
	// 启动消息处理 Worker
	go c.startWorker()
	// 启动心跳
	go c.heartbeatLoop()

	return c, nil
}

// enqueueMessage 仅仅负责将消息放入队列，不处理业务逻辑，不会阻塞接收循环
func (c *Consumer) enqueueMessage(msg *pb.ConsumeMessage) error {
	select {
	case c.msgChan <- msg:
		return nil
	case <-c.ctx.Done():
		return c.ctx.Err()
	}
}

// startWorker 负责从队列中取出消息并调用用户的处理逻辑
func (c *Consumer) startWorker() {
	c.workerWg.Add(1)
	defer c.workerWg.Done()

	for {
		select {
		case <-c.ctx.Done():
			return
		case msg, ok := <-c.msgChan:
			if !ok {
				return
			}
			if c.messageHandler != nil {
				// 执行用户业务逻辑
				err := c.messageHandler(msg)
				if err == nil && msg.Qos == pb.QoS_QOS_AT_LEAST_ONCE {
					// 自动 Ack
					c.Ack(context.Background(), msg)
				}
			}
		}
	}
}

func (c *Consumer) Subscribe(ctx context.Context, topic string, opts ...SubscribeOption) error {
	req := &pb.SubscribeRequest{
		RequestId:      uuid.New().String(),
		Topic:          topic,
		ConsumerGroup:  c.consumerGroup,
		ConsumerId:     c.consumerID,
		Qos:            pb.QoS_QOS_AT_MOST_ONCE,
		PullIntervalMs: c.pullIntervalMs,
		ClientId:       c.clientID,
	}
	for _, opt := range opts {
		opt(req)
	}

	respChan := make(chan *pb.StreamMessage, 1)
	c.requestMu.Lock()
	c.pendingRequests[req.RequestId] = respChan
	c.requestMu.Unlock()
	defer func() {
		c.requestMu.Lock()
		delete(c.pendingRequests, req.RequestId)
		c.requestMu.Unlock()
	}()

	c.mu.Lock()
	err := c.stream.Send(&pb.StreamMessage{
		Type: pb.MessageType_MESSAGE_TYPE_SUBSCRIBE_REQUEST,
		Payload: &pb.StreamMessage_SubscribeReq{
			SubscribeReq: req,
		},
	})
	c.mu.Unlock()

	if err != nil {
		return err
	}

	select {
	case resp := <-respChan:
		subResp := resp.GetSubscribeResp()
		if !subResp.Success {
			return fmt.Errorf("订阅失败: %s", subResp.ErrorMessage)
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(10 * time.Second):
		return fmt.Errorf("等待订阅响应超时")
	}
}

func (c *Consumer) Ack(ctx context.Context, msg *pb.ConsumeMessage) error {
	req := &pb.AckRequest{
		RequestId:     uuid.New().String(),
		MessageId:     msg.MessageId,
		Topic:         msg.Topic,
		PartitionId:   msg.PartitionId,
		ConsumerGroup: c.consumerGroup,
		ConsumerId:    c.consumerID,
		Offset:        msg.Offset,
	}

	c.mu.Lock()
	err := c.stream.Send(&pb.StreamMessage{
		Type: pb.MessageType_MESSAGE_TYPE_ACK_REQUEST,
		Payload: &pb.StreamMessage_AckReq{
			AckReq: req,
		},
	})
	c.mu.Unlock()

	return err
}

// PublishOption 发布选项
type PublishOption func(*pb.PublishRequest)

func WithPartitionKey(key string) PublishOption {
	return func(req *pb.PublishRequest) { req.PartitionKey = key }
}

func WithPartitionID(id int32) PublishOption {
	return func(req *pb.PublishRequest) { req.PartitionId = id }
}

func WithProperties(props map[string]string) PublishOption {
	return func(req *pb.PublishRequest) { req.Properties = props }
}

func WithQoS(qos pb.QoS) PublishOption {
	return func(req *pb.PublishRequest) { req.Qos = qos }
}

// SubscribeOption 订阅选项
type SubscribeOption func(*pb.SubscribeRequest)

func WithPartitions(partitions []int32) SubscribeOption {
	return func(req *pb.SubscribeRequest) { req.Partitions = partitions }
}

func WithSubscribeQoS(qos pb.QoS) SubscribeOption {
	return func(req *pb.SubscribeRequest) { req.Qos = qos }
}
