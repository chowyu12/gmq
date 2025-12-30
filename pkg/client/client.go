package client

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/chowyu12/gmq/pkg/log"
	pb "github.com/chowyu12/gmq/proto"
	"github.com/rs/xid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// MessageContext 消息处理上下文
type MessageContext interface {
	context.Context
	Messages() []*pb.MessageItem
}

type messageContext struct {
	context.Context
	msgs     []*pb.MessageItem
	consumer *Consumer
}

func (m *messageContext) Messages() []*pb.MessageItem { return m.msgs }

// MessageHandler 消息处理函数
type MessageHandler func(ctx MessageContext) error

// ErrorHandler 错误处理函数
type ErrorHandler func(err error)

// rawMessageHandler 基础消息处理函数（内部使用）
type rawMessageHandler func(*pb.ConsumeMessage) error

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
	serverAddr    string
	isClosed      bool
}

func newBaseClient(addr string, errHandler ErrorHandler) (*baseClient, error) {
	bc := &baseClient{
		serverAddr:      addr,
		errorHandler:    errHandler,
		pendingRequests: make(map[string]chan *pb.StreamMessage),
	}

	if err := bc.connect(); err != nil {
		return nil, err
	}

	return bc, nil
}

func (c *baseClient) connect() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn != nil {
		c.conn.Close()
	}

	conn, err := grpc.Dial(c.serverAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
		grpc.WithTimeout(5*time.Second))
	if err != nil {
		return fmt.Errorf("连接服务器失败: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	c.conn = conn
	c.client = pb.NewGMQServiceClient(conn)
	c.ctx = ctx
	c.cancel = cancel

	stream, err := c.client.Stream(ctx)
	if err != nil {
		conn.Close()
		cancel()
		return fmt.Errorf("建立流连接失败: %w", err)
	}
	c.stream = stream
	return nil
}

func (c *baseClient) Close() error {
	c.mu.Lock()
	c.isClosed = true
	c.mu.Unlock()

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

func (c *baseClient) receiveLoop(msgHandler rawMessageHandler, onReconnect func()) {
	for {
		msg, err := c.stream.Recv()
		if err != nil {
			c.mu.RLock()
			closed := c.isClosed
			c.mu.RUnlock()

			// 如果是主动关闭，则直接退出，不记录错误日志
			if closed {
				return
			}

			if err == io.EOF {
				log.Info("服务器已关闭连接，尝试重连...")
			} else {
				log.Error("接收流消息失败，准备重连", "error", err)
			}

			c.reconnect(msgHandler, onReconnect)
			return
		}
		c.handleStreamMessage(msg, msgHandler)
	}
}

func (c *baseClient) reconnect(msgHandler rawMessageHandler, onReconnect func()) {
	backoff := 1 * time.Second
	maxBackoff := 30 * time.Second

	for {
		c.mu.RLock()
		if c.isClosed {
			c.mu.RUnlock()
			return
		}
		c.mu.RUnlock()

		log.Info("正在尝试重新连接...", "addr", c.serverAddr, "wait", backoff)
		time.Sleep(backoff)

		if err := c.connect(); err == nil {
			log.Info("重连成功")
			// 重新启动接收循环
			go c.receiveLoop(msgHandler, onReconnect)
			// 执行重连后的恢复逻辑（如重订阅）
			if onReconnect != nil {
				onReconnect()
			}
			return
		}

		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}

func (c *baseClient) handleStreamMessage(msg *pb.StreamMessage, msgHandler rawMessageHandler) {
	switch msg.Type {
	case pb.MessageType_MESSAGE_TYPE_CONSUME_MESSAGE:
		if msgHandler != nil {
			msgHandler(msg.GetConsumeMsg())
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
			err := fmt.Errorf("服务器错误: %s", msg.GetErrorResp().Message)
			c.errorHandler(err)
			log.Error("收到服务器错误响应", "error", err)
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
	producerID     string
	sequenceNumber int64
	seqMu          sync.Mutex
}

type ProducerConfig struct {
	ServerAddr   string
	ProducerID   string // 生产者 ID，用于幂等
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
		bc.clientID = "p-" + xid.New().String()
	} else {
		bc.clientID = cfg.ClientID
	}

	producerID := cfg.ProducerID
	if producerID == "" {
		producerID = "prod-" + xid.New().String()
	}

	p := &Producer{
		baseClient: bc,
		producerID: producerID,
	}
	go p.receiveLoop(nil, nil)
	return p, nil
}

func (p *Producer) Publish(ctx context.Context, items []*pb.PublishItem) (*pb.PublishResponse, error) {
	if len(items) == 0 {
		return nil, fmt.Errorf("发送项不能为空")
	}

	p.seqMu.Lock()
	for _, item := range items {
		if item.ProducerId == "" {
			item.ProducerId = p.producerID
		}
		p.sequenceNumber++
		item.SequenceNumber = p.sequenceNumber
	}
	p.seqMu.Unlock()

	req := &pb.PublishRequest{
		RequestId: xid.New().String(),
		Items:     items,
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

	// 自动重连时恢复订阅
	subscribedTopics map[string][]SubscribeOption
	subMu            sync.RWMutex
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
		bc.consumerID = "c-" + xid.New().String()
	} else {
		bc.consumerID = cfg.ConsumerID
	}
	if cfg.ClientID == "" {
		bc.clientID = "cli-" + xid.New().String()
	} else {
		bc.clientID = cfg.ClientID
	}

	if cfg.PrefetchCount <= 0 {
		cfg.PrefetchCount = 100
	}

	c := &Consumer{
		baseClient:       bc,
		messageHandler:   cfg.MessageHandler,
		pullIntervalMs:   cfg.PullIntervalMs,
		msgChan:          make(chan *pb.ConsumeMessage, cfg.PrefetchCount),
		subscribedTopics: make(map[string][]SubscribeOption),
	}

	// 启动接收循环 (将消息放入 msgChan)，并注册重连回调
	go c.receiveLoop(c.enqueueMessage, c.onReconnect)
	// 启动消息处理 Worker
	go c.startWorker()
	// 启动心跳
	go c.heartbeatLoop()

	return c, nil
}

func (c *Consumer) onReconnect() {
	log.Info("正在恢复订阅...")
	c.subMu.RLock()
	defer c.subMu.RUnlock()

	for topic, opts := range c.subscribedTopics {
		if err := c.Subscribe(context.Background(), topic, opts...); err != nil {
			log.Error("恢复订阅失败", "topic", topic, "error", err)
		} else {
			log.Info("订阅已恢复", "topic", topic)
		}
	}
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
			if c.messageHandler != nil && len(msg.Items) > 0 {
				// 创建消息上下文，包含整批消息
				mctx := &messageContext{
					Context:  c.ctx,
					msgs:     msg.Items,
					consumer: c,
				}
				// 执行用户业务逻辑（批量处理）
				_ = c.messageHandler(mctx)
			}
		}
	}
}

func (c *Consumer) Subscribe(ctx context.Context, topic string, opts ...SubscribeOption) error {
	// 记录订阅信息以便重连
	c.subMu.Lock()
	c.subscribedTopics[topic] = opts
	c.subMu.Unlock()

	req := &pb.SubscribeRequest{
		RequestId:      xid.New().String(),
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

func (c *Consumer) Ack(ctx context.Context, items []*pb.MessageItem) error {
	if len(items) == 0 {
		return nil
	}

	ackItems := make([]*pb.AckItem, len(items))
	for i, item := range items {
		ackItems[i] = &pb.AckItem{
			Topic:       item.Topic,
			PartitionId: item.PartitionId,
			MessageId:   item.MessageId,
			Offset:      item.Offset,
		}
	}

	req := &pb.AckRequest{
		RequestId:     xid.New().String(),
		ConsumerGroup: c.consumerGroup,
		ConsumerId:    c.consumerID,
		Items:         ackItems,
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
type PublishOption func(*pb.PublishItem)

func WithPartitionKey(key string) PublishOption {
	return func(req *pb.PublishItem) { req.PartitionKey = key }
}

func WithPartitionID(id int32) PublishOption {
	return func(req *pb.PublishItem) { req.PartitionId = id }
}

func WithProperties(props map[string]string) PublishOption {
	return func(req *pb.PublishItem) { req.Properties = props }
}

func WithQoS(qos pb.QoS) PublishOption {
	return func(req *pb.PublishItem) { req.Qos = qos }
}

// SubscribeOption 订阅选项
type SubscribeOption func(*pb.SubscribeRequest)

func WithPartitions(partitions []int32) SubscribeOption {
	return func(req *pb.SubscribeRequest) { req.Partitions = partitions }
}

func WithSubscribeQoS(qos pb.QoS) SubscribeOption {
	return func(req *pb.SubscribeRequest) { req.Qos = qos }
}
