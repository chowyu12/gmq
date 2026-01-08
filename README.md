# GMQ (Go Message Queue)

GMQ is a high-performance, production-grade distributed message queue system based on gRPC bidirectional Stream protocol. It adopts a storage and distribution separation architecture, supporting Topic, Partition, and consumer group load balancing.

## 🚀 Performance & Innovation (Why GMQ?)

GMQ is engineered for extreme performance and reliability, achieving over **140,000+ msg/s** on a single node with minimal resource footprint.

- **Zero-Copy Architecture**: Stores raw Protobuf binary streams directly in Redis. No JSON/BSON overhead, drastically reducing CPU usage and memory fragmentation.
- **Global Signal Hub**: Implements a PSubscribe-based notification engine. A single Broker-to-Redis connection can wake up thousands of consumer sessions in sub-millisecond time. No polling, zero idle CPU waste.
- **Session-Based Connectivity**: Handshake-driven identity management. Consumers bind their state once, reducing network packet size and protocol overhead for every subsequent pull.
- **Event-Driven Push**: Leverages Redis Pub/Sub combined with persistent gRPC streams to deliver messages instantly as they arrive, eliminating the traditional "polling delay".
- **Self-Healing Consumer Groups**: Built-in support for Redis PEL (Pending Entries List) and XCLAIM. Messages from crashed consumers are automatically reassigned and recovered.
- **Compact & Efficient**: Achieving high-performance distributed queuing in **under 2,000 lines of Go code** — proof of clean engineering and efficient architecture.
- **Admin Portal (Management UI)**: Built-in management dashboard for real-time monitoring of topics, consumer groups, and message exploration.

---

## 🏗️ System Architecture

```
┌─────────────────┐      ┌──────────────────────────────────┐
│   Producers     │      │           Consumers              │
└────────┬────────┘      └────────────────┬─────────────────┘
         │                                │
         │ gRPC Stream                    │ gRPC Stream
         ↓                                ↓
         ┌─────────────────────┼─────────────────────┐
         ↓                     ↓                     ↓
┌─────────────────┐   ┌─────────────────┐   ┌─────────────────┐
│    Broker-1     │   │    Broker-2     │   │    Broker-N     │
│ (Stateless)     │   │ (Stateless)     │   │ (Stateless)     │
└────────┬────────┘   └────────┬────────┘   └────────┬────────┘
         │                     │                     │
         └──────────────┬──────┴─────────────────────┘
                        │ gRPC
                        ↓
┌───────────────────────────────────────────────────────────┐
│                 Storage Service (Stateful)                │
├───────────────────────────────────────────────────────────┤
│  - Message Logs (Redis Streams)                          │
│  - Consumer/Group States (Redis Hash)                    │
│  - Native Consumer Groups (XREADGROUP/XACK)              │
└───────────────────────────────────────────────────────────┘
                        ↑
                        │ gRPC
                ┌───────┴───────┐
                │ Admin Portal  │
                │   (Port 8080) │
                └───────────────┘
```

## 📂 Project Structure

```
gmq/
├── cmd/
│   ├── broker-service/       # Gateway and distribution service (stateless)
│   └── storage-service/      # Storage service (stateful)
├── internal/
│   └── storage/              # Storage engine with Redis/DragonflyDB
├── pkg/
│   ├── client/               # Client SDK
│   └── log/                  # Logging utilities
├── proto/                    # gRPC protocol definitions (Broker/Storage)
├── examples/                 # Producer, consumer, consumer group examples
├── docker-compose.yml        # One-click deployment orchestration
└── Makefile                  # Automated build tools
```

## 🛠️ Quick Start

### Method 1: Docker Compose Deployment (Recommended)

```bash
# Start all services (1 Storage + 2 Broker)
make docker

# View logs
make docker-logs
```

### Method 2: Local Manual Compilation and Startup

```bash
# 1. Build binaries
make build

# 2. Start storage service
./bin/gmq-storage-service -redis-addr localhost:6379

# 3. Start broker service
./bin/gmq-broker-service -storage localhost:50052
```

## 💻 Client Usage Examples

### Producer

```go
producer, _ := client.NewProducer(&client.ProducerConfig{
    ServerAddr: "localhost:50051", // Connect to Broker port
})
defer producer.Close()

// Send a message with partition key
items := []*pb.PublishItem{
    {
        Topic:       "orders",
        Payload:     []byte("Order#1001"),
        PartitionKey: "user_id_123", // Hash-based routing
    },
}
resp, _ := producer.Publish(ctx, items)
```

### Consumer

```go
consumer, _ := client.NewConsumer(&client.ConsumerConfig{
    ServerAddr:    "localhost:50051",
    ConsumerGroup: "order-processors",
    Topic:         "orders",
})
defer consumer.Close()

for {
    msgCtx, err := consumer.Receive(ctx, 5*time.Second)
    if err != nil {
        continue
    }
    for _, msg := range msgCtx.Messages() {
        fmt.Printf("Received order: %s\n", string(msg.Payload))
    }
    msgCtx.Ack()
}
```

## 📊 Operations Commands

| Command | Description |
|---------|-------------|
| `make build` | Build all binaries |
| `make up` | Start services with Docker Compose |
| `make admin` | Run admin dashboard service |
| `make logs` | View container logs |
| `make ps` | View container status |
| `make clean` | Clean build artifacts and storage data |
| `make proto` | Regenerate gRPC protocol code |


---

**License**: MIT | **Go Version**: 1.24+
