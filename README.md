# GMQ (Go Message Queue)

GMQ is a high-performance, production-grade distributed message queue system based on gRPC bidirectional Stream protocol. It adopts a storage and distribution separation architecture, supporting Topic, Partition, consumer group load balancing, and QoS 0/1 message quality guarantees.

## 🚀 Core Features

- **High-Performance Communication**: Based on gRPC Bidirectional Stream, maintaining long connections between clients and Broker.
- **Modern Architecture**:
  - **Broker Service**: Integrates connection gateway and distribution logic, completely stateless, supports unlimited horizontal scaling.
  - **Storage Service**: Independent storage layer, supports message persistence and strong consistency management of state (consumers/consumer groups).
- **Strong Consistency State**: Storage layer uses Redis/DragonflyDB with atomic operations (Lua scripts) to ensure consumer group metadata integrity.
- **Flexible Routing**: Supports Partition Key (Hash), specified Partition ID, and random assignment.
- **Automatic Management**: Supports automatic Topic creation (default 4 partitions), also supports manual interface creation.
- **Parameterized Connections**: Clients can customize ClientID and message pull interval.
- **Reliability Guarantees**: Supports QoS 1 (At-Least-Once) acknowledgment mechanism, consumption progress persisted in storage.
- **Containerization Support**: Pre-configured Docker Compose deployment configuration.

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
│  - Atomic Fetch (Lua Scripts)                           │
└───────────────────────────────────────────────────────────┘
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

// Send a QoS 1 message with partition key
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
| `make docker` | Start services with Docker Compose |
| `make docker-logs` | View container logs |
| `make clean` | Clean build artifacts and storage data |
| `make proto` | Regenerate gRPC protocol code |


---

**License**: MIT | **Go Version**: 1.24+
