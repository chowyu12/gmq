# GMQ 架构演进总结：Broker 合并与一致性存储

## 🚀 核心变更

根据需求，我们对 GMQ 进行了深度的架构优化和功能增强：

### 1. 服务合并：Gateway + Dispatcher → Broker
我们将原有的 `gateway-service`（负责连接）和 `dispatcher-service`（负责逻辑）合并为统一的 **Broker Service**。

- **优势**：
  - **降低延迟**：消除了 Gateway 和 Dispatcher 之间的内部 gRPC 调用开销。
  - **简化拓扑**：减少了一个独立维护的服务组件，降低了运维复杂度。
  - **无状态扩展**：Broker Service 依然保持无状态设计，可以根据负载轻松水平扩展。

### 2. 状态持久化：从内存到 Storage Service
消费者信息、消费组状态和分区分配关系不再存储在 Broker 内存中，而是统一持久化到 **Storage Service**。

- **实现方式**：
  - 在 `storage.proto` 中增加了状态管理接口。
  - Broker 在处理 `Subscribe`、`Ack`、`Heartbeat` 等请求时，同步调用 Storage Service 进行状态更新。
  - 消费者下线时，Broker 会通知 Storage Service 清理状态。

### 3. 存储一致性增强：引入 SQLite
Storage Service 内部引入了 **SQLite** (基于 `modernc.org/sqlite` 纯 Go 实现) 作为状态存储后端。

- **优势**：
  - **强一致性**：利用数据库事务保证消费者状态和分配关系的原子性。
  - **零依赖**：纯 Go 实现，无需 CGO，无需安装 C 编译器即可跨平台构建。
  - **易于备份**：所有状态存储在 `state.db` 单文件中，极大简化了备份和恢复。

## 📊 新旧架构对比

| 特性 | 旧架构 | 新架构 (当前) |
|-----|--------|-------------|
| 组件结构 | Gateway + Dispatcher | **Broker Service** |
| 消费者状态 | Dispatcher 内存 (重启丢失) | **Storage Service (持久化)** |
| 存储一致性 | 简单文件写入 | **SQLite 数据库事务** |
| 网络跳数 | Client → Gateway → Dispatcher | **Client → Broker** |
| 扩展性 | 需协调多个 Dispatcher 状态 | **Broker 全无状态，按需水平扩展** |

## 🛠️ 快速开始 (新版本)

### 1. 构建所有分布式服务
```bash
make build-dist
```

### 2. 启动分布式集群
```bash
# 终端 1: 启动持久化存储
make run-storage

# 终端 2: 启动合并后的 Broker (连接 + 分发)
make run-broker
```

### 3. 使用 Docker Compose 一键启动
```bash
make docker
```

## 📁 存储结构说明
- `storage-data/`：主目录
  - `state.db`：消费者、消费组、分区分配状态 (SQLite)
  - `{topic}/partition_{id}.log`：原始消息数据 (JSON Lines)
  - `_offsets/`：消费偏移量持久化文件

## 🔮 未来扩展
- **分布式事务**：如果 Storage Service 也需要多实例横向扩展，可以将 SQLite 替换为 **etcd** 或 **TiKV**。
- **动态负载均衡**：Broker 可以根据 `state.db` 中的活跃消费者数量，自动进行分区的 Rebalance。

---
**架构师笔记**: 本次重构标志着 GMQ 从“演示级”向“生产级”迈出了一大步。通过状态下沉和组件合并，我们在性能和可靠性之间取得了完美的平衡。
