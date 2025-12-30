.PHONY: help proto build run clean up down logs ps scale example-prod example-group

# 默认显示帮助
help:
	@echo "GMQ (Go Message Queue) 管理命令"
	@echo ""
	@echo "开发命令:"
	@echo "  make proto          - 生成 gRPC 协议代码"
	@echo "  make build          - 构建所有二进制文件"
	@echo "  make clean          - 清理构建产物和数据"
	@echo ""
	@echo "本地运行 (需先手动启动 Redis/Dragonfly):"
	@echo "  make run-storage    - 启动存储服务 (默认文件引擎)"
	@echo "  make run-broker     - 启动 Broker 服务"
	@echo ""
	@echo "Docker 部署 (推荐):"
	@echo "  make up             - 启动分布式集群 (Storage + Broker)"
	@echo "  make down           - 停止并移除容器"
	@echo "  make logs           - 查看容器日志"
	@echo "  make ps             - 查看容器状态"
	@echo ""
	@echo "示例运行:"
	@echo "  make example-prod   - 运行生产者示例"
	@echo "  make example-group  - 运行消费组示例"

# --- 协议生成 ---
proto:
	@echo "生成 gRPC 协议代码..."
	@chmod +x gen_proto.sh
	@./gen_proto.sh
	@echo "✓ 代码生成完成"

# --- 构建 ---
build: proto
	@echo "构建分布式服务..."
	@go build -o gmq-storage-service cmd/storage-service/main.go
	@go build -o gmq-broker-service cmd/broker-service/main.go
	@echo "构建示例..."
	@go build -o examples/producer/producer examples/producer/main.go
	@go build -o examples/consumer_group/consumer_group examples/consumer_group/main.go
	@echo "✓ 构建完成"

# --- 本地运行 ---
run-storage: build
	@echo "启动 Storage Service..."
	@./gmq-storage-service -addr :50052 -data ./storage-data

run-broker: build
	@echo "启动 Broker Service..."
	@./gmq-broker-service -addr :50051 -storage localhost:50052 -partitions 4

# --- Docker 编排 ---
up:
	@echo "正在启动 GMQ 集群..."
	@docker-compose up -d --build
	@echo "✓ 集群已启动，可用端口: Broker(50051), Storage(50052)"

down:
	@docker-compose down

logs:
	@docker-compose logs -f

ps:
	@docker-compose ps

# --- 清理 ---
clean:
	@echo "清理中..."
	@rm -f gmq-storage-service gmq-broker-service
	@rm -f examples/producer/producer examples/consumer_group/consumer_group
	@rm -rf storage-data
	@echo "✓ 清理完成"

# --- 示例 ---
example-prod:
	@echo "运行生产者示例..."
	@go run examples/producer/main.go

example-group:
	@echo "运行消费组示例..."
	@go run examples/consumer_group/main.go
