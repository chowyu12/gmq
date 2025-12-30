.PHONY: help proto build run clean up down logs ps example-prod example-group

# 默认目标
help:
	@echo "GMQ (Go Message Queue) 管理命令"
	@echo "  make proto          - 生成 gRPC 协议代码"
	@echo "  make build          - 编译所有服务及示例"
	@echo "  make up             - 使用 Docker Compose 启动集群"
	@echo "  make down           - 停止集群"
	@echo "  make clean          - 清理编译产物"
	@echo "  make example-prod   - 运行生产者示例"
	@echo "  make example-group  - 运行消费组示例"

# 生成协议代码
proto:
	@echo "生成 gRPC 协议代码..."
	@chmod +x gen_proto.sh
	@./gen_proto.sh
	@echo "✓ 代码生成完成"

# 编译分布式服务
build: proto
	@echo "构建分布式服务..."
	@mkdir -p bin
	@go build -o bin/gmq-storage-service cmd/storage-service/main.go
	@go build -o bin/gmq-broker-service cmd/broker-service/main.go
	@echo "构建示例..."
	@go build -o bin/producer examples/producer/main.go
	@go build -o bin/consumer_group examples/consumer_group/main.go
	@go build -o bin/benchmark examples/benchmark/main.go
	@echo "✓ 构建完成，二进制文件位于 bin/ 目录"

# Docker 编排
up:
	@echo "正在启动 GMQ 集群..."
	@docker-compose up -d --build
	@echo "✓ 集群已启动，可用端口: Broker(50051), Storage(50052), Dragonfly(6379)"

down:
	@echo "停止集群..."
	@docker-compose down

logs:
	@docker-compose logs -f

ps:
	@docker-compose ps

# 清理
clean:
	@echo "清理中..."
	@rm -rf bin/
	@echo "✓ 清理完成"

# 运行示例
example-prod:
	@go run examples/producer/main.go

example-group:
	@go run examples/consumer_group/main.go

# 性能压测
bench-prod: build
	@bin/benchmark -mode prod -c 50 -n 200000 -b 100 -s 512

bench-cons: build
	@bin/benchmark -mode cons -c 10
