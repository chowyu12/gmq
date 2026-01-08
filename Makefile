.PHONY: help proto build run clean up down logs ps example-prod example-group admin

# Default target
help:
	@echo "GMQ (Go Message Queue) Management Commands"
	@echo "  make proto          - Generate gRPC protocol code"
	@echo "  make build          - Compile all services and examples"
	@echo "  make up             - Start the cluster using Docker Compose"
	@echo "  make down           - Stop the cluster"
	@echo "  make clean          - Clean up build artifacts"
	@echo "  make example-prod   - Run producer example"
	@echo "  make example-group  - Run consumer group example"

# Generate protocol code
proto:
	@echo "Generating gRPC protocol code..."
	@chmod +x gen_proto.sh
	@./gen_proto.sh
	@echo "✓ Code generation completed"

# Compile distributed services
build: proto
	@echo "Building distributed services..."
	@mkdir -p bin
	@go build -o bin/gmq-storage-service cmd/storage-service/main.go
	@go build -o bin/gmq-broker-service cmd/broker-service/main.go
	@go build -o bin/gmq-admin-service cmd/admin-service/main.go
	@echo "Building examples..."
	@go build -o bin/producer examples/producer/main.go
	@go build -o bin/consumer_group examples/consumer_group/main.go
	@go build -o bin/benchmark examples/benchmark/main.go
	@echo "✓ Build completed, binaries are in bin/ directory"

# Docker orchestration
up:
	@echo "Starting GMQ cluster..."
	@docker-compose up -d --build
	@echo "✓ Cluster started, available ports: Broker(50051), Storage(50052), Dragonfly(6379)"

down:
	@echo "Stopping cluster..."
	@docker-compose down

logs:
	@docker-compose logs -f

ps:
	@docker-compose ps

# Cleanup
clean:
	@echo "Cleaning up..."
	@rm -rf bin/
	@echo "✓ Cleanup completed"

# Run examples
example-prod:
	@go run examples/producer/main.go

example-group:
	@go run examples/consumer_group/main.go

# Performance benchmark
bench-prod: build
	make build
	@bin/benchmark -mode prod -c 50 -n 200000 -b 500 -s 512

bench-cons: build
	make build
	@bin/benchmark -mode cons -c 10
