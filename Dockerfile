# 多阶段构建 Dockerfile
FROM golang:1.25-alpine AS builder

# 安装必要工具
RUN apk add --no-cache git

WORKDIR /app

# 复制依赖文件
COPY go.mod go.sum ./
RUN go mod download

# 复制源代码
COPY . .

# 构建所有服务
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o gmq-storage-service cmd/storage-service/main.go
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o gmq-broker-service cmd/broker-service/main.go

# Storage Service 镜像
FROM alpine:latest AS storage
RUN apk --no-cache add ca-certificates netcat-openbsd
WORKDIR /app
COPY --from=builder /app/gmq-storage-service .
EXPOSE 50052
CMD ["/app/gmq-storage-service", "-addr", ":50052", "-data", "/data"]

# Broker Service 镜像 (Gateway + Dispatcher)
FROM alpine:latest AS broker
RUN apk --no-cache add ca-certificates netcat-openbsd
WORKDIR /app
COPY --from=builder /app/gmq-broker-service .
EXPOSE 50051
CMD ["/app/gmq-broker-service", "-addr", ":50051", "-storage", "storage:50052", "-partitions", "4"]
