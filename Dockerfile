# 多阶段构建 Dockerfile
FROM golang:1.25.3-alpine AS builder

# 安装必要工具 (不再需要 make)
RUN apk add --no-cache git

WORKDIR /app

# 复制依赖文件
COPY go.mod go.sum ./
RUN go mod download

# 复制源代码目录
COPY cmd/ ./cmd/
COPY internal/ ./internal/
COPY pkg/ ./pkg/
COPY proto/ ./proto/

# 直接使用 go build 编译二进制
RUN CGO_ENABLED=0 GOOS=linux go build -o bin/gmq-storage-service cmd/storage-service/main.go
RUN CGO_ENABLED=0 GOOS=linux go build -o bin/gmq-broker-service cmd/broker-service/main.go
RUN CGO_ENABLED=0 GOOS=linux go build -o bin/gmq-admin-service cmd/admin-service/main.go

# Storage Service 镜像
FROM alpine:latest AS storage
RUN apk --no-cache add ca-certificates
WORKDIR /app
COPY --from=builder /app/bin/gmq-storage-service .
EXPOSE 50052
CMD ["./gmq-storage-service", "-addr", ":50052"]

# Broker Service 镜像
FROM alpine:latest AS broker
RUN apk --no-cache add ca-certificates
WORKDIR /app
COPY --from=builder /app/bin/gmq-broker-service .
EXPOSE 50051
CMD ["./gmq-broker-service", "-addr", ":50051", "-storage", "gmq-storage:50052"]

# Admin Service 镜像
FROM alpine:latest AS admin
RUN apk --no-cache add ca-certificates
WORKDIR /app
COPY --from=builder /app/bin/gmq-admin-service .
EXPOSE 8080
CMD ["./gmq-admin-service", "-addr", ":8080"]
