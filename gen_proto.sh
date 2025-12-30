#!/bin/bash

echo "正在生成 gRPC 协议代码..."

# 1. 客户端与 Broker 通用协议
protoc --go_out=. --go_opt=paths=source_relative \
    --go-grpc_out=. --go-grpc_opt=paths=source_relative \
    proto/gmq.proto

# 2. Storage Service 内部协议 (含状态管理接口)
protoc --go_out=. --go_opt=paths=source_relative \
    --go-grpc_out=. --go-grpc_opt=paths=source_relative \
    proto/storage/storage.proto

echo "✓ 代码生成完成"
