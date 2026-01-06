#!/bin/bash

echo "Generating gRPC protocol code..."

# 1. General protocol for Client and Broker
protoc --go_out=. --go_opt=paths=source_relative \
    --go-grpc_out=. --go-grpc_opt=paths=source_relative \
    proto/gmq.proto

# 2. Internal protocol for Storage Service (including status management)
protoc --go_out=. --go_opt=paths=source_relative \
    --go-grpc_out=. --go-grpc_opt=paths=source_relative \
    proto/storage/storage.proto

echo "✓ Code generation completed"
