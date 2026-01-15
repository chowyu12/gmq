# Multi-stage build Dockerfile
FROM golang:1.25.3-alpine AS builder

# Install necessary tools (no longer need make)
RUN apk add --no-cache git

WORKDIR /app

# Copy dependency files
COPY go.mod go.sum ./   
RUN go mod download

# Copy source code directories
COPY cmd/ ./cmd/
COPY internal/ ./internal/
COPY pkg/ ./pkg/
COPY proto/ ./proto/

# Compile binaries directly using go build
RUN CGO_ENABLED=0 GOOS=linux go build -o bin/gmq-storage-service cmd/storage-service/main.go
RUN CGO_ENABLED=0 GOOS=linux go build -o bin/gmq-broker-service cmd/broker-service/main.go
RUN CGO_ENABLED=0 GOOS=linux go build -o bin/gmq-admin-service cmd/admin-service/main.go

# Storage Service image
FROM alpine:latest AS storage
RUN apk --no-cache add ca-certificates
WORKDIR /app
COPY --from=builder /app/bin/gmq-storage-service .
EXPOSE 50052
CMD ["./gmq-storage-service", "-addr", ":50052"]

# Broker Service image
FROM alpine:latest AS broker
RUN apk --no-cache add ca-certificates
WORKDIR /app
COPY --from=builder /app/bin/gmq-broker-service .
EXPOSE 50051
CMD ["./gmq-broker-service", "-addr", ":50051", "-storage", "gmq-storage:50052"]

# Admin Service image
FROM alpine:latest AS admin
RUN apk --no-cache add ca-certificates
WORKDIR /app
COPY --from=builder /app/bin/gmq-admin-service .
EXPOSE 8080
CMD ["./gmq-admin-service", "-addr", ":8080"]
