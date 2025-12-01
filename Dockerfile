# Multi-stage build for ByteFreezer Receiver
# Stage 1: Build the Go binary
FROM golang:1.24.4 AS builder

# Set working directory
WORKDIR /src

# Copy go mod files first for better layer caching
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build the binary with optimization flags
# Disable CGO for a fully static binary
ARG VERSION=unknown
ARG BUILD_TIME=unknown
ARG GIT_COMMIT=unknown
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags="-s -w -X main.version=${VERSION} -X main.buildTime=${BUILD_TIME} -X main.gitCommit=${GIT_COMMIT}" \
    -o /bytefreezer-receiver .

# Debug: Check what we built
RUN ls -l /bytefreezer-receiver

# Verify the binary is statically linked
RUN ldd /bytefreezer-receiver 2>&1 | grep -q "not a dynamic executable" || (echo "Binary is not static!" && ldd /bytefreezer-receiver && exit 1)

# Stage 2: Create minimal runtime image
FROM alpine:3.19

# Install runtime dependencies
RUN apk add --no-cache \
    ca-certificates \
    tzdata \
    curl \
    netcat-openbsd \
    && rm -rf /var/cache/apk/*

# Create non-root user for security
RUN addgroup -g 1000 -S bytefreezer && \
    adduser -u 1000 -S bytefreezer -G bytefreezer -s /bin/sh -D

# Create required directories
RUN mkdir -p /etc/bytefreezer-receiver \
             /var/log/bytefreezer-receiver \
             /var/spool/bytefreezer-receiver && \
    chown -R bytefreezer:bytefreezer /etc/bytefreezer-receiver \
                                    /var/log/bytefreezer-receiver \
                                    /var/spool/bytefreezer-receiver

# Copy the binary from builder stage
COPY --from=builder /bytefreezer-receiver /bytefreezer-receiver

# Copy default configuration
COPY --chown=bytefreezer:bytefreezer config.yaml /etc/bytefreezer-receiver/config.yaml

# Copy CA certificates from builder
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

# Set up proper permissions
RUN chmod +x /bytefreezer-receiver

# Debug: Check the binary is there
RUN ls -l /bytefreezer-receiver

# Switch to non-root user
USER bytefreezer

# Set environment variables
ENV CONFIG_FILE=/etc/bytefreezer-receiver/config.yaml

# Expose ports
# 8080: API endpoint
# 8081: Webhook endpoint
# 9098: Prometheus metrics endpoint
EXPOSE 8080/tcp 8081/tcp 9098/tcp

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1

# Set working directory
WORKDIR /

# Default command
CMD ["/bytefreezer-receiver", "--config", "/etc/bytefreezer-receiver/config.yaml"]

# Metadata labels following OCI standards
ARG VERSION=unknown
ARG BUILD_TIME=unknown
LABEL maintainer="ByteFreezer Team" \
      org.opencontainers.image.title="ByteFreezer Receiver" \
      org.opencontainers.image.description="High-performance data processing and storage service for ByteFreezer platform" \
      org.opencontainers.image.vendor="ByteFreezer" \
      org.opencontainers.image.source="https://github.com/n0needt0/bytefreezer-receiver" \
      org.opencontainers.image.documentation="https://github.com/n0needt0/bytefreezer-receiver/blob/main/README.md" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.created="${BUILD_TIME}"