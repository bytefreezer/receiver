# ByteFreezer Receiver

HTTP webhook receiver service for the ByteFreezer platform. Receiver accepts incoming data via webhooks, stores it reliably through a three-stage spooling pipeline, and uploads to S3 for downstream processing.

## Overview

ByteFreezer Receiver is the **ingestion layer** in the ByteFreezer four-service architecture:

1. **bytefreezer-proxy**: Protocol data collection → Receiver
2. **bytefreezer-receiver** (this service): HTTP webhook ingestion → S3 raw/
3. **bytefreezer-piper**: Data processing pipeline → S3 processed/
4. **bytefreezer-packer**: Parquet optimization → S3 parquet/

### Key Features

- **HTTP Webhook API** - RESTful webhook endpoint for data ingestion
- **Three-Stage Spooling** - Crash-resilient queue → retry → S3/DLQ pipeline
- **Multi-Tenant Support** - Tenant/dataset isolation with validation
- **Compression** - Automatic gzip compression of incoming data
- **Health Reporting** - Reports status to ByteFreezer Control
- **Dead Letter Queue** - Failed uploads retained for manual recovery
- **OpenTelemetry Integration** - Comprehensive metrics and tracing

## Installation

### Docker (Recommended)

```bash
# Pull the latest image
docker pull ghcr.io/bytefreezer/receiver:latest

# Run with configuration
docker run -p 8080:8080 -v $(pwd)/config.yaml:/config.yaml ghcr.io/bytefreezer/receiver:latest
```

### Build from Source

```bash
go build -o bytefreezer-receiver .
./bytefreezer-receiver --config config.yaml
```

## Configuration

```yaml
app:
  name: "bytefreezer-receiver"
  version: "1.0.0"

logging:
  level: "info"
  encoding: "json"

server:
  webhook_port: 8080
  api_port: 8081

s3:
  bucket_name: "raw-data"
  region: "us-east-1"

spooling:
  base_path: "/var/spool/bytefreezer-receiver"
  queue_interval_seconds: 5
  retry_interval_seconds: 60
  max_retries: 3

control_service:
  base_url: "http://control:8082"
  api_key: "your-api-key"
```

### Environment Variables

All configuration options support environment variable overrides with the `BYTEFREEZER_RECEIVER_` prefix:

```bash
export BYTEFREEZER_RECEIVER_LOGGING_LEVEL=debug
export BYTEFREEZER_RECEIVER_SERVER_WEBHOOK_PORT=8080
```

## Three-Stage Spooling Pipeline

The receiver implements a crash-resilient data pipeline:

```
                      ┌─────────────────┐
    Webhook POST ────▶│     Queue       │
                      │  (Immediate)    │
                      └────────┬────────┘
                               │ 5s interval
                      ┌────────▼────────┐
                      │     Retry       │
                      │  (S3 Upload)    │
                      └────────┬────────┘
                               │
              ┌────────────────┼────────────────┐
              │                │                │
       ┌──────▼──────┐  ┌──────▼──────┐  ┌──────▼──────┐
       │   Success   │  │   Retry     │  │    DLQ      │
       │  (Delete)   │  │  (Backoff)  │  │  (Manual)   │
       └─────────────┘  └─────────────┘  └─────────────┘
```

### Directory Structure

```
/var/spool/bytefreezer-receiver/{tenant}/{dataset}/
├── queue/    # Immediate webhook storage (.ndjson.gz files)
├── retry/    # Files pending S3 upload (.ndjson.gz + .meta files)
└── dlq/      # Dead letter queue for failed uploads
```

## API Endpoints

### Webhook API

- `POST /webhook/{tenantId}/{datasetId}` - Receive data for tenant/dataset

### Management API

- `GET /api/v1/health` - Service health check
- `GET /api/v1/config` - Current configuration
- `GET /api/v1/stats` - Processing statistics

### Example Usage

```bash
# Send data to receiver
curl -X POST http://localhost:8080/webhook/tenant-001/dataset-001 \
  -H "Content-Type: application/json" \
  -d '{"event": "test", "timestamp": "2024-01-15T14:30:00Z"}'

# Health check
curl http://localhost:8081/api/v1/health
```

## License

ByteFreezer is licensed under the [Elastic License 2.0](LICENSE.txt).

You're free to use, modify, and self-host. You cannot offer it as a managed service.
