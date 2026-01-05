# Instructions for Claude - ByteFreezer Project

## Project Overview
This is part of the ByteFreezer ecosystem - a comprehensive data ingestion, processing, and analytics platform. Each component serves a specific purpose in the data pipeline.

## Default Workflow
When working on ANY ByteFreezer project, always follow these steps:

### 1. Before Making Changes
- [ ] Read this file first for any specific instructions
- [ ] Check RELEASENOTES.md to understand recent changes (create if missing)
- [ ] Review the current todo list if applicable
- [ ] Understand which ByteFreezer component this is and its role

### 2. When Adding Features
- [ ] Implement the feature with proper error handling
- [ ] Add appropriate logging (use existing log patterns)
- [ ] Build and test the changes (language-appropriate: `go build`, `npm run build`, etc.)
- [ ] Update RELEASENOTES.md with feature description
- [ ] Create example usage documentation if it's an API feature
- [ ] Consider impact on other ByteFreezer components

### 3. When Fixing Bugs
- [ ] Identify root cause and document it
- [ ] Implement fix with error handling
- [ ] Test the fix thoroughly
- [ ] Update RELEASENOTES.md with bug fix details
- [ ] Check if the bug affects other ByteFreezer components

### 4. Code Standards (Language-Specific)

#### For Go Projects (proxy, receiver, soc, control, packer, piper)
- [ ] Follow existing Go conventions in the codebase
- [ ] Use existing imports and patterns (don't add new dependencies without asking)
- [ ] Add proper error logging for user-facing issues
- [ ] Keep functions focused and well-documented
- [ ] Use structured logging with appropriate levels (debug, info, warn, error)

#### For JavaScript/TypeScript Projects (ui, website)
- [ ] Follow existing TypeScript/JavaScript patterns
- [ ] Use existing component patterns and styling approaches
- [ ] Add proper error handling and user feedback
- [ ] Follow existing import/export patterns

#### For Infrastructure Projects (postgres, localstack)
- [ ] Follow existing configuration patterns
- [ ] Document any infrastructure changes
- [ ] Test configuration changes thoroughly
- [ ] Consider impact on dependent services

### 5. API Changes (For Services with APIs)
- [ ] Follow existing API patterns (usually `/api/v1/` for ByteFreezer)
- [ ] Add new endpoints to appropriate router files
- [ ] Create request/response structs with proper JSON tags
- [ ] Test with curl examples or appropriate testing tools
- [ ] Document API changes in RELEASENOTES.md

### 6. Always Update
- [ ] RELEASENOTES.md with clear feature/fix descriptions
- [ ] Any relevant example files or documentation
- [ ] Build the binary/application to ensure it compiles
- [ ] Consider if README.md needs updates

## ByteFreezer Component Roles

### Data Ingestion Layer
- **bytefreezer-proxy**: UDP data collection and forwarding to receiver
- **bytefreezer-receiver**: HTTP webhook receiver with three-stage spooling pipeline (queue → retry → S3/DLQ) for reliable data processing

### Processing Layer
- **bytefreezer-packer**: Data compression and packaging
- **bytefreezer-piper**: Data pipeline orchestration and processing

### Control & Monitoring
- **bytefreezer-control**: Central control plane and configuration management
- **bytefreezer-soc**: Security operations center and alerting

### Infrastructure
- **bytefreezer-postgres**: Database schemas and migrations
- **bytefreezer-localstack**: Local development environment setup

### User Interface
- **bytefreezer-ui**: Main web application interface
- **bytefreezer-website**: Public website and documentation

## Current Priority Areas (All Projects)
1. **Reliability**: Error handling, graceful degradation, retry logic
2. **Observability**: Logging, metrics, health checks, debugging tools
3. **Security**: Authentication, authorization, input validation
4. **Performance**: Optimization, caching, resource management
5. **Documentation**: Clear APIs, examples, troubleshooting guides

## ByteFreezer Standards

### Configuration
- Use YAML for configuration files
- Support environment variable overrides
- Validate configuration on startup
- Document all configuration options

### Logging
- Use structured logging (JSON format preferred)
- Log levels: debug, info, warn, error
- Include relevant context (tenant_id, request_id, etc.)
- Don't log sensitive information (tokens, passwords)

### Error Handling
- Always return meaningful error messages
- Include context about what operation failed
- Use appropriate HTTP status codes for APIs
- Log errors with sufficient detail for debugging

### APIs
- Follow REST conventions where applicable
- Use consistent JSON response formats
- Include API versioning (typically `/api/v1/`)
- Provide comprehensive error responses
- Document with examples

### Testing
- Build/compile before finishing work
- Test happy path and error conditions
- Verify integration points between components
- Use appropriate testing tools for the language

## Don't Do This
- Don't add complex automation without asking
- Don't change core architecture without discussion
- Don't add new external dependencies without approval
- Don't modify CI/CD or infrastructure without asking
- Don't break backward compatibility without discussion
- Don't commit secrets or sensitive data

## Component Integration Notes
- **Proxy → Receiver**: HTTP POST with compressed data to `/webhook/{tenantId}/{datasetId}`
- **Receiver Data Flow**: Webhook → Queue → Retry → S3/DLQ (three-stage spooling pipeline)
- **Receiver → S3**: Direct uploads of compressed `.ndjson.gz` files with metadata and retry logic
- **Control → All**: Configuration and health monitoring
- **SOC → All**: Security alerts and monitoring
- **UI ↔ Control**: API calls for management interface

## Receiver Architecture
The receiver implements a three-stage spooling pipeline for reliable data processing:

### Directory Structure
```
/var/spool/bytefreezer-receiver/{tenant}/{dataset}/
├── queue/    # Immediate webhook storage (.ndjson.gz files)
├── retry/    # Files pending S3 upload (.ndjson.gz + .meta files)
└── dlq/      # Dead letter queue for failed uploads (.ndjson.gz + .meta)
```

### Three-Stage Data Flow
1. **Queue Stage**: Webhook handler immediately saves compressed data to `/queue`
   - Fast response to clients (minimal processing)
   - Single responsibility: receive and store
   - Queue processor moves files to retry every 5 seconds

2. **Retry Stage**: Retry processor attempts S3 uploads with backoff
   - Configurable retry attempts (default: 3)
   - Metadata tracking with retry counts and timestamps
   - S3 upload with comprehensive metadata
   - Retry processor runs every 60 seconds (configurable)

3. **Final Stage**: Success (S3) or DLQ
   - Successful uploads: Files removed from retry
   - Failed uploads: Moved to DLQ after max retries
   - DLQ files available for manual reprocessing

### Key Features
- **Crash Resilience**: File-based state survives service restarts
- **Minimal Data Loss**: Data persisted immediately to disk
- **Single Responsibility**: Each stage has one clear purpose
- **Proxy Pattern Match**: Same architecture as bytefreezer-proxy
- **Orphaned File Recovery**: Processes leftover queue files on startup

**Note**: With proxy now generating large files (10MB+), merging is no longer needed. The packer handles final data consolidation.

## When In Doubt
- Ask before making architectural changes
- Follow existing patterns in the codebase
- Keep it simple and maintainable
- Consider the impact on the entire ByteFreezer ecosystem
- Prioritize reliability and observability

## Current Focus (Update as needed)
- Enhanced error handling and debugging capabilities
- API standardization across all components
- Improved observability and monitoring
- Documentation and examples

## Known Issues
- **Staticcheck pre-commit hook**: Temporarily disabled due to Go version compatibility issue. The staticcheck binary was built with Go 1.24.2 but the module requires Go 1.24.4. This is a known issue with staticcheck distribution. The code passes staticcheck when run manually with a compatible version.