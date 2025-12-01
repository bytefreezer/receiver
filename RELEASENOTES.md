# ByteFreezer Receiver - Release Notes

## 2025-10-29 - Authentication Fixes (Dataset Metrics & Health Reporting)

### Bug Fixes

#### 🔧 Dataset Metrics Recording Authentication
- **Issue**: Dataset metrics recording to control service was failing with 401 Unauthorized errors
  - `DatasetMetricsClient` was not sending Authorization header with API key
  - Error logged: "Dataset metrics recording failed with status 401 for {tenant}/{dataset}"
  - Control service requires `Authorization: Bearer <api_key>` for dataset metrics endpoint
- **Fix**: Added API key authentication to dataset metrics client
  - Added `apiKey` field to `DatasetMetricsClient` struct
  - Updated `NewDatasetMetricsClient()` to accept `apiKey` parameter
  - Modified HTTP request creation to include Authorization header
  - Now uses `control_service.api_key` from configuration
- **Impact**: Dataset metrics successfully recorded to control service
- **Files Changed**:
  - `metrics/dataset_metrics.go:17,40,94-96` (added apiKey field and Authorization header)
  - `config/config.go:224` (pass API key to client constructor)

#### 🔧 Health Reporting Authentication
- **Issue**: Health reporting to control service was missing Authorization header
  - `HealthReportingService` was using `http.Client.Post()` which doesn't allow setting headers
  - Service registration and health reports would fail with 401 when Control Service JWT middleware is enabled
- **Fix**: Added API key authentication to health reporting service
  - Added `apiKey` field to `HealthReportingService` struct
  - Updated `NewHealthReportingService()` to accept `apiKey` parameter
  - Replaced `http.Client.Post()` with `http.NewRequest()` and `http.Client.Do()` to allow headers
  - Added `Authorization: Bearer <key>` header to both registration and health report requests
  - Now uses `control_service.api_key` from configuration
- **Impact**: Health reporting and service registration successfully authenticated
- **Files Changed**:
  - `services/health_reporting.go:23,63,80,132-141,186-195` (added apiKey field and Authorization headers)
  - `main.go:123` (pass API key to service constructor)

### Configuration
The receiver uses the existing `control_service.api_key` configuration for both dataset metrics and health reporting:
```yaml
control_service:
  api_key: "bytefreezer-service-api-key-..."  # System-wide service API key
```

Both services now use consistent Bearer token authentication for all Control Service communication.

## 2025-10-28 - Security Hardening

### Security Fixes
- **CRITICAL: Removed "Open Mode" for Tenants Without Bearer Tokens**
  - Previously, tenants with no configured bearer token (`authentication.bearer_token = ""`) would allow unauthenticated data ingestion
  - This created a security vulnerability where misconfigured tenants accepted any data without authentication
  - Now, tenants **must** have a bearer token configured, or all requests are rejected with HTTP 401
  - High-severity SOC alert is triggered when a misconfigured tenant (no bearer token) attempts data ingestion
  - Error message: "Tenant authentication not configured"

### Updated Authentication Behavior

| Scenario | Receiver Behavior |
|----------|------------------|
| **Authentication.Enabled = false** | Skip all token validation (allow all requests) |
| **Tenant.BearerToken = ""** (empty) | ❌ Reject with 401 + Send SOC alert (HIGH severity) |
| **Tenant.BearerToken = "xyz"** + Valid token | ✅ Allow request |
| **Tenant.BearerToken = "xyz"** + Invalid/missing token | ❌ Return 401 + Send SOC alert (MEDIUM severity) |
| **Tenant not found/inactive** | ❌ Return 410 Gone |

### Security Implications
- **Breaking Change**: Tenants without configured bearer tokens will now be rejected (previously allowed)
- **Action Required**: All active tenants must have `authentication.bearer_token` configured in Control Service
- **Fail-Secure Pattern**: Missing authentication configuration now fails closed (secure) rather than open (insecure)

### Files Modified
- `webhook/middleware.go` (lines 68-84): Changed "open mode" behavior to reject requests and send SOC alerts

### Related Documentation
- See Control Service documentation for configuring tenant bearer tokens in `control_tenants.config->authentication.bearer_token`
- Proxy configuration must include matching bearer token in tenant config or global `bearer_token` setting
