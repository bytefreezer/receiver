// Licensed under Elastic License 2.0
// See LICENSE.txt for details

package api

import (
	"context"
	"fmt"

	"github.com/bytefreezer/goodies/log"
	"github.com/bytefreezer/receiver/config"
	"github.com/bytefreezer/receiver/domain"
	"github.com/bytefreezer/receiver/services"
	"github.com/swaggest/usecase"
	"github.com/swaggest/usecase/status"
)

// HealthResponse represents the health check response
type HealthResponse struct {
	Status          string `json:"status"`
	Version         string `json:"version"`
	TenantCount     int    `json:"tenant_count"`
	DatabaseHealthy bool   `json:"database_healthy"`
}

// TenantsResponse represents the tenants list response
type TenantsResponse struct {
	Tenants []domain.Tenant `json:"tenants"`
	Count   int             `json:"count"`
}

// TenantResponse represents a single tenant response
type TenantResponse struct {
	Tenant *domain.Tenant `json:"tenant"`
}

// GetTenantRequest represents the get tenant request
type GetTenantRequest struct {
	TenantID string `path:"tenantId" required:"true"`
}

// TenantCountResponse represents the tenant count response
type TenantCountResponse struct {
	Count int `json:"count"`
}

// ConfigResponse represents the current system configuration
type ConfigResponse struct {
	App             AppConfig               `json:"app"`
	Server          ServerConfig            `json:"server"`
	Webhook         WebhookConfig           `json:"webhook"`
	Bytefreezer     BytefreezerConfig       `json:"bytefreezer"`
	ControlService  ControlServiceConfig    `json:"control_service"`
	SOC             SOCConfig               `json:"soc"`
	S3Destination   S3ConfigMasked          `json:"s3_destination"`
	Housekeeping    HousekeepingConfig      `json:"housekeeping"`
	Dev             bool                    `json:"dev"`
	ComponentStatus ComponentStatusResponse `json:"component_status"`
}

// Individual config sections for response
type AppConfig struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

type ServerConfig struct {
	ApiPort int `json:"api_port"`
}

type WebhookConfig struct {
	Port           int `json:"port"`
	MaxPayloadSize int `json:"max_payload_size"`
}

type BytefreezerConfig struct {
	UploadWorkerCount int `json:"upload_worker_count"`
}

type ControlServiceConfig struct {
	Enabled        bool   `json:"enabled"`
	BaseURL        string `json:"base_url"`
	APIKey         string `json:"api_key"` // Will be masked
	TimeoutSeconds int    `json:"timeout_seconds"`
}

type SOCConfig struct {
	Enabled  bool   `json:"enabled"`
	Endpoint string `json:"endpoint"`
	Timeout  int    `json:"timeout"`
}

type HousekeepingConfig struct {
	Enabled         bool `json:"enabled"`
	IntervalSeconds int  `json:"interval_seconds"`
}

// Masked config sections (sensitive data redacted)

type S3ConfigMasked struct {
	BucketName string `json:"bucket_name"`
	Region     string `json:"region"`
	AccessKey  string `json:"access_key"` // This will be masked
	SecretKey  string `json:"secret_key"` // This will be masked
	SecretName string `json:"secret_name"`
	Endpoint   string `json:"endpoint"`
	Ssl        bool   `json:"ssl"`
}

// PostgreSQL lock configuration removed - using spool-based file approach

type ComponentStatusResponse struct {
	TenantDatabase bool `json:"tenant_database"`
	SOCAlertClient bool `json:"soc_alert_client"`
	// PostgreSQL lock removed - using spool-based file approach
	S3Destination    bool `json:"s3_destination"`
	UploadWorkerPool bool `json:"upload_worker_pool"`
}

// HealthCheck returns a health check handler
func (api *API) HealthCheck() usecase.Interactor {
	u := usecase.NewInteractor(func(ctx context.Context, input struct{}, output *HealthResponse) error {
		// Check service health
		status := "ok"
		tenantCount := 0
		databaseHealthy := false

		if api.Services.Config.TenantDatabase != nil {
			tenantCount = api.Services.Config.TenantDatabase.GetTenantCount()
			databaseHealthy = api.Services.Config.TenantDatabase.IsHealthy()
		}

		if !databaseHealthy {
			status = "degraded"
		}

		output.Status = status
		output.Version = api.Config.App.Version
		output.TenantCount = tenantCount
		output.DatabaseHealthy = databaseHealthy

		log.Debugf("Health check: status=%s, tenants=%d, db_healthy=%t", status, tenantCount, databaseHealthy)

		return nil
	})

	u.SetTitle("Health Check")
	u.SetDescription("Check the health status of the ByteFreezer Receiver service")
	u.SetTags("Health")

	return u
}

// GetTenants returns a handler for getting all tenants
func (api *API) GetTenants() usecase.Interactor {
	u := usecase.NewInteractor(func(ctx context.Context, input struct{}, output *TenantsResponse) error {
		// Get all tenants from database
		tenants := []domain.Tenant{}
		if api.Services.Config.TenantDatabase != nil {
			tenants = api.Services.Config.TenantDatabase.GetAllTenants()
		}

		output.Tenants = tenants
		output.Count = len(tenants)

		log.Debugf("Retrieved %d tenants", len(tenants))

		return nil
	})

	u.SetTitle("Get All Tenants")
	u.SetDescription("Retrieve all tenants and their datasets")
	u.SetTags("Tenants")

	return u
}

// GetTenant returns a handler for getting a specific tenant
func (api *API) GetTenant() usecase.Interactor {
	u := usecase.NewInteractor(func(ctx context.Context, input GetTenantRequest, output *TenantResponse) error {
		// Get specific tenant from database
		var tenant *domain.Tenant
		if api.Services.Config.TenantDatabase != nil {
			tenant = api.Services.Config.TenantDatabase.GetTenantByID(input.TenantID)
		}

		if tenant == nil {
			return status.Wrap(status.InvalidArgument, status.NotFound)
		}

		output.Tenant = tenant

		log.Debugf("Retrieved tenant: %s", input.TenantID)

		return nil
	})

	u.SetTitle("Get Tenant")
	u.SetDescription("Retrieve a specific tenant by ID")
	u.SetTags("Tenants")

	return u
}

// GetTenantCount returns a handler for getting tenant count
func (api *API) GetTenantCount() usecase.Interactor {
	u := usecase.NewInteractor(func(ctx context.Context, input struct{}, output *TenantCountResponse) error {
		// Get tenant count from database
		count := 0
		if api.Services.Config.TenantDatabase != nil {
			count = api.Services.Config.TenantDatabase.GetTenantCount()
		}

		output.Count = count

		log.Debugf("Retrieved tenant count: %d", count)

		return nil
	})

	u.SetTitle("Get Tenant Count")
	u.SetDescription("Retrieve the number of tenants in the local database")
	u.SetTags("Tenants")

	return u
}

// maskSensitiveValue masks sensitive configuration values
func maskSensitiveValue(value string) string {
	if value == "" {
		return ""
	}
	if len(value) <= 8 {
		return "***"
	}
	return value[:4] + "***" + value[len(value)-4:]
}

// GetConfig returns a handler for getting current system configuration
func (api *API) GetConfig() usecase.Interactor {
	u := usecase.NewInteractor(func(ctx context.Context, input struct{}, output *ConfigResponse) error {
		cfg := api.Config

		// Basic app configuration
		output.App = AppConfig{
			Name:    cfg.App.Name,
			Version: cfg.App.Version,
		}

		// Server configuration
		output.Server = ServerConfig{
			ApiPort: cfg.Server.ApiPort,
		}

		// Webhook configuration
		output.Webhook = WebhookConfig{
			Port:           cfg.Protocols.Webhook.Port,
			MaxPayloadSize: cfg.Protocols.Webhook.MaxPayloadSize,
		}

		// ByteFreezer configuration
		output.Bytefreezer = BytefreezerConfig{
			UploadWorkerCount: cfg.Bytefreezer.UploadWorkerCount,
		}

		// Control Service configuration (with masked API key)
		output.ControlService = ControlServiceConfig{
			Enabled:        cfg.ControlService.Enabled,
			BaseURL:        cfg.ControlService.BaseURL,
			APIKey:         maskSensitiveValue(cfg.ControlService.APIKey),
			TimeoutSeconds: cfg.ControlService.TimeoutSeconds,
		}

		// SOC configuration
		output.SOC = SOCConfig{
			Enabled:  cfg.SOC.Enabled,
			Endpoint: cfg.SOC.Endpoint,
			Timeout:  cfg.SOC.Timeout,
		}

		// S3 destination configuration (with masked secrets)
		output.S3Destination = S3ConfigMasked{
			BucketName: cfg.S3Destination.BucketName,
			Region:     cfg.S3Destination.Region,
			AccessKey:  maskSensitiveValue(cfg.S3Destination.AccessKey),
			SecretKey:  maskSensitiveValue(cfg.S3Destination.SecretKey),
			SecretName: cfg.S3Destination.SecretName,
			Endpoint:   cfg.S3Destination.Endpoint,
			Ssl:        cfg.S3Destination.Ssl,
		}

		// PostgreSQL lock configuration removed - using spool-based file approach

		// Housekeeping configuration
		output.Housekeeping = HousekeepingConfig{
			Enabled:         cfg.Housekeeping.Enabled,
			IntervalSeconds: cfg.Housekeeping.IntervalSeconds,
		}

		// Dev flag
		output.Dev = cfg.Dev

		// Component status
		output.ComponentStatus = ComponentStatusResponse{
			TenantDatabase: cfg.TenantDatabase != nil && cfg.TenantDatabase.IsHealthy(),
			SOCAlertClient: cfg.SOCAlertClient != nil,
			// PostgreSQL lock removed - using spool-based file approach
			S3Destination:    cfg.S3DestinationClient != nil,
			UploadWorkerPool: cfg.UploadWorkerPool != nil,
		}

		log.Debugf("Retrieved system configuration")

		return nil
	})

	u.SetTitle("Get System Configuration")
	u.SetDescription("Retrieve the current system configuration (sensitive values are masked)")
	u.SetTags("Configuration")

	return u
}

// GetDLQStats returns DLQ statistics
func GetDLQStats() usecase.Interactor {
	u := usecase.NewInteractor(func(ctx context.Context, input struct{}, output *struct {
		Data *services.DLQStats `json:"data"`
	}) error {
		cfg, ok := ctx.Value("config").(*config.Config)
		if !ok {
			return fmt.Errorf("config not found in context")
		}

		dlqService, ok := cfg.DLQService.(*services.DLQService)
		if !ok || dlqService == nil {
			return fmt.Errorf("DLQ service not available")
		}

		stats, err := dlqService.GetStats()
		if err != nil {
			return fmt.Errorf("failed to get DLQ stats: %w", err)
		}

		output.Data = stats
		return nil
	})

	u.SetTitle("Get DLQ Statistics")
	u.SetDescription("Retrieve Dead Letter Queue statistics including file counts and sizes")
	u.SetTags("DLQ")

	return u
}

// ListDLQFiles returns a list of DLQ files
func ListDLQFiles() usecase.Interactor {
	u := usecase.NewInteractor(func(ctx context.Context, input struct {
		TenantID  string `query:"tenant_id"`
		DatasetID string `query:"dataset_id"`
	}, output *struct {
		Data []*services.DLQFile `json:"data"`
	}) error {
		cfg, ok := ctx.Value("config").(*config.Config)
		if !ok {
			return fmt.Errorf("config not found in context")
		}

		dlqService, ok := cfg.DLQService.(*services.DLQService)
		if !ok || dlqService == nil {
			return fmt.Errorf("DLQ service not available")
		}

		files, err := dlqService.ListFiles(input.TenantID, input.DatasetID)
		if err != nil {
			return fmt.Errorf("failed to list DLQ files: %w", err)
		}

		output.Data = files
		return nil
	})

	u.SetTitle("List DLQ Files")
	u.SetDescription("Retrieve a list of files in the Dead Letter Queue")
	u.SetTags("DLQ")

	return u
}

// RetryDLQFiles retries files from DLQ
func RetryDLQFiles() usecase.Interactor {
	u := usecase.NewInteractor(func(ctx context.Context, input struct {
		TenantID  string `json:"tenant_id,omitempty"`
		DatasetID string `json:"dataset_id,omitempty"`
	}, output *struct {
		Message string `json:"message"`
		Count   int    `json:"count"`
	}) error {
		cfg, ok := ctx.Value("config").(*config.Config)
		if !ok {
			return fmt.Errorf("config not found in context")
		}

		dlqService, ok := cfg.DLQService.(*services.DLQService)
		if !ok || dlqService == nil {
			return fmt.Errorf("DLQ service not available")
		}

		count, err := dlqService.RetryFiles(input.TenantID, input.DatasetID)
		if err != nil {
			return fmt.Errorf("failed to retry DLQ files: %w", err)
		}

		output.Message = fmt.Sprintf("Moved %d files from DLQ back to queue for retry", count)
		output.Count = count
		return nil
	})

	u.SetTitle("Retry DLQ Files")
	u.SetDescription("Move files from Dead Letter Queue back to processing queue for retry")
	u.SetTags("DLQ")

	return u
}
