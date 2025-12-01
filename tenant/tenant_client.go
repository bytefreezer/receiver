package tenant

import (
	"context"
	"fmt"
	"net/http"
	"time"

	client "github.com/bytefreezer/goodies/control-client"
	"github.com/bytefreezer/goodies/log"
	"github.com/bytefreezer/receiver/domain"
)

// AppConfig represents application configuration
type AppConfig struct {
	Name    string `mapstructure:"name"`
	Version string `mapstructure:"version"`
}

// BytefreezerConfig represents bytefreezer configuration (deprecated - keeping for backwards compatibility)
type BytefreezerConfig struct {
	// All fields moved to control_service configuration
}

// ControlServiceConfig represents control service configuration
type ControlServiceConfig struct {
	Enabled        bool   `mapstructure:"enabled"`
	BaseURL        string `mapstructure:"base_url"`
	APIKey         string `mapstructure:"api_key"`
	TimeoutSeconds int    `mapstructure:"timeout_seconds"`
}

// TenantClientConfig represents config needed by TenantClient
type TenantClientConfig struct {
	App            AppConfig            `mapstructure:"app"`
	Bytefreezer    BytefreezerConfig    `mapstructure:"bytefreezer"`
	ControlService ControlServiceConfig `mapstructure:"control_service"`
	Dev            bool                 `mapstructure:"dev"` // Deprecated - use control_service
}

type TenantClient struct {
	config        TenantClientConfig
	httpClient    *http.Client
	controlClient *client.Client
}

func NewTenantClient(conf TenantClientConfig) *TenantClient {
	tc := &TenantClient{
		config: conf,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}

	// Initialize control client if enabled
	if conf.ControlService.Enabled && conf.ControlService.BaseURL != "" {
		tc.controlClient = client.NewClient(client.Config{
			BaseURL:        conf.ControlService.BaseURL,
			APIKey:         conf.ControlService.APIKey,
			TimeoutSeconds: conf.ControlService.TimeoutSeconds,
		})
		log.Info("Control Service client initialized for tenant management")
	}

	return tc
}

func (tc *TenantClient) FetchTenants() ([]domain.Tenant, error) {
	ctx := context.Background()

	// Priority 1: Try Control Service API (if enabled)
	if tc.controlClient != nil {
		tenants, err := tc.fetchTenantsFromControlService(ctx)
		if err == nil {
			log.Infof("Successfully fetched %d tenants from Control Service", len(tenants))
			return tenants, nil
		}
		log.Warnf("Failed to fetch tenants from Control Service: %v, falling back", err)
	}

	// Priority 2: Fallback to dev mode / fake data
	if tc.config.Dev {
		log.Warn("Using fake tenant data (dev mode fallback)")
		return tc.getFakeTenants(), nil
	}

	return nil, fmt.Errorf("no tenant data source available: control service and dev mode all unavailable")
}

// fetchTenantsFromControlService fetches ALL accounts and tenants from the Control Service
func (tc *TenantClient) fetchTenantsFromControlService(ctx context.Context) ([]domain.Tenant, error) {
	// First, fetch all accounts
	accounts, err := tc.controlClient.ListAccounts(ctx, 1000) // limit 1000 accounts
	if err != nil {
		return nil, fmt.Errorf("failed to fetch accounts: %w", err)
	}

	log.Infof("Fetched %d accounts from Control Service", len(accounts))

	// Then fetch all tenants for each account
	allTenants := make([]domain.Tenant, 0)
	for _, account := range accounts {
		// Fetch tenants for this account
		controlTenants, err := tc.controlClient.ListTenants(ctx, account.ID, 1000) // limit 1000 tenants per account
		if err != nil {
			log.Warnf("Failed to fetch tenants for account %s: %v", account.ID, err)
			continue
		}

		log.Debugf("Fetched %d tenants for account %s", len(controlTenants), account.ID)

		// Convert control-client tenants to domain.Tenant
		for _, ct := range controlTenants {
			// Only include active tenants
			if !ct.Active {
				log.Debugf("Skipping inactive tenant: %s (%s)", ct.Name, ct.ID)
				continue
			}

			// Extract bearer token from tenant config
			bearerToken := ""
			if authConfig, ok := ct.Config["authentication"].(map[string]interface{}); ok {
				if token, ok := authConfig["bearer_token"].(string); ok {
					bearerToken = token
				}
			}

			// Fetch datasets for this tenant
			datasets := make([]*domain.Dataset, 0)
			controlDatasets, err := tc.controlClient.ListDatasets(ctx, ct.ID, 1000)
			if err != nil {
				log.Warnf("Failed to fetch datasets for tenant %s: %v", ct.ID, err)
			} else {
				for _, cd := range controlDatasets {
					// Only include active datasets
					if !cd.Active {
						continue
					}

					// Extract data hint from config if available
					dataHint := ""
					if sourceConfig, ok := cd.Config["source"].(map[string]interface{}); ok {
						if hint, ok := sourceConfig["data_hint"].(string); ok {
							dataHint = hint
						}
					}

					datasets = append(datasets, &domain.Dataset{
						ID:       cd.ID,
						Name:     cd.Name,
						TenantID: ct.ID,
						DataHint: dataHint,
						ProcessingConfig: &domain.ProcessingConfig{
							EnableRawStorage:   true,
							PartitioningScheme: "date",
						},
					})
				}
			}

			allTenants = append(allTenants, domain.Tenant{
				ID:          ct.ID,
				Name:        ct.Name,
				BearerToken: bearerToken,
				Datasets:    datasets,
			})
		}
	}

	log.Infof("Successfully fetched total of %d tenants from Control Service", len(allTenants))
	return allTenants, nil
}

// DisableTenant calls the control service to disable a tenant
func (tc *TenantClient) DisableTenant(tenantID string, reason string) error {
	// In development mode, just log and return
	if tc.config.Dev {
		log.Warnf("Development mode - would disable tenant %s: %s", tenantID, reason)
		return nil
	}

	// Use Control Service API if available
	if tc.controlClient != nil {
		log.Infof("Disabling tenant %s via Control Service: %s", tenantID, reason)

		ctx := context.Background()

		// First get the tenant to find its account_id
		// We need to search through all accounts to find which account owns this tenant
		accounts, err := tc.controlClient.ListAccounts(ctx, 1000)
		if err != nil {
			return fmt.Errorf("failed to list accounts: %w", err)
		}

		var accountID string
		for _, account := range accounts {
			// Check if tenant exists in this account
			_, err := tc.controlClient.GetTenant(ctx, account.ID, tenantID)
			if err == nil {
				accountID = account.ID
				break
			}
		}

		if accountID == "" {
			return fmt.Errorf("tenant %s not found in any account", tenantID)
		}

		// Set active to false using UpdateTenantRequest
		active := false
		updateReq := client.UpdateTenantRequest{
			Active: &active,
		}

		// Update tenant
		if _, err := tc.controlClient.UpdateTenant(ctx, accountID, tenantID, updateReq); err != nil {
			return fmt.Errorf("failed to disable tenant: %w", err)
		}

		log.Infof("Successfully disabled tenant %s via Control Service", tenantID)
		return nil
	}

	return fmt.Errorf("control service not configured - cannot disable tenant")
}

// getFakeTenants returns fake tenant data for development
func (tc *TenantClient) getFakeTenants() []domain.Tenant {
	return []domain.Tenant{
		{
			ID:          "customer-1",
			Name:        "Customer 1 - Production eBPF Data",
			BearerToken: "eb4ba9e3236eaefae736495bd79f0d6de753bd7b6547b184ac0c439ff76982cf",
			Datasets: []*domain.Dataset{
				{
					ID:       "ebpf-data",
					Name:     "eBPF Observability Data",
					TenantID: "customer-1",
					ProcessingConfig: &domain.ProcessingConfig{
						EnableRawStorage:   true,
						PartitioningScheme: "date",
					},
				},
				{
					ID:       "sflow-data",
					Name:     "sFlow Network Monitoring Data",
					TenantID: "customer-1",
					DataHint: "sflow",
					ProcessingConfig: &domain.ProcessingConfig{
						EnableRawStorage:   true,
						PartitioningScheme: "date",
					},
				},
			},
		},
		{
			ID:          "tenant-001",
			Name:        "Development Tenant Alpha",
			BearerToken: "bearer-token-tenant-001-dev",
			Datasets: []*domain.Dataset{
				{
					ID:       "dataset-001",
					Name:     "Web Analytics",
					TenantID: "tenant-001",
					ProcessingConfig: &domain.ProcessingConfig{
						EnableRawStorage:   true,
						PartitioningScheme: "date",
					},
				},
				{
					ID:       "dataset-004",
					Name:     "User Events",
					TenantID: "tenant-001",
					ProcessingConfig: &domain.ProcessingConfig{
						EnableRawStorage:   true,
						PartitioningScheme: "date",
					},
				},
				{
					ID:       "dataset-007",
					Name:     "Sales Data",
					TenantID: "tenant-001",
					ProcessingConfig: &domain.ProcessingConfig{
						EnableRawStorage:   true,
						PartitioningScheme: "date",
					},
				},
			},
		},
		{
			ID:          "tenant-002",
			Name:        "Development Tenant Beta",
			BearerToken: "bearer-token-tenant-002-dev",
			Datasets: []*domain.Dataset{
				{
					ID:       "dataset-002",
					Name:     "User Events",
					TenantID: "tenant-002",
					ProcessingConfig: &domain.ProcessingConfig{
						EnableRawStorage:   true,
						PartitioningScheme: "date",
					},
				},
				{
					ID:       "dataset-005",
					Name:     "Web Analytics",
					TenantID: "tenant-002",
					ProcessingConfig: &domain.ProcessingConfig{
						EnableRawStorage:   true,
						PartitioningScheme: "date",
					},
				},
				{
					ID:       "dataset-008",
					Name:     "Sales Data",
					TenantID: "tenant-002",
					ProcessingConfig: &domain.ProcessingConfig{
						EnableRawStorage:   true,
						PartitioningScheme: "date",
					},
				},
			},
		},
		{
			ID:          "tenant-003",
			Name:        "Development Tenant Gamma",
			BearerToken: "bearer-token-tenant-003-dev",
			Datasets: []*domain.Dataset{
				{
					ID:       "dataset-003",
					Name:     "Sales Data",
					TenantID: "tenant-003",
					ProcessingConfig: &domain.ProcessingConfig{
						EnableRawStorage:   true,
						PartitioningScheme: "date",
					},
				},
				{
					ID:       "dataset-006",
					Name:     "Web Analytics",
					TenantID: "tenant-003",
					ProcessingConfig: &domain.ProcessingConfig{
						EnableRawStorage:   true,
						PartitioningScheme: "date",
					},
				},
				{
					ID:       "dataset-009",
					Name:     "User Events",
					TenantID: "tenant-003",
					ProcessingConfig: &domain.ProcessingConfig{
						EnableRawStorage:   true,
						PartitioningScheme: "date",
					},
				},
			},
		},
	}
}

func (tc *TenantClient) GetTenantByID(tenants []domain.Tenant, tenantID string) *domain.Tenant {
	for _, tenant := range tenants {
		if tenant.ID == tenantID {
			return &tenant
		}
	}
	return nil
}
