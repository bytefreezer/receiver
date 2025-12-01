package config

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/env"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/v2"
	"github.com/n0needt0/go-goodies/log"
	pkgerrors "github.com/pkg/errors"

	"github.com/n0needt0/bytefreezer-receiver/alerts"
	"github.com/n0needt0/bytefreezer-receiver/domain"
	"github.com/n0needt0/bytefreezer-receiver/errors"
	"github.com/n0needt0/bytefreezer-receiver/metrics"
	"github.com/n0needt0/bytefreezer-receiver/storage"
	"github.com/n0needt0/bytefreezer-receiver/tenant"
)

var k = koanf.New(".")

type Config struct {
	App                  App                          `mapstructure:"app"`
	Logging              LoggingConfig                `mapstructure:"logging"`
	Server               Server                       `mapstructure:"server"`
	Protocols            Protocols                    `mapstructure:"protocols"`
	Bytefreezer          Bytefreezer                  `mapstructure:"bytefreezer"`
	ControlService       ControlServiceConfig         `mapstructure:"control_service"`
	HealthReporting      HealthReportingConfig        `mapstructure:"health_reporting"`
	ErrorTracking        ErrorTrackingConfig          `mapstructure:"error_tracking"`
	Authentication       AuthenticationConfig         `mapstructure:"authentication"`
	Otel                 Otel                         `mapstructure:"otel"`
	Housekeeping         Housekeeping                 `mapstructure:"housekeeping"`
	S3Destination        S3Destination                `mapstructure:"s3destination"`
	SOC                  SOCAlert                     `mapstructure:"soc"`
	DLQ                  DLQConfig                    `mapstructure:"dlq"`
	Dev                  bool                         `mapstructure:"dev"` // Deprecated - use control_service instead
	InstanceID           string                       `mapstructure:"-"`   // Auto-generated unique instance identifier
	TenantClient         *tenant.TenantClient         `mapstructure:"-"`
	TenantDatabase       *tenant.TenantDatabase       `mapstructure:"-"`
	SOCAlertClient       *alerts.SOCAlertClient       `mapstructure:"-"`
	S3DestinationClient  *storage.S3DestinationClient `mapstructure:"-"`
	ErrorReporter        *errors.ErrorReporter        `mapstructure:"-"`
	UploadWorkerPool     interface{}                  `mapstructure:"-"` // Interface to avoid import cycles
	DLQService           interface{}                  `mapstructure:"-"` // Interface to avoid import cycles
	WebhookMetrics       interface{}                  `mapstructure:"-"` // Interface to avoid import cycles
	ReceiverMetrics      interface{}                  `mapstructure:"-"` // Interface to avoid import cycles
	DatasetMetricsClient interface{}                  `mapstructure:"-"` // Interface to avoid import cycles
}

// ErrorTrackingConfig represents error tracking configuration
type ErrorTrackingConfig struct {
	Enabled bool `mapstructure:"enabled"`
}

type AuthenticationConfig struct {
	Enabled bool `mapstructure:"enabled"` // Enable bearer token authentication
}

type ControlServiceConfig struct {
	Enabled        bool   `mapstructure:"enabled"`
	BaseURL        string `mapstructure:"base_url"`
	APIKey         string `mapstructure:"api_key"`
	TimeoutSeconds int    `mapstructure:"timeout_seconds"`
}

// HealthReportingConfig represents health reporting configuration
// Note: Uses control_service.base_url for the control service endpoint
type HealthReportingConfig struct {
	Enabled            bool   `mapstructure:"enabled"`
	ReportInterval     int    `mapstructure:"report_interval"` // Interval in seconds
	TimeoutSeconds     int    `mapstructure:"timeout_seconds"`
	RegisterOnStartup  bool   `mapstructure:"register_on_startup"`
}

type Bytefreezer struct {
	UploadWorkerCount int    `mapstructure:"upload_worker_count"`
	SpoolPath         string `mapstructure:"spool_path"`
}

type Housekeeping struct {
	Enabled         bool `mapstructure:"enabled"`
	IntervalSeconds int  `mapstructure:"intervalseconds"`
}

type App struct {
	Name    string `mapstructure:"name"`
	Version string `mapstructure:"version"`
}

// LoggingConfig stores global logging configurations
type LoggingConfig struct {
	Level    string `mapstructure:"level"`
	Encoding string `mapstructure:"encoding"`
}

type Server struct {
	ApiPort int `mapstructure:"api_port"`
}

type S3Destination struct {
	BucketName string `mapstructure:"bucket_name"`
	Region     string `mapstructure:"region"`
	AccessKey  string `mapstructure:"access_key"`
	SecretKey  string `mapstructure:"secret_key"`
	SecretName string `mapstructure:"secret_name"`
	Endpoint   string `mapstructure:"endpoint"`
	Ssl        bool   `mapstructure:"ssl"`
	UseIamRole bool   `mapstructure:"use_iam_role"`
}

type Otel struct {
	Enabled               bool   `mapstructure:"enabled"`
	Endpoint              string `mapstructure:"endpoint"`
	ServiceName           string `mapstructure:"service_name"`
	ScrapeIntervalSeconds int    `mapstructure:"scrapeIntervalseconds"`
	PrometheusMode        bool   `mapstructure:"prometheus_mode"`
	MetricsPort           int    `mapstructure:"metrics_port"`
	MetricsHost           string `mapstructure:"metrics_host"`
}

type SOCAlert struct {
	Enabled  bool   `mapstructure:"enabled"`
	Endpoint string `mapstructure:"endpoint"`
	Timeout  int    `mapstructure:"timeout"`
}

// DLQConfig defines Dead Letter Queue configuration
type DLQConfig struct {
	Enabled                bool   `mapstructure:"enabled"`
	Directory              string `mapstructure:"directory"`
	MaxSizeBytes           int64  `mapstructure:"max_size_bytes"`
	RetryAttempts          int    `mapstructure:"retry_attempts"`
	RetryIntervalSeconds   int    `mapstructure:"retry_interval_seconds"`
	CleanupIntervalSeconds int    `mapstructure:"cleanup_interval_seconds"`
	MaxAgeDays             int    `mapstructure:"max_age_days"`
}

// Protocols defines the multi-protocol ingestion configuration
type Protocols struct {
	Webhook WebhookProtocol `mapstructure:"webhook"`
	Upload  UploadProtocol  `mapstructure:"upload"`
}

// WebhookProtocol defines HTTP webhook ingestion settings
type WebhookProtocol struct {
	Enabled            bool     `mapstructure:"enabled"`
	Port               int      `mapstructure:"port"`
	MaxPayloadSize     int      `mapstructure:"max_payload_size"`
	SupportedFormats   []string `mapstructure:"supported_formats"`
	EnableFormatHeader bool     `mapstructure:"enable_format_header"`
}

// UploadProtocol defines file upload settings
type UploadProtocol struct {
	Enabled           bool     `mapstructure:"enabled"`
	MaxFileSize       int      `mapstructure:"max_file_size"`
	AllowedExtensions []string `mapstructure:"allowed_extensions"`
}

// generateInstanceID creates a unique instance identifier for distributed locking
func generateInstanceID() string {
	// Try to get hostname first
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}

	// Generate random suffix
	bytes := make([]byte, 4)
	if _, err := rand.Read(bytes); err != nil {
		// Fallback to timestamp if crypto/rand fails
		return fmt.Sprintf("%s-%d", hostname, time.Now().Unix())
	}

	return fmt.Sprintf("%s-%s", hostname, hex.EncodeToString(bytes))
}

func LoadConfig(cfgFile, envPrefix string, cfg *Config) error {
	if cfgFile == "" {
		cfgFile = "config.yaml"
	}

	err := k.Load(file.Provider(cfgFile), yaml.Parser())
	if err != nil {
		return pkgerrors.Wrapf(err, "failed to parse %s", cfgFile)
	}

	if err := k.Load(env.Provider(envPrefix, ".", func(s string) string {
		return strings.Replace(strings.ToLower(strings.TrimPrefix(s, envPrefix)), "_", ".", -1)
	}), nil); err != nil {
		return pkgerrors.Wrapf(err, "error loading config from env")
	}

	err = k.UnmarshalWithConf("", &cfg, koanf.UnmarshalConf{Tag: "mapstructure"})
	if err != nil {
		return pkgerrors.Wrapf(err, "failed to unmarshal %s", cfgFile)
	}

	// Generate unique instance ID for distributed locking
	cfg.InstanceID = generateInstanceID()
	log.Infof("Generated instance ID: %s", cfg.InstanceID)

	return nil
}

// InitializeComponents initializes all components (tenant, SOC, spool directory)
func (cfg *Config) InitializeComponents() error {
	// Initialize error reporter if configured
	if cfg.ErrorTracking.Enabled && cfg.ControlService.BaseURL != "" {
		cfg.ErrorReporter = errors.NewErrorReporter(
			cfg.ControlService.BaseURL,
			cfg.ControlService.APIKey,
			"receiver",
			true,
		)
		log.Infof("Error reporter initialized - reporting to control service at %s", cfg.ControlService.BaseURL)
	} else {
		log.Infof("Error reporting disabled (enabled: %v, control_service: %s)", cfg.ErrorTracking.Enabled, cfg.ControlService.BaseURL)
	}

	// Initialize SOC alert client
	cfg.SOCAlertClient = alerts.NewSOCAlertClient(alerts.AlertClientConfig{
		SOC: alerts.SOCConfig{
			Enabled:  cfg.SOC.Enabled,
			Endpoint: cfg.SOC.Endpoint,
			Timeout:  cfg.SOC.Timeout,
		},
		App: alerts.AppConfig{
			Name:    cfg.App.Name,
			Version: cfg.App.Version,
		},
		Dev: cfg.Dev,
	})

	// Initialize dataset metrics client
	cfg.DatasetMetricsClient = metrics.NewDatasetMetricsClient(
		cfg.ControlService.BaseURL,
		cfg.ControlService.APIKey,
		cfg.ControlService.TimeoutSeconds,
		cfg.ControlService.Enabled,
	)
	log.Infof("Dataset metrics client initialized (enabled: %v, endpoint: %s)",
		cfg.ControlService.Enabled, cfg.ControlService.BaseURL)

	// Validate and create spool directory
	spoolPath := cfg.Bytefreezer.SpoolPath
	if spoolPath == "" {
		spoolPath = "/var/spool/bytefreezer-receiver" // Default
		cfg.Bytefreezer.SpoolPath = spoolPath
	}

	if err := os.MkdirAll(spoolPath, 0750); err != nil {
		// Send critical alert for spool directory failure
		if alertErr := cfg.SOCAlertClient.SendCriticalAlert(
			"Spool Directory Creation Failed",
			"Failed to create spool directory - service cannot start",
			fmt.Sprintf("Path: %s, Error: %v", spoolPath, err),
		); alertErr != nil {
			log.Warnf("Failed to send SOC alert: %v", alertErr)
		}
		return fmt.Errorf("failed to create spool directory %s: %w", spoolPath, err)
	}
	log.Infof("Spool directory validated/created: %s", spoolPath)

	// Initialize S3 destination client if configured
	if cfg.S3Destination.BucketName != "" {
		s3Dest := &domain.S3Destination{
			BucketName: cfg.S3Destination.BucketName,
			Region:     cfg.S3Destination.Region,
			AccessKey:  cfg.S3Destination.AccessKey,
			SecretKey:  cfg.S3Destination.SecretKey,
			Endpoint:   cfg.S3Destination.Endpoint,
			Ssl:        cfg.S3Destination.Ssl,
			UseIamRole: cfg.S3Destination.UseIamRole,
		}

		s3Client, err := storage.NewS3DestinationClient(s3Dest)
		if err != nil {
			// Send critical alert for S3 destination failure
			if alertErr := cfg.SOCAlertClient.SendCriticalAlert(
				"S3 Destination Client Initialization Failed",
				"Failed to initialize S3 destination client - service cannot start",
				fmt.Sprintf("Error: %v", err),
			); alertErr != nil {
				log.Warnf("Failed to send SOC alert: %v", alertErr)
			}
			return fmt.Errorf("failed to initialize S3 destination client: %w", err)
		}
		cfg.S3DestinationClient = s3Client

		// Test S3 connection
		if err := cfg.S3DestinationClient.TestConnection(); err != nil {
			// Send critical alert for S3 connection failure
			if alertErr := cfg.SOCAlertClient.SendCriticalAlert(
				"S3 Destination Connection Failed",
				"Failed to connect to S3 destination - service cannot start",
				fmt.Sprintf("Error: %v", err),
			); alertErr != nil {
				log.Warnf("Failed to send SOC alert: %v", alertErr)
			}
			return fmt.Errorf("failed to test S3 destination connection: %w", err)
		}
		log.Info("S3 destination client initialized and tested successfully")
	}

	// Initialize tenant client and database
	tenantClientConf := tenant.TenantClientConfig{
		App: tenant.AppConfig{
			Name:    cfg.App.Name,
			Version: cfg.App.Version,
		},
		Bytefreezer: tenant.BytefreezerConfig{},
		ControlService: tenant.ControlServiceConfig{
			Enabled:        cfg.ControlService.Enabled,
			BaseURL:        cfg.ControlService.BaseURL,
			APIKey:         cfg.ControlService.APIKey,
			TimeoutSeconds: cfg.ControlService.TimeoutSeconds,
		},
		Dev: cfg.Dev,
	}

	cfg.TenantClient = tenant.NewTenantClient(tenantClientConf)
	cfg.TenantDatabase = tenant.NewTenantDatabase(cfg.TenantClient)

	// Populate tenant database with retry logic
	// PopulateDatabase will retry with exponential backoff and start with empty database if control service is unavailable
	// Service will continue to retry fetching tenants in background via UpdateDatabase
	if err := cfg.TenantDatabase.PopulateDatabase(); err != nil {
		// This should never happen now since PopulateDatabase returns nil to allow service to start
		// Keeping error handling for safety
		log.Warnf("Unexpected error during tenant database population: %v", err)
	}

	return nil
}

// UpdateTenantDatabase updates the tenant database from the controller
func (cfg *Config) UpdateTenantDatabase() error {
	return cfg.TenantDatabase.UpdateDatabase()
}
