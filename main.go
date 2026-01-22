// Licensed under Elastic License 2.0
// See LICENSE.txt for details

package main

import (
	"bytes"
	"flag"
	"fmt"
	mathrand "math/rand"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/bytedance/sonic"
	"github.com/bytefreezer/goodies/log"
	"github.com/bytefreezer/receiver/api"
	"github.com/bytefreezer/receiver/config"
	"github.com/bytefreezer/receiver/metrics"
	"github.com/bytefreezer/receiver/services"
	"github.com/bytefreezer/receiver/upload"
	"github.com/bytefreezer/receiver/webhook"
	"github.com/pkg/errors"
)

var (
	version   = "dev"
	buildTime = "unknown"
	gitCommit = "unknown"
	conf      = config.Config{}
	envPrefix = "BYTEFREEZER_RECEIVER_"
)

// Execute executes the root command.
func Run() error {
	// Note: As of Go 1.20, automatic seeding is used, no need to call rand.Seed

	// Define command line flags
	var (
		showVersion = flag.Bool("version", false, "Show version and exit")
		showHelp    = flag.Bool("help", false, "Show help and exit")
		cfgFilePath = flag.String("config", "config.yaml", "Path to configuration file")
	)

	flag.Parse()

	// Handle version flag
	if *showVersion {
		fmt.Printf("bytefreezer-receiver version %s (built %s, commit %s)\n", version, buildTime, gitCommit)
		os.Exit(0)
	}

	// Handle help flag
	if *showHelp {
		fmt.Printf("ByteFreezer Receiver - Data collection and processing service\n\n")
		fmt.Printf("Usage: %s [options]\n\n", os.Args[0])
		fmt.Printf("Options:\n")
		flag.PrintDefaults()
		os.Exit(0)
	}

	err := config.LoadConfig(*cfgFilePath, envPrefix, &conf)
	if err != nil {
		return errors.Wrap(err, "failed to load config")
	}

	setLogLevel(conf.Logging.Level)

	// Initialize OpenTelemetry
	if conf.Otel.Enabled {
		cleanup, err := initOTEL(&conf)
		if err != nil {
			return errors.Wrap(err, "failed to initialize OpenTelemetry")
		}
		defer cleanup()
		log.Info("OpenTelemetry initialized successfully")
	} else {
		log.Info("OpenTelemetry disabled")
	}

	// Initialize all components - fail hard if this fails
	log.Info("Initializing service components...")
	if err := conf.InitializeComponents(); err != nil {
		return errors.Wrap(err, "failed to initialize service components - service cannot start")
	}

	log.Infof("Tenant database initialized successfully with %d tenants", conf.TenantDatabase.GetTenantCount())

	// Send service start alert
	if err := conf.SOCAlertClient.SendServiceStartAlert(); err != nil {
		log.Warnf("Failed to send service start alert: %v", err)
	}

	// Cleanup stale operations from previous runs (self-healing)
	log.Infof("Cleaning up stale operations from previous runs...")
	cleanupStaleOperations(&conf)

	// Initialize health reporting service
	var healthReportingService *services.HealthReportingService
	if conf.HealthReporting.Enabled && conf.ControlService.ControlURL != "" {
		reportInterval := time.Duration(conf.HealthReporting.ReportInterval) * time.Second
		if reportInterval <= 0 {
			reportInterval = 5 * time.Minute // Default 5 minutes
			log.Warnf("Invalid report_interval %d, using default %v", conf.HealthReporting.ReportInterval, reportInterval)
		}

		timeout := time.Duration(conf.HealthReporting.TimeoutSeconds) * time.Second
		if timeout <= 0 {
			timeout = 30 * time.Second // Default 30 seconds
		}

		// Get actual hostname
		hostname, err := os.Hostname()
		if err != nil {
			log.Warnf("Failed to get hostname, using 'localhost': %v", err)
			hostname = "localhost"
		}

		// Determine instance API URL without protocol (receiver webhook endpoint)
		instanceAPI := fmt.Sprintf("%s:%d", hostname, conf.Protocols.Webhook.Port)

		// Build configuration data with masked sensitive fields
		configuration := buildHealthConfiguration(&conf, instanceAPI)

		healthReportingService = services.NewHealthReportingService(
			conf.ControlService.ControlURL,
			"bytefreezer-receiver",
			instanceAPI,
			conf.ControlService.APIKey, // System API key for authentication
			reportInterval,
			timeout,
			configuration,
		)

		// Register service on startup if enabled
		if conf.HealthReporting.RegisterOnStartup {
			if err := healthReportingService.RegisterService(); err != nil {
				log.Warnf("Failed to register service on startup: %v", err)
			}
		}

		// Start health reporting
		healthReportingService.Start()
		defer healthReportingService.Stop()
		log.Infof("Health reporting service started - reporting to %s every %v", conf.ControlService.ControlURL, reportInterval)
	} else {
		log.Info("Health reporting is disabled")
	}

	// Start server
	server := NewServer(&conf)

	server.HttpApi = api.NewAPI(&conf)

	// Initialize upload worker pool with config values

	workerCount := conf.Bytefreezer.UploadWorkerCount
	if workerCount <= 0 {
		workerCount = 5 // Default 5 workers
	}

	// Initialize spooling service (replaces DLQ service and upload pool)
	spoolingService := services.NewSpoolingService(&conf)

	// Initialize DLQ service (keeping for backward compatibility)
	dlqService := services.NewDLQService(&conf)
	conf.DLQService = dlqService

	// Initialize upload worker pool without cache
	uploadPool := services.NewUploadWorkerPool(&conf, conf.S3DestinationClient, nil, workerCount)

	// Connect DLQ trigger channel to upload worker pool for immediate processing
	uploadPool.SetTriggerChannel(dlqService.GetTriggerChannel())
	log.Debug("Connected DLQ trigger channel to upload worker pool for immediate processing")

	// Direct queue processing - three-stage pipeline: queue → retry → S3/DLQ
	log.Info("Using three-stage spooling pipeline for webhook data processing")

	// Initialize metrics if OTEL is enabled
	var receiverMetrics *metrics.ReceiverMetrics
	if conf.Otel.Enabled {
		var err error
		receiverMetrics, err = metrics.NewReceiverMetrics()
		if err != nil {
			log.Warnf("Failed to initialize receiver metrics: %v", err)
		} else {
			log.Info("Receiver metrics initialized successfully")
		}
	}

	// Store references in config
	conf.UploadWorkerPool = uploadPool
	conf.ReceiverMetrics = receiverMetrics

	// Start spooling service (three-stage pipeline)
	_ = spoolingService.Start() // #nosec G104 - service start handled by defer Stop
	defer spoolingService.Stop()

	// Start upload pool (for backward compatibility)
	uploadPool.Start()
	defer uploadPool.Stop()

	// Start DLQ service for failed upload management (backup system)
	dlqService.Start()
	defer dlqService.Stop()

	// Initialize webhook server
	server.WebhookServer = webhook.NewWebhookServer(&conf)

	// Batch accumulator removed - using spool-based approach

	// Start webhook server in background
	go server.WebhookServer.Start()

	// Initialize upload server (registers routes with existing API server)
	if conf.Protocols.Upload.Enabled {
		_ = upload.NewUploadServer(&conf)
		// Upload routes will be registered with the main API server
		log.Info("File upload server initialized")
	}

	// Track tenant update failures for SOC alerting
	var tenantUpdateFailures int

	// Define housekeeping function for tenant database updates
	housekeepingFn := func() {
		log.Debug("Starting housekeeping cycle...")

		// Step 1: Update tenant database
		log.Debug("Updating tenant database during housekeeping...")
		if err := conf.UpdateTenantDatabase(); err != nil {
			tenantUpdateFailures++
			log.Errorf("Failed to update tenant database during housekeeping: %v", err)

			// Send SOC alert for repeated failures
			if tenantUpdateFailures >= 3 && (tenantUpdateFailures%3 == 0) { // Alert every 3rd failure after the first 3
				if alertErr := conf.SOCAlertClient.SendTenantUpdateFailureAlert(err, tenantUpdateFailures); alertErr != nil {
					log.Warnf("Failed to send tenant update failure alert: %v", alertErr)
				}
			}
			return // Don't process tenants if database update failed
		} else {
			// Reset failure count on success
			if tenantUpdateFailures > 0 {
				log.Infof("Tenant database update recovered after %d failures", tenantUpdateFailures)
				tenantUpdateFailures = 0
			}
			log.Debugf("Tenant database updated successfully, current count: %d", conf.TenantDatabase.GetTenantCount())
		}

		// Step 2: Process retry files using spooling service
		log.Debug("Processing retry files during housekeeping...")
		if spoolingService != nil {
			// Spooling service processes files automatically in background
			log.Debug("Spooling service running for retry processing")
		}

		log.Info("Housekeeping completed: tenant database updated and retry files processed")

		log.Debug("Housekeeping cycle completed")
	}

	//start server
	go server.Start(housekeepingFn, nil)

	//start api server
	server.HttpApi.Serve(":"+strconv.Itoa(conf.Server.ApiPort), server.HttpApi.NewRouter())

	return nil
}

func setLogLevel(levelStr string) {
	switch strings.ToLower(levelStr) {
	case "debug":
		log.SetMinLogLevel(log.MinLevelDebug)
	case "info":
		log.SetMinLogLevel(log.MinLevelInfo)
	case "warn":
		log.SetMinLogLevel(log.MinLevelWarn)
	case "error":
		log.SetMinLogLevel(log.MinLevelError)
	}
}

// Server provides basic service functions and state common to all service types
type Server struct {
	Config        *config.Config
	Name          string
	quitterC      chan time.Duration // also internal-only
	HttpApi       *api.API
	WebhookServer *webhook.WebhookServer
}

func NewServer(conf *config.Config) *Server {
	return &Server{
		Config:   conf,
		Name:     conf.App.Name,
		quitterC: make(chan time.Duration),
	}
}

func (svc *Server) Start(housekeepingFn func(), quitterFn func(time.Duration)) {

	// exit cleanly on signal
	signalC := make(chan os.Signal, 1)
	signal.Notify(signalC, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGABRT, syscall.SIGTERM)
	go func() {
		sig := <-signalC
		log.Debugf("Received signal %v", sig)

		if err := svc.Stop(2 * time.Second); err != nil {
			log.Fatalf("error stopping service: %v", err)
		}
	}()

	baseInterval := time.Duration(svc.Config.Housekeeping.IntervalSeconds) * time.Second

	if baseInterval <= 0 {
		baseInterval = 10 * time.Second
		log.Errorf("invalid housekeeping-interval: %d", baseInterval)
	}

	log.Infof("Starting housekeeping with base interval %v (randomized 1x-2x for load balancing)", baseInterval)

	// Run initial housekeeping immediately on startup
	log.Info("Running initial housekeeping on startup...")
	if housekeepingFn != nil && svc.Config.Housekeeping.Enabled {
		housekeepingFn()
	}

	// Continue with randomized intervals for subsequent cycles
	for {
		// Generate random interval between baseInterval and 2*baseInterval
		// #nosec G404 - Using math/rand for timing jitter, not cryptographic purposes
		randomMultiplier := 1.0 + mathrand.Float64() // Random between 1.0 and 2.0
		nextInterval := time.Duration(float64(baseInterval) * randomMultiplier)

		log.Debugf("Next housekeeping in %v (base: %v, multiplier: %.2f)", nextInterval, baseInterval, randomMultiplier)

		timer := time.NewTimer(nextInterval)

		select {
		case <-timer.C:
			log.Debug("housekeeping")

			if housekeepingFn != nil && svc.Config.Housekeeping.Enabled {
				housekeepingFn()
			}

		case timeout := <-svc.quitterC:
			log.Debug("exiting service")
			timer.Stop()

			// Cleanup operations immediately - don't wait for completion
			log.Info("Cleaning up in-progress operations before shutdown...")
			cleanupStaleOperations(svc.Config)

			if quitterFn != nil {
				quitterFn(timeout)
			}

			svc.HttpApi.Stop()
			if svc.WebhookServer != nil {
				svc.WebhookServer.Stop()
			}

			return
		}
	}
}

func (svc *Server) Stop(timeout time.Duration) error {
	log.Debugf("sending timeout %s to quitterC:", timeout)

	// Try to send timeout to main loop, wait up to the timeout duration
	select {
	case svc.quitterC <- timeout:
		log.Debug("sent timeout to quitterC - graceful shutdown initiated")
	case <-time.After(timeout):
		log.Warn("timed out sending shutdown signal - forcing shutdown")
		// Main loop didn't respond within timeout, force close
		select {
		case <-svc.quitterC:
			// Channel already closed, nothing to do
			log.Debug("quitterC already closed")
		default:
			// Force close the channel to unblock main loop
			close(svc.quitterC)
			log.Debug("forced close of quitterC channel")
		}
	}
	return nil
}

// buildHealthConfiguration builds comprehensive configuration for health reporting
// with sensitive data masked
func buildHealthConfiguration(conf *config.Config, instanceAPI string) map[string]interface{} {
	maskSensitive := func(value string) string {
		if value == "" {
			return ""
		}
		if len(value) <= 4 {
			return "****"
		}
		return value[:2] + "****" + value[len(value)-2:]
	}

	return map[string]interface{}{
		"service_type":    "bytefreezer-receiver",
		"version":         conf.App.Version,
		"instance_api":    instanceAPI,
		"instance_id":     conf.InstanceID,
		"report_interval": conf.HealthReporting.ReportInterval,
		"timeout":         fmt.Sprintf("%ds", conf.HealthReporting.TimeoutSeconds),
		"api": map[string]interface{}{
			"port": conf.Server.ApiPort,
		},
		"webhook": map[string]interface{}{
			"port":                 conf.Protocols.Webhook.Port,
			"max_payload_size":     conf.Protocols.Webhook.MaxPayloadSize,
			"supported_formats":    conf.Protocols.Webhook.SupportedFormats,
			"enable_format_header": conf.Protocols.Webhook.EnableFormatHeader,
			"enabled":              conf.Protocols.Webhook.Enabled,
		},
		"upload": map[string]interface{}{
			"enabled":            conf.Protocols.Upload.Enabled,
			"max_file_size":      conf.Protocols.Upload.MaxFileSize,
			"allowed_extensions": conf.Protocols.Upload.AllowedExtensions,
		},
		"bytefreezer": map[string]interface{}{
			"upload_worker_count": conf.Bytefreezer.UploadWorkerCount,
			"spool_path":          conf.Bytefreezer.SpoolPath,
		},
		"s3_destination": map[string]interface{}{
			"bucket_name": conf.S3Destination.BucketName,
			"region":      conf.S3Destination.Region,
			"endpoint":    conf.S3Destination.Endpoint,
			"ssl":         conf.S3Destination.Ssl,
			"access_key":  maskSensitive(conf.S3Destination.AccessKey),
			"secret_key":  maskSensitive(conf.S3Destination.SecretKey),
		},
		"dlq": map[string]interface{}{
			"enabled":                  conf.DLQ.Enabled,
			"directory":                conf.DLQ.Directory,
			"max_size_bytes":           conf.DLQ.MaxSizeBytes,
			"retry_attempts":           conf.DLQ.RetryAttempts,
			"retry_interval_seconds":   conf.DLQ.RetryIntervalSeconds,
			"cleanup_interval_seconds": conf.DLQ.CleanupIntervalSeconds,
			"max_age_days":             conf.DLQ.MaxAgeDays,
		},
		"housekeeping": map[string]interface{}{
			"enabled":          conf.Housekeeping.Enabled,
			"interval_seconds": conf.Housekeeping.IntervalSeconds,
		},
		"control_service": map[string]interface{}{
			"enabled":     conf.ControlService.Enabled,
			"control_url": conf.ControlService.ControlURL,
			"api_key":     maskSensitive(conf.ControlService.APIKey),
			"timeout":     conf.ControlService.TimeoutSeconds,
		},
		"authentication": map[string]interface{}{
			"enabled": conf.Authentication.Enabled,
		},
		"otel": map[string]interface{}{
			"enabled":         conf.Otel.Enabled,
			"endpoint":        conf.Otel.Endpoint,
			"service_name":    conf.Otel.ServiceName,
			"prometheus_mode": conf.Otel.PrometheusMode,
		},
		"soc": map[string]interface{}{
			"enabled":  conf.SOC.Enabled,
			"endpoint": conf.SOC.Endpoint,
			"timeout":  conf.SOC.Timeout,
		},
		"capabilities": []string{
			"webhook_ingestion",
			"file_upload",
			"multi_protocol",
			"spooling",
			"dead_letter_queue",
			"format_detection",
		},
	}
}

// cleanupStaleOperations marks all in-progress operations for this instance as interrupted
// This allows the system to self-heal on restart - files will be picked up on next processing cycle
func cleanupStaleOperations(cfg *config.Config) {
	if cfg.ControlService.ControlURL == "" {
		log.Debug("Control service URL not configured, skipping operation cleanup")
		return
	}

	instanceID := cfg.InstanceID
	if instanceID == "" {
		instanceID, _ = os.Hostname()
		if instanceID == "" {
			instanceID = "receiver-unknown"
		}
	}

	payload := map[string]string{
		"service_type": "receiver",
		"instance_id":  instanceID,
	}

	body, err := sonic.Marshal(payload)
	if err != nil {
		log.Warnf("Failed to marshal cleanup request: %v", err)
		return
	}

	client := &http.Client{Timeout: 10 * time.Second}
	req, err := http.NewRequest("POST", cfg.ControlService.ControlURL+"/api/v1/activity/operations/cleanup", bytes.NewBuffer(body))
	if err != nil {
		log.Warnf("Failed to create cleanup request: %v", err)
		return
	}

	req.Header.Set("Content-Type", "application/json")
	if cfg.ControlService.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+cfg.ControlService.APIKey)
	}

	resp, err := client.Do(req)
	if err != nil {
		log.Warnf("Failed to cleanup stale operations: %v", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		var result struct {
			OperationsCleaned int `json:"operations_cleaned"`
		}
		if err := sonic.ConfigDefault.NewDecoder(resp.Body).Decode(&result); err == nil && result.OperationsCleaned > 0 {
			log.Infof("Cleaned up %d stale operations from previous run", result.OperationsCleaned)
		}
	} else {
		log.Warnf("Failed to cleanup stale operations: HTTP %d", resp.StatusCode)
	}
}

func main() {

	err := Run()
	if err != nil {
		log.Fatalf("failed to start: %s\n", err.Error())
		os.Exit(11)
	}
}
