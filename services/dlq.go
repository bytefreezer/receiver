package services

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/bytedance/sonic"
	"github.com/bytefreezer/receiver/config"
	"github.com/bytefreezer/receiver/utils"
	"github.com/bytefreezer/goodies/log"
)

// DLQFile represents a file in the Dead Letter Queue
type DLQFile struct {
	ID            string    `json:"id"`
	TenantID      string    `json:"tenant_id"`
	DatasetID     string    `json:"dataset_id"`
	Filename      string    `json:"filename"`
	Size          int64     `json:"size"`
	LineCount     int64     `json:"line_count"`
	OriginalSize  int64     `json:"original_size"`
	CreatedAt     time.Time `json:"created_at"`
	LastRetry     time.Time `json:"last_retry"`
	RetryCount    int       `json:"retry_count"`
	Status        string    `json:"status"` // "pending", "retrying", "failed", "dlq"
	FailureReason string    `json:"failure_reason,omitempty"`
	S3Key         string    `json:"s3_key"`
}

// DLQStats represents DLQ statistics
type DLQStats struct {
	TotalFiles      int                 `json:"total_files"`
	TotalBytes      int64               `json:"total_bytes"`
	QueueFiles      int                 `json:"queue_files"`
	QueueBytes      int64               `json:"queue_bytes"`
	DLQFiles        int                 `json:"dlq_files"`
	DLQBytes        int64               `json:"dlq_bytes"`
	RetryFiles      int                 `json:"retry_files"`
	RetryBytes      int64               `json:"retry_bytes"`
	ByTenant        map[string]DLQStats `json:"by_tenant"`
	ByDataset       map[string]DLQStats `json:"by_dataset"`
	OldestQueueFile *time.Time          `json:"oldest_queue_file,omitempty"`
	OldestDLQFile   *time.Time          `json:"oldest_dlq_file,omitempty"`
}

// DLQService manages the Dead Letter Queue for failed S3 uploads
//
// The DLQ service provides management and monitoring capabilities for the
// spool-based file processing system. It does not handle the actual file
// progression (queue → retry → dlq) which is managed by the merge and upload workers.
//
// Key responsibilities:
// - Providing statistics on files in queue, retry, and dlq directories
// - Listing files in the DLQ system with filtering by tenant/dataset
// - Moving files from dlq back to retry for re-processing
// - Cleaning up old DLQ files based on age limits
//
// Architecture Integration:
// - Works with the same spool directory structure as other workers
// - Reads .ndjson.gz data files and .meta metadata files
// - Provides API endpoints for DLQ management and monitoring
type DLQService struct {
	config         *config.Config
	directory      string
	mutex          sync.RWMutex
	ctx            context.Context
	cancel         context.CancelFunc
	wg             sync.WaitGroup
	triggerChannel chan struct{} // Channel to trigger immediate queue processing
}

// NewDLQService creates a new DLQ service
func NewDLQService(cfg *config.Config) *DLQService {
	ctx, cancel := context.WithCancel(context.Background())

	// Use the same spool directory as the rest of the system
	directory := cfg.Bytefreezer.SpoolPath
	if directory == "" {
		directory = "/var/spool/bytefreezer-receiver"
	}

	service := &DLQService{
		config:         cfg,
		directory:      directory,
		ctx:            ctx,
		cancel:         cancel,
		triggerChannel: make(chan struct{}, 10), // Buffered channel for triggers
	}

	// Base directory is created by other components
	log.Infof("DLQ service using spool directory: %s", directory)

	return service
}

// Start starts the DLQ background workers
func (d *DLQService) Start() {
	if !d.config.DLQ.Enabled {
		log.Info("DLQ service is disabled")
		return
	}

	log.Info("Starting DLQ service...")

	// Start retry worker
	d.wg.Add(1)
	go d.retryWorker()

	// Start cleanup worker
	d.wg.Add(1)
	go d.cleanupWorker()

	log.Info("DLQ service started")
}

// Stop stops the DLQ service
func (d *DLQService) Stop() {
	if !d.config.DLQ.Enabled {
		return
	}

	log.Info("Stopping DLQ service...")
	d.cancel()
	d.wg.Wait()
	log.Info("DLQ service stopped")
}

// StoreFailedUpload is deprecated - DLQ progression is handled by upload worker
// Files move from retry -> dlq directly via the upload worker moveRetryToDLQ function
func (d *DLQService) StoreFailedUpload(tenantID, datasetID, s3Key string, data []byte, lineCount, originalSize int64, failureReason string) error {
	return fmt.Errorf("StoreFailedUpload is deprecated - use upload worker DLQ progression")
}

// retryWorker periodically retries failed uploads
func (d *DLQService) retryWorker() {
	defer d.wg.Done()

	retryInterval := time.Duration(d.config.DLQ.RetryIntervalSeconds) * time.Second
	if retryInterval <= 0 {
		retryInterval = 60 * time.Second // Default 1 minute
	}

	ticker := time.NewTicker(retryInterval)
	defer ticker.Stop()

	log.Infof("DLQ retry worker started (interval: %v)", retryInterval)

	for {
		select {
		case <-d.ctx.Done():
			log.Debug("DLQ retry worker stopping")
			return
		case <-ticker.C:
			d.processRetries()
		}
	}
}

// cleanupWorker periodically cleans up old DLQ files
func (d *DLQService) cleanupWorker() {
	defer d.wg.Done()

	cleanupInterval := time.Duration(d.config.DLQ.CleanupIntervalSeconds) * time.Second
	if cleanupInterval <= 0 {
		cleanupInterval = 300 * time.Second // Default 5 minutes
	}

	ticker := time.NewTicker(cleanupInterval)
	defer ticker.Stop()

	log.Infof("DLQ cleanup worker started (interval: %v)", cleanupInterval)

	for {
		select {
		case <-d.ctx.Done():
			log.Debug("DLQ cleanup worker stopping")
			return
		case <-ticker.C:
			d.processCleanup()
		}
	}
}

// processRetries processes files in queue and retry directories
func (d *DLQService) processRetries() {
	log.Debug("Processing DLQ retries...")

	// Process queue directory
	if err := d.processQueueDirectory(); err != nil {
		log.Warnf("Failed to process DLQ queue directory: %v", err)
	}

	// No retry directory processing needed with direct queue approach
}

// processQueueDirectory - Not used in new architecture
// DLQ processing is handled by upload worker scanning retry directories
func (d *DLQService) processQueueDirectory() error {
	// Queue processing is handled by merge worker (queue -> retry)
	// No direct DLQ processing from queue
	return nil
}


// processCleanup removes old files based on age and size limits
func (d *DLQService) processCleanup() {
	log.Debug("Processing DLQ cleanup...")

	maxAge := time.Duration(d.config.DLQ.MaxAgeDays) * 24 * time.Hour
	if maxAge <= 0 {
		return // No age-based cleanup
	}

	cutoffTime := time.Now().Add(-maxAge)

	_ = filepath.Walk(d.directory, func(path string, info os.FileInfo, err error) error { // #nosec G104 - DLQ cleanup is best effort
		if err != nil {
			return nil
		}

		// Only clean files in DLQ directory (not queue or retry)
		if !strings.Contains(path, "/dlq/") || !strings.HasSuffix(path, ".meta") {
			return nil
		}

		if info.ModTime().Before(cutoffTime) {
			// Remove old DLQ files (data file and .meta)
			// Extract original filename by removing .meta suffix
			dataPath := strings.TrimSuffix(path, ".meta")

			if err := os.Remove(path); err != nil {
				log.Warnf("Failed to remove old DLQ metadata %s: %v", path, err)
			}

			if err := os.Remove(dataPath); err != nil {
				log.Warnf("Failed to remove old DLQ data %s: %v", dataPath, err)
			}

			log.Debugf("Removed old DLQ file: %s", path)
		}

		return nil
	})
}

// loadMetadata loads DLQ metadata from a file
func (d *DLQService) loadMetadata(metaPath string) (*DLQFile, error) {
	// Validate file path to prevent directory traversal attacks
	if err := utils.ValidateFilePath(metaPath, d.directory); err != nil {
		return nil, fmt.Errorf("invalid metadata file path: %w", err)
	}

	data, err := os.ReadFile(metaPath) // #nosec G304 - path validated above
	if err != nil {
		return nil, err
	}

	var dlqFile DLQFile
	if err := sonic.Unmarshal(data, &dlqFile); err != nil {
		return nil, err
	}

	return &dlqFile, nil
}


// GetStats returns DLQ statistics
func (d *DLQService) GetStats() (*DLQStats, error) {
	if !d.config.DLQ.Enabled {
		return &DLQStats{}, nil
	}

	d.mutex.RLock()
	defer d.mutex.RUnlock()

	stats := &DLQStats{
		ByTenant:  make(map[string]DLQStats),
		ByDataset: make(map[string]DLQStats),
	}

	// Walk through spool directory and collect statistics
	err := filepath.Walk(d.directory, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}

		// Count .ndjson.gz files in queue, retry, and dlq directories
		if strings.HasSuffix(path, ".ndjson.gz") {
			pathParts := strings.Split(path, string(filepath.Separator))
			if len(pathParts) < 4 {
				return nil
			}

			// Extract tenant/dataset from path: /{spool}/{tenant}/{dataset}/{queue|retry|dlq}/file.ndjson.gz
			var tenantID, datasetID, dirType string
			for i, part := range pathParts {
				if part == "queue" || part == "retry" || part == "dlq" {
					if i >= 2 {
						tenantID = pathParts[i-2]
						datasetID = pathParts[i-1]
						dirType = part
					}
					break
				}
			}

			if tenantID == "" || datasetID == "" || dirType == "" {
				return nil
			}

			// Update total stats
			stats.TotalFiles++
			stats.TotalBytes += info.Size()

			// Categorize by directory type
			switch dirType {
			case "queue":
				stats.QueueFiles++
				stats.QueueBytes += info.Size()
				if stats.OldestQueueFile == nil || info.ModTime().Before(*stats.OldestQueueFile) {
					modTime := info.ModTime()
					stats.OldestQueueFile = &modTime
				}
			case "retry":
				stats.RetryFiles++
				stats.RetryBytes += info.Size()
			case "dlq":
				stats.DLQFiles++
				stats.DLQBytes += info.Size()
				if stats.OldestDLQFile == nil || info.ModTime().Before(*stats.OldestDLQFile) {
					modTime := info.ModTime()
					stats.OldestDLQFile = &modTime
				}
			}
		}

		return nil
	})

	return stats, err
}

// ListFiles returns a list of DLQ files for a tenant/dataset
func (d *DLQService) ListFiles(tenantID, datasetID string) ([]*DLQFile, error) {
	if !d.config.DLQ.Enabled {
		return []*DLQFile{}, nil
	}

	d.mutex.RLock()
	defer d.mutex.RUnlock()

	var files []*DLQFile
	searchPath := d.directory

	if tenantID != "" {
		searchPath = filepath.Join(d.directory, tenantID)
		if datasetID != "" {
			searchPath = filepath.Join(searchPath, datasetID)
		}
	}

	err := filepath.Walk(searchPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}

		// Look for .meta files in retry and dlq directories
		if !strings.HasSuffix(path, ".meta") {
			return nil
		}

		pathParts := strings.Split(path, string(filepath.Separator))
		dirType := ""
		for _, part := range pathParts {
			if part == "retry" || part == "dlq" {
				dirType = part
				break
			}
		}

		if dirType == "" {
			return nil // Skip queue files - those are handled by merge worker
		}

		dlqFile, err := d.loadMetadata(path)
		if err != nil {
			log.Warnf("Failed to load metadata from %s: %v", path, err)
			return nil
		}

		// Filter by tenant/dataset if specified
		if tenantID != "" && dlqFile.TenantID != tenantID {
			return nil
		}
		if datasetID != "" && dlqFile.DatasetID != datasetID {
			return nil
		}

		// Update status based on directory
		if dirType == "dlq" {
			dlqFile.Status = "dlq"
		} else if dirType == "retry" {
			dlqFile.Status = "retry"
		}

		files = append(files, dlqFile)
		return nil
	})

	return files, err
}

// RetryFiles moves files from DLQ back to retry for retry
// Supports wildcard patterns:
// - tenantID="*", datasetID="*": All tenants, all datasets
// - tenantID="*", datasetID="xyz": All tenants with dataset "xyz"
// - tenantID="xyz", datasetID="*": All datasets for tenant "xyz"
// - tenantID="xyz", datasetID="abc": Specific tenant and dataset
func (d *DLQService) RetryFiles(tenantID, datasetID string) (int, error) {
	if !d.config.DLQ.Enabled {
		return 0, fmt.Errorf("DLQ is disabled")
	}

	d.mutex.Lock()
	defer d.mutex.Unlock()

	retriedCount := 0

	// Handle wildcard patterns
	matchAllTenants := tenantID == "*"
	matchAllDatasets := datasetID == "*"

	log.Infof("Starting DLQ retry with pattern: tenant_id=%s, dataset_id=%s", tenantID, datasetID)

	err := filepath.Walk(d.directory, func(path string, info os.FileInfo, err error) error {
		if err != nil || !strings.HasSuffix(path, ".meta") || !strings.Contains(path, "/dlq/") {
			return nil
		}

		dlqFile, err := d.loadMetadata(path)
		if err != nil {
			return nil
		}

		// Apply wildcard matching logic
		tenantMatch := matchAllTenants || dlqFile.TenantID == tenantID
		datasetMatch := matchAllDatasets || dlqFile.DatasetID == datasetID

		if !tenantMatch || !datasetMatch {
			return nil
		}

		// Move back to queue directory for reprocessing
		if err := d.moveToQueue(dlqFile, path); err != nil {
			log.Warnf("Failed to move DLQ file back to queue: %v", err)
			return nil
		}

		retriedCount++
		log.Debugf("Moved DLQ file from tenant=%s, dataset=%s back to queue", dlqFile.TenantID, dlqFile.DatasetID)
		return nil
	})

	log.Infof("Moved %d files from DLQ back to queue for re-upload (pattern: tenant=%s, dataset=%s)", retriedCount, tenantID, datasetID)

	// Trigger immediate queue processing if files were moved
	if retriedCount > 0 {
		d.triggerQueueProcessing()
	}

	return retriedCount, err
}

// moveToQueue moves a file from DLQ back to queue directory for reprocessing
func (d *DLQService) moveToQueue(dlqFile *DLQFile, metaPath string) error {
	// Create queue directory
	queueDir := filepath.Join(d.directory, dlqFile.TenantID, dlqFile.DatasetID, "queue")
	if err := os.MkdirAll(queueDir, 0750); err != nil {
		return fmt.Errorf("failed to create queue directory: %w", err)
	}

	// Move only the data file back to queue
	// Queue files don't have metadata - they are processed directly
	dataPath := strings.TrimSuffix(metaPath, ".meta")
	newDataPath := filepath.Join(queueDir, filepath.Base(dataPath))

	// Move data file back to queue
	if err := os.Rename(dataPath, newDataPath); err != nil {
		return fmt.Errorf("failed to move data file to queue: %w", err)
	}

	// Remove original metadata file (no longer needed in queue)
	if err := os.Remove(metaPath); err != nil {
		log.Warnf("Failed to remove original DLQ metadata file %s: %v", metaPath, err)
	}

	log.Debugf("Moved DLQ file %s back to queue directory", dlqFile.ID)
	return nil
}

// triggerQueueProcessing sends a signal to trigger immediate queue processing
func (d *DLQService) triggerQueueProcessing() {
	select {
	case d.triggerChannel <- struct{}{}:
		log.Debug("Triggered immediate queue processing")
	default:
		log.Debug("Queue processing trigger channel is full, skipping")
	}
}

// GetTriggerChannel returns the channel used to signal queue processing triggers
// This allows other components (like upload worker pool) to listen for triggers
func (d *DLQService) GetTriggerChannel() <-chan struct{} {
	return d.triggerChannel
}
