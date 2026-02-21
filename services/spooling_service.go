// Licensed under Elastic License 2.0
// See LICENSE.txt for details

package services

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bytedance/sonic"
	"github.com/bytefreezer/goodies/log"
	"github.com/bytefreezer/receiver/config"
	"github.com/bytefreezer/receiver/utils"
)

// parseTimestamp attempts to parse various timestamp formats to time.Time
func parseTimestamp(timestampStr string) time.Time {
	// Try Unix timestamp (nanoseconds)
	if len(timestampStr) >= 10 {
		if unixNano, err := strconv.ParseInt(timestampStr, 10, 64); err == nil {
			// Check if it's in nanoseconds (19 digits) or seconds (10 digits)
			if len(timestampStr) >= 15 {
				return time.Unix(0, unixNano).UTC()
			} else {
				return time.Unix(unixNano, 0).UTC()
			}
		}
	}

	// Try common timestamp formats
	formats := []string{
		time.RFC3339,
		"20060102-150405",  // YYYYMMDD-HHMMSS
		"20060102T150405Z", // ISO-like format
		"2006-01-02T15:04:05Z",
		"2006-01-02 15:04:05",
	}

	for _, format := range formats {
		if t, err := time.Parse(format, timestampStr); err == nil {
			return t.UTC()
		}
	}

	// Could not parse
	return time.Time{}
}

// checkMalformedFilename checks if a filename contains double dots and stores it in malformed_local if so
func (s *SpoolingService) checkMalformedFilename(filename, filePath string, data []byte) bool {
	if strings.Contains(filename, "..") {
		log.Errorf("MALFORMED LOCAL FILENAME DETECTED: %s at path: %s", filename, filePath)

		// Store in malformed_local directory for investigation
		malformedDir := filepath.Join(s.directory, "../malformed_local")
		if err := os.MkdirAll(malformedDir, 0750); err != nil {
			log.Errorf("Failed to create malformed_local directory: %v", err)
			return true // Still consider it malformed
		}

		// Use timestamp for unique malformed filename
		malformedFilename := fmt.Sprintf("malformed_%d_%s", time.Now().UnixNano(), filename)
		malformedPath := filepath.Join(malformedDir, malformedFilename)

		// Store the malformed file for investigation
		if err := os.WriteFile(malformedPath, data, 0600); err != nil {
			log.Errorf("Failed to write malformed local file: %v", err)
		}

		log.Errorf("Malformed local file stored at: %s", malformedPath)
		return true // File is malformed
	}
	return false // File is OK
}

// SpoolingService manages the three-stage pipeline: queue → retry → S3/DLQ
type SpoolingService struct {
	config        *config.Config
	directory     string // Base spool directory
	wg            sync.WaitGroup
	shutdown      chan struct{}
	retryInterval time.Duration
	running       bool
	mu            sync.RWMutex
}

// QueueFile represents a file in the queue
type QueueFile struct {
	TenantID         string    `json:"tenant_id"`
	DatasetID        string    `json:"dataset_id"`
	Filename         string    `json:"filename"`
	FilePath         string    `json:"file_path"`
	CompressedSize   int64     `json:"compressed_size"`
	UncompressedSize int64     `json:"uncompressed_size"`
	LineCount        int64     `json:"line_count"`
	CreatedAt        time.Time `json:"created_at"`
	Status           string    `json:"status"` // "queue", "retry", "dlq", "success"
}

// RetryFile represents a file in retry with metadata
type RetryFile struct {
	QueueFile
	RetryCount    int       `json:"retry_count"`
	LastRetry     time.Time `json:"last_retry"`
	FailureReason string    `json:"failure_reason,omitempty"`
}

// RetryJob represents a retry task for a worker (proxy pattern)
type RetryJob struct {
	TenantID  string
	DatasetID string
	BatchID   string
	FilePath  string
	MetaPath  string
}

// NewSpoolingService creates a new spooling service
func NewSpoolingService(cfg *config.Config) *SpoolingService {
	return &SpoolingService{
		config:        cfg,
		directory:     cfg.Bytefreezer.SpoolPath,
		shutdown:      make(chan struct{}),
		retryInterval: time.Duration(cfg.DLQ.RetryIntervalSeconds) * time.Second,
		running:       false,
	}
}

// Start starts the spooling service with queue and retry processors
func (s *SpoolingService) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.running {
		return fmt.Errorf("spooling service already running")
	}

	log.Info("Starting spooling service with queue and retry processors")

	// Process any orphaned queue files from previous session
	if err := s.processOrphanedQueueFiles(); err != nil {
		log.Warnf("Failed to process orphaned queue files: %v", err)
	}

	// Start workers
	s.wg.Add(2)
	go s.queueWorker() // Queue → Retry processor
	go s.retryWorker() // Retry → S3/DLQ processor

	s.running = true
	log.Info("Spooling service started successfully")
	return nil
}

// Stop gracefully stops the spooling service
func (s *SpoolingService) Stop() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.running {
		return nil
	}

	log.Info("Stopping spooling service...")
	close(s.shutdown)
	s.wg.Wait()
	s.running = false
	log.Info("Spooling service stopped")
	return nil
}

// queueWorker processes files from queue directory and moves them to retry
func (s *SpoolingService) queueWorker() {
	defer s.wg.Done()
	ticker := time.NewTicker(5 * time.Second) // Process queue every 5 seconds
	defer ticker.Stop()

	log.Info("Queue processor started")

	for {
		select {
		case <-s.shutdown:
			log.Info("Queue processor shutting down")
			return
		case <-ticker.C:
			s.processQueueFiles()
		}
	}
}

// retryWorker processes files from retry directory and uploads to S3 or moves to DLQ
func (s *SpoolingService) retryWorker() {
	defer s.wg.Done()

	log.Info("Retry processor started (continuous mode)")

	// Continuous processing with brief pauses only when no work is found
	for {
		select {
		case <-s.shutdown:
			log.Info("Retry processor shutting down")
			return
		default:
			// Process files immediately
			filesProcessed := s.processRetryFiles()

			// Only pause if no files were processed to avoid busy-waiting
			if filesProcessed == 0 {
				select {
				case <-s.shutdown:
					return
				case <-time.After(5 * time.Second): // Brief pause when no work
					continue
				}
			}
			// If files were processed, immediately check for more work
		}
	}
}

// processQueueFiles scans all queue directories and moves files to retry
func (s *SpoolingService) processQueueFiles() {
	tenants, err := s.getTenants()
	if err != nil {
		log.Errorf("Failed to get tenants: %v", err)
		return
	}

	log.Debugf("QUEUE DEBUG: Found %d tenants: %v", len(tenants), tenants)
	totalProcessed := 0
	for _, tenantID := range tenants {
		datasets, err := s.getDatasets(tenantID)
		if err != nil {
			log.Errorf("Failed to get datasets for tenant %s: %v", tenantID, err)
			continue
		}

		log.Debugf("QUEUE DEBUG: Tenant %s has %d datasets: %v", tenantID, len(datasets), datasets)
		for _, datasetID := range datasets {
			processed := s.processQueueForDataset(tenantID, datasetID)
			totalProcessed += processed
		}
	}

	if totalProcessed > 0 {
		log.Infof("Queue processor: moved %d files from queue to retry", totalProcessed)
	} else {
		log.Debugf("QUEUE DEBUG: No files processed this cycle")
	}
}

// processQueueForDataset processes queue files for a specific tenant/dataset
func (s *SpoolingService) processQueueForDataset(tenantID, datasetID string) int {
	queueDir := filepath.Join(s.directory, tenantID, datasetID, "queue")
	retryDir := filepath.Join(s.directory, tenantID, datasetID, "retry")

	// Check if queue directory exists
	if _, err := os.Stat(queueDir); os.IsNotExist(err) {
		return 0
	}

	// Create retry directory if it doesn't exist
	if err := os.MkdirAll(retryDir, 0750); err != nil {
		log.Errorf("Failed to create retry directory %s: %v", retryDir, err)
		return 0
	}

	// Read queue directory
	entries, err := os.ReadDir(queueDir)
	if err != nil {
		log.Errorf("Failed to read queue directory %s: %v", queueDir, err)
		return 0
	}

	log.Debugf("QUEUE DEBUG: Processing %s/%s queue with %d entries", tenantID, datasetID, len(entries))

	processed := 0
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".gz") {
			log.Debugf("QUEUE DEBUG: Skipping non-gz file: %s", entry.Name())
			continue
		}
		log.Debugf("QUEUE DEBUG: Processing gz file: %s", entry.Name())

		if s.moveQueueFileToRetry(tenantID, datasetID, entry.Name()) {
			processed++
		}
	}

	return processed
}

// moveQueueFileToRetry moves a file from queue to retry with metadata
func (s *SpoolingService) moveQueueFileToRetry(tenantID, datasetID, filename string) bool {
	queueFilePath := filepath.Join(s.directory, tenantID, datasetID, "queue", filename)
	retryFilePath := filepath.Join(s.directory, tenantID, datasetID, "retry", filename)
	// Create meta filename by adding .meta to the end of the data filename
	retryMetaPath := filepath.Join(s.directory, tenantID, datasetID, "retry", filename+".meta")

	// Check for locally generated malformed filenames
	metaFilename := filename + ".meta"
	if s.checkMalformedFilename(metaFilename, retryMetaPath, nil) {
		log.Errorf("Refusing to create malformed metadata file: %s", metaFilename)
		return false
	}

	// Get file info
	fileInfo, err := os.Stat(queueFilePath)
	if err != nil {
		log.Errorf("Failed to stat queue file %s: %v", queueFilePath, err)
		return false
	}

	// Read proxy metadata if available, otherwise estimate line count
	compressedSize := fileInfo.Size()
	var actualLineCount int64
	metadataPath := queueFilePath + ".meta"
	// Validate metadata path to prevent directory traversal attacks
	if err := utils.ValidateFilePath(metadataPath, s.config.Bytefreezer.SpoolPath); err != nil {
		log.Warnf("Invalid metadata file path: %v", err)
		actualLineCount = compressedSize / 100 // Fallback estimate
	} else if metadataData, err := os.ReadFile(filepath.Clean(metadataPath)); err == nil {
		var proxyMetadata map[string]interface{}
		if err := sonic.Unmarshal(metadataData, &proxyMetadata); err == nil {
			if lc, ok := proxyMetadata["line_count"].(float64); ok {
				actualLineCount = int64(lc)
				log.Infof("RETRY FIX: Using proxy line count from metadata: %d", actualLineCount)
			} else {
				actualLineCount = compressedSize / 100 // Fallback estimate
				log.Warnf("No line_count in metadata, using estimate: %d", actualLineCount)
			}
		} else {
			actualLineCount = compressedSize / 100 // Fallback estimate
			log.Warnf("Failed to parse metadata, using estimate: %d", actualLineCount)
		}
	} else {
		actualLineCount = compressedSize / 100 // Fallback estimate
		log.Warnf("No metadata file found, using estimate: %d", actualLineCount)
	}

	// Create retry metadata
	retryFile := RetryFile{
		QueueFile: QueueFile{
			TenantID:         tenantID,
			DatasetID:        datasetID,
			Filename:         filename,
			FilePath:         retryFilePath,
			CompressedSize:   compressedSize,
			UncompressedSize: 0, // Not used - files are already compressed
			LineCount:        actualLineCount,
			CreatedAt:        fileInfo.ModTime(),
			Status:           "retry",
		},
		RetryCount:    0,
		LastRetry:     time.Time{},
		FailureReason: "",
	}

	// Move file to retry directory
	if err := os.Rename(queueFilePath, retryFilePath); err != nil {
		log.Errorf("Failed to move file from queue to retry: %v", err)
		return false
	}

	// Write metadata
	metaData, err := sonic.Marshal(retryFile)
	if err != nil {
		log.Errorf("Failed to marshal retry metadata: %v", err)
		// Try to move file back to queue
		_ = os.Rename(retryFilePath, queueFilePath) // #nosec G104 - best effort cleanup on error
		return false
	}

	if err := os.WriteFile(retryMetaPath, metaData, 0600); err != nil {
		log.Errorf("Failed to write retry metadata: %v", err)
		// Try to move file back to queue
		_ = os.Rename(retryFilePath, queueFilePath) // #nosec G104 - best effort cleanup on error
		return false
	}

	log.Debugf("Moved file from queue to retry: %s/%s/%s", tenantID, datasetID, filename)
	return true
}

// processRetryFiles scans all retry directories and uploads files to S3 using concurrent workers (proxy pattern)
func (s *SpoolingService) processRetryFiles() int {
	tenants, err := s.getTenants()
	if err != nil {
		log.Errorf("Failed to get tenants for retry processing: %v", err)
		return 0
	}

	// Collect all retry jobs from all tenants
	var jobs []RetryJob
	log.Debugf("Found %d tenants for retry processing: %v", len(tenants), tenants)

	for _, tenantID := range tenants {
		tenantJobs := s.collectRetryJobs(tenantID)
		log.Debugf("Tenant %s: collected %d retry jobs", tenantID, len(tenantJobs))
		jobs = append(jobs, tenantJobs...)
	}

	if len(jobs) == 0 {
		log.Debugf("No retry jobs found for processing")
		return 0 // No jobs to process
	}

	// Use worker pool pattern like proxy
	workerCount := s.config.Bytefreezer.UploadWorkerCount
	if workerCount <= 0 {
		workerCount = 5 // Default to 5 workers like proxy
	}

	log.Infof("Processing %d retry jobs with %d upload workers", len(jobs), workerCount)

	// Create job channel and results channel
	jobChannel := make(chan RetryJob, len(jobs))
	resultChannel := make(chan struct {
		success  bool
		tenantID string
		batchID  string
	}, len(jobs))

	// Start worker pool
	var wg sync.WaitGroup
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go s.retryJobWorker(i, jobChannel, resultChannel, &wg)
	}

	// Send jobs to workers
	for _, job := range jobs {
		jobChannel <- job
	}
	close(jobChannel)

	// Wait for all workers to complete
	wg.Wait()
	close(resultChannel)

	// Collect results
	totalSuccessCount := 0
	totalFailureCount := 0
	tenantResults := make(map[string]struct{ success, failed int })

	for result := range resultChannel {
		if result.success {
			totalSuccessCount++
		} else {
			totalFailureCount++
		}

		stats := tenantResults[result.tenantID]
		if result.success {
			stats.success++
		} else {
			stats.failed++
		}
		tenantResults[result.tenantID] = stats
	}

	// Log results per tenant
	for tenantID, stats := range tenantResults {
		if stats.success > 0 || stats.failed > 0 {
			log.Infof("Tenant %s retry results: %d succeeded, %d failed", tenantID, stats.success, stats.failed)
		}
	}

	if totalSuccessCount > 0 || totalFailureCount > 0 {
		log.Infof("Total retry results: %d succeeded, %d failed", totalSuccessCount, totalFailureCount)
	}

	// Return total number of jobs processed (both success and failure)
	return totalSuccessCount + totalFailureCount
}

// collectRetryJobs collects all retry jobs for a tenant (proxy pattern)
func (s *SpoolingService) collectRetryJobs(tenantID string) []RetryJob {
	var jobs []RetryJob

	// Get all datasets for this tenant
	datasets, err := s.getDatasets(tenantID)
	if err != nil {
		log.Errorf("Failed to get datasets for tenant %s: %v", tenantID, err)
		return jobs
	}

	for _, datasetID := range datasets {
		retryDir := filepath.Join(s.directory, tenantID, datasetID, "retry")

		// Check if retry directory exists
		if _, err := os.Stat(retryDir); os.IsNotExist(err) {
			continue
		}

		// Read retry directory
		entries, err := os.ReadDir(retryDir)
		if err != nil {
			log.Errorf("Failed to read retry directory %s: %v", retryDir, err)
			continue
		}

		log.Debugf("Found %d entries in retry directory %s", len(entries), retryDir)

		// Process each file
		for _, entry := range entries {
			if entry.IsDir() {
				continue
			}

			fileName := entry.Name()
			if !strings.HasSuffix(fileName, ".meta") {
				continue // Only process .meta metadata files
			}

			// Extract batch ID from metadata filename
			batchID := strings.TrimSuffix(fileName, ".meta")
			metaFilePath := filepath.Join(retryDir, fileName)

			// Find corresponding data file - batchID is now the full filename
			dataFilePath := filepath.Join(retryDir, batchID)
			if _, err := os.Stat(dataFilePath); err != nil {
				dataFilePath = "" // File doesn't exist
			}

			if dataFilePath == "" {
				log.Warnf("Metadata file %s exists but no corresponding data file found: %s",
					metaFilePath, batchID)
				continue
			}

			log.Debugf("RETRY DEBUG: Found retry job - Tenant: %s, Dataset: %s, BatchID: %s, DataFile: %s, MetaFile: %s",
				tenantID, datasetID, batchID, dataFilePath, metaFilePath)

			jobs = append(jobs, RetryJob{
				TenantID:  tenantID,
				DatasetID: datasetID,
				BatchID:   batchID,
				FilePath:  dataFilePath,
				MetaPath:  metaFilePath,
			})
		}
	}

	return jobs
}

// retryJobWorker processes retry jobs from the job channel (proxy pattern)
func (s *SpoolingService) retryJobWorker(workerID int, jobChannel <-chan RetryJob, resultChannel chan<- struct {
	success  bool
	tenantID string
	batchID  string
}, wg *sync.WaitGroup) {
	defer wg.Done()

	log.Debugf("Retry worker %d started", workerID)

	for job := range jobChannel {
		log.Debugf("Worker %d processing retry job for batch %s", workerID, job.BatchID)

		success := s.processRetryJob(job)

		resultChannel <- struct {
			success  bool
			tenantID string
			batchID  string
		}{
			success:  success,
			tenantID: job.TenantID,
			batchID:  job.BatchID,
		}
	}

	log.Debugf("Retry worker %d finished", workerID)
}

// processRetryJob processes a single retry job (proxy pattern)
func (s *SpoolingService) processRetryJob(job RetryJob) bool {
	// Validate metadata file path to prevent directory traversal
	if err := utils.ValidateFilePath(job.MetaPath, s.directory); err != nil {
		log.Errorf("Invalid metadata file path %s: %v", job.MetaPath, err)
		return false
	}

	// Read metadata
	// #nosec G304 - metaPath is validated against controlled spool directory
	metaData, err := os.ReadFile(job.MetaPath)
	if err != nil {
		log.Errorf("Failed to read metadata for batch %s: %v", job.BatchID, err)
		return false
	}

	var retryFile RetryFile
	if err := sonic.Unmarshal(metaData, &retryFile); err != nil {
		log.Errorf("Failed to unmarshal metadata for batch %s: %v", job.BatchID, err)
		return false
	}

	// Check if we should retry this file
	if retryFile.RetryCount >= s.config.DLQ.RetryAttempts {
		log.Warnf("Batch %s exceeded max retries (%d), moving to DLQ", job.BatchID, s.config.DLQ.RetryAttempts)
		if s.moveToDLQFromJob(job, &retryFile, "max_retries_exceeded") {
			return false // Moved to DLQ, not a success but handled
		}
		return false
	}

	// Try to upload the file
	log.Debugf("RETRY DEBUG: Attempting S3 upload for batch %s - File: %s, Filename in metadata: %s",
		job.BatchID, job.FilePath, retryFile.Filename)
	success := s.attemptS3Upload(job.TenantID, job.DatasetID, job.FilePath, &retryFile)

	if success {
		// Remove successful file and metadata
		log.Infof("✅ Retry upload successful for batch %s (%d bytes compressed, %d lines)",
			job.BatchID, retryFile.CompressedSize, retryFile.LineCount)

		if err := s.removeSuccessfulRetryFile(job); err != nil {
			log.Errorf("Failed to cleanup successful retry file %s: %v", job.BatchID, err)
		}
		return true
	} else {
		// Update retry count and metadata
		retryFile.RetryCount++
		retryFile.LastRetry = time.Now()
		retryFile.FailureReason = "S3 upload failed"

		if err := s.updateRetryMetadata(job.MetaPath, &retryFile); err != nil {
			log.Errorf("Failed to update retry metadata for batch %s: %v", job.BatchID, err)
		}
		return false
	}
}

// attemptS3Upload attempts to upload a file to S3
func (s *SpoolingService) attemptS3Upload(tenantID, datasetID, filePath string, retryFile *RetryFile) bool {
	if s.config.S3DestinationClient == nil {
		log.Error("S3 destination client not configured")
		return false
	}

	// Validate data file path to prevent directory traversal
	if err := utils.ValidateFilePath(filePath, s.directory); err != nil {
		log.Errorf("Invalid data file path %s: %v", filePath, err)
		return false
	}

	// Read file data
	// #nosec G304 - filePath is validated against controlled spool directory
	data, err := os.ReadFile(filePath)
	if err != nil {
		log.Errorf("Failed to read file %s: %v", filePath, err)
		return false
	}

	// Create S3 key preserving original filename structure
	// For proxy files: tenant--dataset--timestamp.[datatype].gz
	// For direct uploads: timestamp-uuid.ndjson.gz
	s3Key := fmt.Sprintf("%s/%s/%s", tenantID, datasetID, retryFile.Filename)
	log.Debugf("S3 DEBUG: Uploading to S3 key: %s (file size: %d bytes)", s3Key, len(data))

	// Parse proxy filename format for enhanced metadata using new format
	var dataType, originalTimestamp string
	filename := retryFile.Filename

	// Extract data type using the new helper function that handles both formats
	dataType = utils.ExtractFileExtension(filename)
	if dataType == "" {
		dataType = "raw" // Default fallback
	}

	// Extract original timestamp from new format: tenant--dataset--timestamp--extension.gz
	if strings.Contains(filename, "--") {
		parts := strings.Split(strings.TrimSuffix(filename, ".gz"), "--")
		if len(parts) >= 4 {
			// parts[2] is the timestamp from the new format
			originalTimestamp = parts[2]
		} else if len(parts) >= 3 {
			// Fallback for old format: try to extract timestamp from third part
			timestampAndType := parts[2]
			if idx := strings.Index(timestampAndType, "."); idx > 0 {
				originalTimestamp = timestampAndType[:idx]
			}
		}
	}

	// Create comprehensive S3 metadata
	metadata := map[string]string{
		"tenant":             tenantID,
		"dataset":            datasetID,
		"original-filename":  retryFile.Filename,
		"bytes":              fmt.Sprintf("%d", retryFile.CompressedSize),
		"line-count":         fmt.Sprintf("%d", retryFile.LineCount),
		"receiver-queued-at": retryFile.CreatedAt.Format(time.RFC3339),
		"s3-uploaded-at":     time.Now().UTC().Format(time.RFC3339),
		"retry-count":        fmt.Sprintf("%d", retryFile.RetryCount),
		"source":             "bytefreezer-receiver",
	}

	// Add proxy-specific metadata if available
	if dataType != "" {
		metadata["data-type"] = dataType
	}
	// Set filename source metadata
	if originalTimestamp != "" {
		metadata["filename-source"] = "proxy"
		// Only add proxy-created-at if we can parse it to RFC3339
		if parsedTime := parseTimestamp(originalTimestamp); !parsedTime.IsZero() {
			metadata["proxy-created-at"] = parsedTime.Format(time.RFC3339)
		}
	} else {
		metadata["filename-source"] = "generated"
	}

	// Upload to S3
	reader := strings.NewReader(string(data))
	err = s.config.S3DestinationClient.PutObjectWithMetadata(s3Key, reader, "application/gzip", metadata)
	if err != nil {
		log.Errorf("Failed to upload %s to S3: %v", s3Key, err)

		// Report error to control service for visibility in dashboard
		if s.config.ErrorReporter != nil {
			s.config.ErrorReporter.ReportErrorSimple(
				context.Background(),
				"s3_upload_failure",
				fmt.Sprintf("S3 upload failed during retry processing: %v", err),
				"error",
				tenantID,
				datasetID,
			)
		}
		return false
	}

	log.Debugf("Successfully uploaded to S3: %s", s3Key)
	return true
}

// processOrphanedQueueFiles processes any orphaned queue files from previous session
func (s *SpoolingService) processOrphanedQueueFiles() error {
	log.Info("Processing orphaned queue files from previous session")

	tenants, err := s.getTenants()
	if err != nil {
		return fmt.Errorf("failed to get tenants: %w", err)
	}

	totalProcessed := 0
	for _, tenantID := range tenants {
		datasets, err := s.getDatasets(tenantID)
		if err != nil {
			log.Errorf("Failed to get datasets for tenant %s: %v", tenantID, err)
			continue
		}

		for _, datasetID := range datasets {
			processed := s.processQueueForDataset(tenantID, datasetID)
			totalProcessed += processed
		}
	}

	if totalProcessed > 0 {
		log.Infof("Processed %d orphaned queue files", totalProcessed)
	}

	return nil
}

// getTenants returns all tenant directories
func (s *SpoolingService) getTenants() ([]string, error) {
	entries, err := os.ReadDir(s.directory)
	if err != nil {
		return nil, err
	}

	var tenants []string
	for _, entry := range entries {
		if entry.IsDir() {
			tenants = append(tenants, entry.Name())
		}
	}

	return tenants, nil
}

// getDatasets returns all dataset directories for a tenant
func (s *SpoolingService) getDatasets(tenantID string) ([]string, error) {
	tenantDir := filepath.Join(s.directory, tenantID)
	entries, err := os.ReadDir(tenantDir)
	if err != nil {
		return nil, err
	}

	var datasets []string
	for _, entry := range entries {
		if entry.IsDir() {
			datasets = append(datasets, entry.Name())
		}
	}

	return datasets, nil
}

// moveToDLQFromJob moves a file to the DLQ directory from a RetryJob (proxy pattern)
func (s *SpoolingService) moveToDLQFromJob(job RetryJob, retryFile *RetryFile, reason string) bool {
	dlqDir := filepath.Join(s.directory, job.TenantID, job.DatasetID, "dlq")

	// Create DLQ directory
	if err := os.MkdirAll(dlqDir, 0750); err != nil {
		log.Errorf("Failed to create DLQ directory %s: %v", dlqDir, err)
		return false
	}

	// Move files to DLQ
	dataDest := filepath.Join(dlqDir, job.BatchID)
	metaDest := filepath.Join(dlqDir, job.BatchID+".meta")

	// Update metadata with DLQ reason
	retryFile.FailureReason = reason
	retryFile.Status = "dlq"

	// Move data file
	if err := os.Rename(job.FilePath, dataDest); err != nil {
		log.Errorf("Failed to move data file to DLQ: %v", err)
		return false
	}

	// Write updated metadata to DLQ
	metaData, err := sonic.Marshal(retryFile)
	if err != nil {
		log.Errorf("Failed to marshal DLQ metadata: %v", err)
		// Try to move data file back
		_ = os.Rename(dataDest, job.FilePath) // #nosec G104 - best effort cleanup on error
		return false
	}

	if err := os.WriteFile(metaDest, metaData, 0600); err != nil {
		log.Errorf("Failed to write DLQ metadata: %v", err)
		// Try to move data file back
		_ = os.Rename(dataDest, job.FilePath) // #nosec G104 - best effort cleanup on error
		return false
	}

	// Remove original metadata file from retry directory
	if err := os.Remove(job.MetaPath); err != nil && !os.IsNotExist(err) {
		log.Warnf("Failed to remove original metadata file %s: %v", job.MetaPath, err)
	}

	log.Infof("Moved file to DLQ: %s/%s/%s (reason: %s)", job.TenantID, job.DatasetID, job.BatchID, reason)
	return true
}

// removeSuccessfulRetryFile removes a successful retry file and metadata (proxy pattern)
func (s *SpoolingService) removeSuccessfulRetryFile(job RetryJob) error {
	// Remove data file
	if err := os.Remove(job.FilePath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to remove data file %s: %w", job.FilePath, err)
	}

	// Remove metadata file
	if err := os.Remove(job.MetaPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to remove metadata file %s: %w", job.MetaPath, err)
	}

	log.Debugf("Cleaned up successful retry files for batch %s", job.BatchID)
	return nil
}

// updateRetryMetadata updates the retry metadata file (proxy pattern)
func (s *SpoolingService) updateRetryMetadata(metaPath string, retryFile *RetryFile) error {
	// Validate metadata file path to prevent directory traversal
	if err := utils.ValidateFilePath(metaPath, s.directory); err != nil {
		return fmt.Errorf("invalid metadata file path %s: %w", metaPath, err)
	}

	metaData, err := sonic.Marshal(retryFile)
	if err != nil {
		return fmt.Errorf("failed to marshal retry metadata: %w", err)
	}

	if err := os.WriteFile(metaPath, metaData, 0600); err != nil {
		return fmt.Errorf("failed to write retry metadata: %w", err)
	}

	return nil
}
