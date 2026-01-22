// Licensed under Elastic License 2.0
// See LICENSE.txt for details

package services

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/bytedance/sonic"
	"github.com/bytefreezer/goodies/log"
	"github.com/bytefreezer/receiver/config"
	"github.com/bytefreezer/receiver/domain"
	"github.com/bytefreezer/receiver/utils"
)

type UploadWorkerPool struct {
	config         *config.Config
	s3Client       S3ClientInterface
	workerCount    int
	uploadQueue    chan *UploadJob
	retryQueue     chan *UploadJob
	workers        []*UploadWorker
	ctx            context.Context
	cancel         context.CancelFunc
	wg             sync.WaitGroup
	stats          *UploadStats
	statsLock      sync.RWMutex
	triggerChannel <-chan struct{} // Channel to receive immediate processing triggers
}

type S3ClientInterface interface {
	PutObject(key string, data io.Reader, contentType string) error
	PutObjectWithMetadata(key string, data io.Reader, contentType string, metadata map[string]string) error
}

type UploadJob struct {
	ID          string
	TenantID    string
	DatasetID   string
	Tenant      *domain.Tenant
	Dataset     *domain.Dataset
	Data        []byte
	RemoteKey   string
	ContentType string
	Metadata    map[string]string
	Attempts    int
	MaxAttempts int
	CreatedAt   time.Time
	LastAttempt time.Time
	ResultChan  chan *UploadResult
	// Enhanced metadata
	LineCount      int64
	OriginalSize   int64
	ProcessedSize  int64
	ProcessingTime time.Duration
	// CacheEntry removed - using spooling service instead
}

type UploadResult struct {
	Job           *UploadJob
	Success       bool
	Error         error
	FileSize      int64
	UploadTime    time.Duration
	Location      string
	CachedLocally bool
	// Enhanced metadata for reporting
	TenantID       string
	DatasetID      string
	LineCount      int64
	ProcessingTime time.Duration
}

type UploadWorker struct {
	id          int
	pool        *UploadWorkerPool
	uploadQueue <-chan *UploadJob
	retryQueue  <-chan *UploadJob
}

type UploadStats struct {
	TotalJobs      int64
	SuccessfulJobs int64
	FailedJobs     int64
	RetriedJobs    int64
	TotalBytes     int64
	AverageTime    time.Duration
	WorkersActive  int
}

func NewUploadWorkerPool(cfg *config.Config, s3Client S3ClientInterface, _ interface{}, workerCount int) *UploadWorkerPool {
	if workerCount <= 0 {
		workerCount = 5 // Default worker count
	}

	ctx, cancel := context.WithCancel(context.Background())

	pool := &UploadWorkerPool{
		config:      cfg,
		s3Client:    s3Client,
		workerCount: workerCount,
		uploadQueue: make(chan *UploadJob, workerCount*10), // Buffer for jobs
		retryQueue:  make(chan *UploadJob, workerCount*5),  // Smaller retry buffer
		workers:     make([]*UploadWorker, workerCount),
		ctx:         ctx,
		cancel:      cancel,
		stats: &UploadStats{
			WorkersActive: workerCount,
		},
	}

	// Initialize workers
	for i := 0; i < workerCount; i++ {
		pool.workers[i] = &UploadWorker{
			id:          i,
			pool:        pool,
			uploadQueue: pool.uploadQueue,
			retryQueue:  pool.retryQueue,
		}
	}

	return pool
}

// SetTriggerChannel sets the channel to receive immediate processing triggers from DLQ service
func (up *UploadWorkerPool) SetTriggerChannel(triggerCh <-chan struct{}) {
	up.triggerChannel = triggerCh
}

// Start starts all workers in the pool
func (up *UploadWorkerPool) Start() {
	log.Infof("Starting upload worker pool with %d workers", up.workerCount)

	for i, worker := range up.workers {
		up.wg.Add(1)
		go worker.run(up.ctx, &up.wg)
		log.Debugf("Started upload worker %d", i)
	}

	// Start retry processor
	up.wg.Add(1)
	go up.retryProcessor(up.ctx, &up.wg)

	// Start queue directory scanner for direct processing
	up.wg.Add(1)
	go up.queueDirectoryScanner(up.ctx, &up.wg)

	log.Info("Upload worker pool started successfully")
}

// Stop stops all workers and waits for them to finish
func (up *UploadWorkerPool) Stop() {
	log.Info("Stopping upload worker pool")

	up.cancel()
	close(up.uploadQueue)
	close(up.retryQueue)

	up.wg.Wait()
	log.Info("Upload worker pool stopped")
}

// SubmitJob submits an upload job to the pool
func (up *UploadWorkerPool) SubmitJob(job *UploadJob) error {
	select {
	case up.uploadQueue <- job:
		up.updateStats(func(s *UploadStats) {
			s.TotalJobs++
		})
		log.Debugf("Submitted upload job %s for tenant %s", job.ID, job.TenantID)
		return nil
	case <-up.ctx.Done():
		return fmt.Errorf("upload worker pool is shutting down")
	default:
		return fmt.Errorf("upload queue is full, try again later")
	}
}

// CreateUploadJob creates a new upload job for webhook data
func (up *UploadWorkerPool) CreateUploadJob(tenant *domain.Tenant, dataset *domain.Dataset, data []byte, remoteKey string, lineCount int64, originalSize int64, processingTime time.Duration) *UploadJob {
	metadata := make(map[string]string)
	metadata["source"] = "webhook"
	metadata["tenant-id"] = tenant.ID
	metadata["dataset-id"] = dataset.ID
	metadata["line-count"] = fmt.Sprintf("%d", lineCount)
	metadata["original-size"] = fmt.Sprintf("%d", originalSize)
	metadata["file-size"] = fmt.Sprintf("%d", len(data))
	metadata["processing-time-ms"] = fmt.Sprintf("%d", processingTime.Milliseconds())
	metadata["processed-at"] = time.Now().Format(time.RFC3339)
	metadata["receiver-version"] = "direct-queue-processing"

	return &UploadJob{
		ID:          fmt.Sprintf("%s-%s-%d", tenant.ID, dataset.ID, time.Now().UnixNano()),
		TenantID:    tenant.ID,
		DatasetID:   dataset.ID,
		Tenant:      tenant,
		Dataset:     dataset,
		Data:        data,
		RemoteKey:   remoteKey,
		ContentType: "application/x-ndjson",
		Metadata:    metadata,
		MaxAttempts: 3,
		CreatedAt:   time.Now(),
		ResultChan:  make(chan *UploadResult, 1),
		// Enhanced metadata
		LineCount:      lineCount,
		OriginalSize:   originalSize,
		ProcessedSize:  int64(len(data)),
		ProcessingTime: processingTime,
	}
}

// CreateUploadJobFromCache removed - using spooling service instead

// GetStats returns current upload statistics
func (up *UploadWorkerPool) GetStats() *UploadStats {
	up.statsLock.RLock()
	defer up.statsLock.RUnlock()

	// Return a copy to avoid race conditions
	statsCopy := *up.stats
	return &statsCopy
}

// updateStats safely updates statistics
func (up *UploadWorkerPool) updateStats(updater func(*UploadStats)) {
	up.statsLock.Lock()
	defer up.statsLock.Unlock()
	updater(up.stats)
}

// retryProcessor handles failed uploads that need to be retried
func (up *UploadWorkerPool) retryProcessor(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	log.Debug("Started retry processor")

	retryTimer := time.NewTicker(30 * time.Second) // Process retries every 30 seconds
	defer retryTimer.Stop()

	for {
		select {
		case job := <-up.retryQueue:
			if job == nil {
				continue
			}

			// Check if we should retry based on backoff
			backoffDuration := time.Duration(job.Attempts*job.Attempts) * 10 * time.Second
			if time.Since(job.LastAttempt) < backoffDuration {
				// Put it back in retry queue after a delay
				go func(j *UploadJob) {
					time.Sleep(backoffDuration - time.Since(j.LastAttempt))
					select {
					case up.retryQueue <- j:
					case <-ctx.Done():
					}
				}(job)
				continue
			}

			// Submit for retry
			select {
			case up.uploadQueue <- job:
				up.updateStats(func(s *UploadStats) {
					s.RetriedJobs++
				})
				log.Debugf("Resubmitted job %s for retry (attempt %d)", job.ID, job.Attempts+1)
			case <-ctx.Done():
				return
			}

		case <-retryTimer.C:
			// Periodic cleanup or monitoring can go here

		case <-ctx.Done():
			log.Debug("Retry processor stopping")
			return
		}
	}
}

// queueDirectoryScanner scans queue directories for files ready for direct upload
func (up *UploadWorkerPool) queueDirectoryScanner(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	log.Debug("Started queue directory scanner")

	scanInterval := 30 * time.Second // Scan every 30 seconds
	ticker := time.NewTicker(scanInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Debug("Queue directory scanner stopping")
			return
		case <-ticker.C:
			up.scanQueueDirectories()
		default:
			// Non-blocking check for trigger channel
			if up.triggerChannel != nil {
				select {
				case <-up.triggerChannel:
					// Immediate scan triggered by DLQ resubmit
					log.Debug("Immediate queue scan triggered by DLQ resubmit")
					up.scanQueueDirectories()
				default:
					// No trigger, continue with normal flow
				}
			}
			// Small delay to prevent busy waiting
			time.Sleep(100 * time.Millisecond)
		}
	}
}

// scanQueueDirectories scans all queue directories for files ready for upload
func (up *UploadWorkerPool) scanQueueDirectories() {
	spoolPath := up.config.Bytefreezer.SpoolPath
	if spoolPath == "" {
		return
	}

	err := filepath.Walk(spoolPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}

		// Only process .ndjson.gz files in queue directories
		if !strings.Contains(path, "/queue/") || !strings.HasSuffix(path, ".ndjson.gz") {
			return nil
		}

		// Process queue file for direct upload
		if err := up.processQueueFile(path); err != nil {
			log.Warnf("Failed to process queue file %s: %v", path, err)
		}

		return nil
	})

	if err != nil {
		log.Warnf("Failed to scan queue directories: %v", err)
	}
}

// processQueueFile processes a single queue file for direct upload
func (up *UploadWorkerPool) processQueueFile(dataPath string) error {
	// Validate file path to prevent directory traversal attacks
	spoolPath := up.config.Bytefreezer.SpoolPath
	if spoolPath == "" {
		spoolPath = "/var/spool/bytefreezer-receiver"
	}
	if err := utils.ValidateFilePath(dataPath, spoolPath); err != nil {
		return fmt.Errorf("invalid file path: %w", err)
	}

	// Extract tenant and dataset from path: /spool/tenant/dataset/queue/filename.ndjson.gz
	pathParts := strings.Split(dataPath, "/")
	if len(pathParts) < 5 {
		return fmt.Errorf("invalid queue file path format: %s", dataPath)
	}

	tenantID := pathParts[len(pathParts)-4]
	datasetID := pathParts[len(pathParts)-3]
	filename := filepath.Base(dataPath)

	// Check file age to prevent processing files that are too new (might still be written)
	fileInfo, err := os.Stat(dataPath)
	if err != nil {
		return fmt.Errorf("failed to stat queue file: %w", err)
	}

	minAge := 5 * time.Second // Don't process files newer than 5 seconds
	if time.Since(fileInfo.ModTime()) < minAge {
		return nil // Skip recent files
	}

	// Load queue file data
	fileData, err := os.ReadFile(dataPath) // #nosec G304 - path validated above
	if err != nil {
		return fmt.Errorf("failed to read queue file: %w", err)
	}

	// Extract timestamp from proxy filename format: tenant--dataset--TIMESTAMP.ndjson.gz
	var originalTimestamp string
	if parts := strings.Split(filename, "--"); len(parts) >= 3 {
		// Format: tenant--dataset--timestamp.xxx.gz (where xxx is any extension)
		timestampPart := strings.TrimSuffix(parts[2], ".gz")
		log.Debugf("Parsing timestamp from filename %s, extracted: %s", filename, timestampPart)

		if parsedTime := parseTimestamp(timestampPart); !parsedTime.IsZero() {
			originalTimestamp = parsedTime.Format(time.RFC3339)
			log.Debugf("Parsed timestamp %s -> %s", timestampPart, originalTimestamp)
		}
	}
	if originalTimestamp == "" {
		originalTimestamp = fileInfo.ModTime().UTC().Format(time.RFC3339) // Fallback to file time
	}

	// Read proxy metadata if available
	var lineCount int64 = 0
	var originalBytes int64 = int64(len(fileData))
	metadataPath := dataPath + ".meta"
	// Validate metadata path to prevent directory traversal attacks
	if err := utils.ValidateFilePath(metadataPath, spoolPath); err != nil {
		log.Warnf("Invalid metadata file path: %v", err)
	} else if metadataData, err := os.ReadFile(filepath.Clean(metadataPath)); err == nil {
		var proxyMetadata map[string]interface{}
		if err := sonic.Unmarshal(metadataData, &proxyMetadata); err == nil {
			if lc, ok := proxyMetadata["line_count"].(float64); ok {
				lineCount = int64(lc)
				log.Infof("METADATA FIX: Using proxy line count from metadata: %d", lineCount)
			}
			if ob, ok := proxyMetadata["original_bytes"].(float64); ok {
				originalBytes = int64(ob)
				log.Debugf("Using proxy original bytes from metadata: %d", originalBytes)
			}
		} else {
			log.Warnf("Failed to parse metadata file %s: %v", metadataPath, err)
		}
	} else {
		log.Warnf("No metadata file found at %s, using estimated line count", metadataPath)
	}

	// Generate S3 key preserving original filename format
	s3Key := fmt.Sprintf("%s/%s/%s", tenantID, datasetID, filename)

	// Create enhanced upload job for queue file
	uploadJob := &UploadJob{
		ID:          fmt.Sprintf("queue-%s-%d", strings.TrimSuffix(filename, ".gz"), time.Now().UnixNano()),
		TenantID:    tenantID,
		DatasetID:   datasetID,
		Data:        fileData,
		RemoteKey:   s3Key,
		ContentType: "application/gzip",
		Metadata: map[string]string{
			"source":             "queue",
			"filename":           filename,
			"tenant-id":          tenantID,
			"dataset-id":         datasetID,
			"original-timestamp": originalTimestamp,
			"file-size":          fmt.Sprintf("%d", len(fileData)),
			"processed-at":       time.Now().Format(time.RFC3339),
			"receiver-version":   "direct-queue-processing",
		},
		MaxAttempts:    3,
		CreatedAt:      time.Now(),
		ResultChan:     make(chan *UploadResult, 1),
		LineCount:      lineCount,     // Use proxy line count from metadata
		OriginalSize:   originalBytes, // Use proxy original bytes from metadata
		ProcessedSize:  int64(len(fileData)),
		ProcessingTime: 0, // No merge processing time
	}

	// Submit to upload queue
	select {
	case up.uploadQueue <- uploadJob:
		log.Debugf("Submitted queue file %s for upload", filename)

		// Wait for result and handle completion
		go up.handleQueueFileResult(uploadJob, dataPath)

		return nil
	default:
		return fmt.Errorf("upload queue full")
	}
}

// handleQueueFileResult handles the result of a queue file upload
func (up *UploadWorkerPool) handleQueueFileResult(job *UploadJob, queuePath string) {
	select {
	case result := <-job.ResultChan:
		if result.Success {
			// Upload successful - remove queue file
			if err := os.Remove(queuePath); err != nil {
				log.Warnf("Failed to remove successful queue file %s: %v", queuePath, err)
			} else {
				log.Infof("Successfully uploaded and removed queue file %s", filepath.Base(queuePath))
			}
		} else {
			// Upload failed - move to DLQ
			if err := up.moveQueueFileToDLQ(queuePath, result.Error); err != nil {
				log.Errorf("Failed to move queue file to DLQ: %v", err)
			}
		}
	case <-time.After(5 * time.Minute):
		// Timeout - move to DLQ
		timeoutErr := fmt.Errorf("upload timeout after 5 minutes")
		if err := up.moveQueueFileToDLQ(queuePath, timeoutErr); err != nil {
			log.Errorf("Failed to move timed out queue file to DLQ: %v", err)
		}
	}
}

// moveQueueFileToDLQ moves a queue file to DLQ after upload failure
func (up *UploadWorkerPool) moveQueueFileToDLQ(queuePath string, uploadError error) error {
	// Extract tenant and dataset from path
	pathParts := strings.Split(queuePath, "/")
	if len(pathParts) < 5 {
		return fmt.Errorf("invalid queue file path format: %s", queuePath)
	}

	tenantID := pathParts[len(pathParts)-4]
	datasetID := pathParts[len(pathParts)-3]
	filename := filepath.Base(queuePath)

	// Create DLQ directory
	dlqDir := filepath.Join(up.config.Bytefreezer.SpoolPath, tenantID, datasetID, "dlq")
	if err := os.MkdirAll(dlqDir, 0750); err != nil {
		return fmt.Errorf("failed to create DLQ directory: %w", err)
	}

	// Create DLQ file paths
	dlqDataPath := filepath.Join(dlqDir, filename)
	// Create meta filename from base filename (remove .gz extension if present)
	dlqMetaPath := filepath.Join(dlqDir, filename+".meta")

	// Check for locally generated malformed filenames
	metaFilename := filename + ".meta"
	if strings.Contains(metaFilename, "..") {
		log.Errorf("MALFORMED LOCAL FILENAME DETECTED in DLQ: %s at path: %s", metaFilename, dlqMetaPath)

		// Store in malformed_local directory for investigation
		malformedDir := "/var/spool/bytefreezer-receiver/malformed_local"
		if err := os.MkdirAll(malformedDir, 0750); err != nil {
			log.Errorf("Failed to create malformed_local directory: %v", err)
		} else {
			malformedFilename := fmt.Sprintf("malformed_dlq_%d_%s", time.Now().UnixNano(), metaFilename)
			malformedPath := filepath.Join(malformedDir, malformedFilename)

			// Read the original file data for storage (safe: queuePath is internal)
			// #nosec G304 - queuePath is internal, not user input
			if fileData, err := os.ReadFile(queuePath); err == nil {
				if err := os.WriteFile(malformedPath, fileData, 0600); err != nil {
					log.Errorf("Failed to write malformed DLQ file: %v", err)
				} else {
					log.Errorf("Malformed DLQ file stored at: %s", malformedPath)
				}
			}
		}

		return fmt.Errorf("refusing to create malformed DLQ metadata file: %s", metaFilename)
	}

	// Move the data file
	if err := os.Rename(queuePath, dlqDataPath); err != nil {
		return fmt.Errorf("failed to move queue file to DLQ: %w", err)
	}

	// Create metadata for DLQ tracking
	metadata := map[string]interface{}{
		"id":             strings.TrimSuffix(filename, ".gz"),
		"tenant_id":      tenantID,
		"dataset_id":     datasetID,
		"filename":       filename,
		"status":         "dlq",
		"failure_reason": uploadError.Error(),
		"failed_at":      time.Now().Format(time.RFC3339),
		"source":         "queue",
		"retry_count":    1,
	}

	metaData, err := sonic.Marshal(metadata)
	if err != nil {
		// Try to move file back if metadata creation fails
		_ = os.Rename(dlqDataPath, queuePath) // #nosec G104 - best effort cleanup on error
		return fmt.Errorf("failed to create DLQ metadata: %w", err)
	}

	if err := os.WriteFile(dlqMetaPath, metaData, 0600); err != nil {
		// Try to move file back if metadata write fails
		_ = os.Rename(dlqDataPath, queuePath) // #nosec G104 - best effort cleanup on error
		return fmt.Errorf("failed to write DLQ metadata: %w", err)
	}

	log.Infof("Moved failed queue file %s to DLQ: %v", filename, uploadError)
	return nil
}

// run executes the worker loop
func (w *UploadWorker) run(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	log.Debugf("Upload worker %d started", w.id)

	for {
		select {
		case job := <-w.uploadQueue:
			if job == nil {
				continue
			}
			w.processJob(ctx, job)

		case job := <-w.retryQueue:
			if job == nil {
				continue
			}
			w.processJob(ctx, job)

		case <-ctx.Done():
			log.Debugf("Upload worker %d stopping", w.id)
			return
		}
	}
}

// processJob processes a single upload job
func (w *UploadWorker) processJob(ctx context.Context, job *UploadJob) {
	startTime := time.Now()
	job.Attempts++
	job.LastAttempt = startTime

	log.Debugf("Worker %d processing job %s for tenant %s (attempt %d)", w.id, job.ID, job.TenantID, job.Attempts)

	// PostgreSQL locks removed - using spool-based file approach for concurrency handling

	result := &UploadResult{
		Job:            job,
		FileSize:       int64(len(job.Data)),
		TenantID:       job.TenantID,
		DatasetID:      job.DatasetID,
		LineCount:      job.LineCount,
		ProcessingTime: job.ProcessingTime,
	}

	// Try S3 upload first with metadata
	dataReader := bytes.NewReader(job.Data)
	if err := w.pool.s3Client.PutObjectWithMetadata(job.RemoteKey, dataReader, job.ContentType, job.Metadata); err != nil {
		log.Warnf("Worker %d failed S3 upload for job %s: %v", w.id, job.ID, err)

		// Report error to control service for visibility in dashboard
		if w.pool.config.ErrorReporter != nil {
			w.pool.config.ErrorReporter.ReportErrorSimple(
				ctx,
				"s3_upload_failure",
				fmt.Sprintf("S3 upload failed: %v", err),
				"error",
				job.TenantID,
				job.DatasetID,
			)
		}

		// TODO: Move to spooling retry directory instead of cache
		// Currently removing cache functionality as per requirements
		result.Error = fmt.Errorf("S3 upload failed: %v", err)

		w.handleJobResult(ctx, job, result)
		return
	}

	// S3 upload successful
	result.Success = true
	result.UploadTime = time.Since(startTime)
	result.Location = fmt.Sprintf("s3://%s/%s", w.pool.config.S3Destination.BucketName, job.RemoteKey)

	// Cache entry handling removed - using spooling service instead

	w.handleJobResult(ctx, job, result)

	// Update stats
	w.pool.updateStats(func(s *UploadStats) {
		s.SuccessfulJobs++
		s.TotalBytes += result.FileSize
		// Update average time (simple moving average approximation)
		if s.SuccessfulJobs == 1 {
			s.AverageTime = result.UploadTime
		} else {
			s.AverageTime = (s.AverageTime + result.UploadTime) / 2
		}
	})

	log.Infof("Worker %d successfully uploaded %d bytes to %s in %v",
		w.id, result.FileSize, result.Location, result.UploadTime)
}

// handleJobResult handles the result of a job (success or failure)
func (w *UploadWorker) handleJobResult(ctx context.Context, job *UploadJob, result *UploadResult) {
	if result.Success {
		// Send result back to caller
		select {
		case job.ResultChan <- result:
		default:
			// Channel might be closed or not being read
		}
		return
	}

	// Handle failure
	w.pool.updateStats(func(s *UploadStats) {
		s.FailedJobs++
	})

	log.Errorf("Worker %d failed to process job %s (attempt %d): %v",
		w.id, job.ID, job.Attempts, result.Error)

	// Send SOC alert for upload failure
	if w.pool.config.SOCAlertClient != nil {
		alertErr := w.pool.config.SOCAlertClient.SendAlert(
			"medium",
			"Webhook Upload Failure",
			"Failed to upload webhook data to S3",
			fmt.Sprintf("Tenant: %s, Dataset: %s, Attempt: %d/%d, Error: %v",
				job.TenantID, job.DatasetID, job.Attempts, job.MaxAttempts, result.Error),
		)
		if alertErr != nil {
			log.Errorf("Failed to send SOC alert for upload failure: %v", alertErr)
		}
	}

	// Retry logic
	if job.Attempts < job.MaxAttempts {
		log.Infof("Queuing job %s for retry (%d/%d)", job.ID, job.Attempts, job.MaxAttempts)
		select {
		case w.pool.retryQueue <- job:
		case <-w.pool.ctx.Done():
			// Send failure result if shutting down
			select {
			case job.ResultChan <- result:
			default:
			}
		}
	} else {
		// Max attempts reached, send failure result
		log.Errorf("Job %s failed permanently after %d attempts", job.ID, job.Attempts)

		// Send critical SOC alert for permanent failures
		if w.pool.config.SOCAlertClient != nil {
			alertErr := w.pool.config.SOCAlertClient.SendAlert(
				"high",
				"Permanent Webhook Upload Failure",
				"Webhook data upload failed permanently after all retries",
				fmt.Sprintf("Tenant: %s, Dataset: %s, Attempts: %d, Final Error: %v",
					job.TenantID, job.DatasetID, job.MaxAttempts, result.Error),
			)
			if alertErr != nil {
				log.Errorf("Failed to send critical SOC alert for permanent upload failure: %v", alertErr)
			}
		}

		select {
		case job.ResultChan <- result:
		default:
		}
	}
}
