package webhook

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/bytedance/sonic"
	"github.com/bytefreezer/goodies/log"
	"github.com/bytefreezer/receiver/domain"
	"github.com/bytefreezer/receiver/utils"
	"github.com/go-chi/chi/v5"
)

// healthHandler handles webhook health checks
func (ws *WebhookServer) healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status": "ok", "service": "webhook"}`)) // #nosec G104 - health endpoint response
}

// receiveDataHandler handles incoming NDJSON data using spool-based architecture
//
// This handler implements the first stage of the proxy-like file processing pipeline:
// 1. Receives webhook data (optionally compressed with gzip)
// 2. Validates payload size limits
// 3. Compresses data to .ndjson.gz format
// 4. Stores in queue directory: /var/spool/bytefreezer-receiver/{tenant}/{dataset}/queue/
// 5. Returns success response with processing metadata
//
// The stored files are later processed by merge workers which batch them for upload.
// This approach ensures crash resilience and no data loss through file-based coordination.
func (ws *WebhookServer) receiveDataHandler(w http.ResponseWriter, r *http.Request) {
	tenantID := chi.URLParam(r, "tenantId")
	datasetID := chi.URLParam(r, "datasetId")

	log.Debugf("Received webhook request for tenant=%s, dataset=%s", tenantID, datasetID)

	// Get tenant and dataset from context (set by middleware)
	tenant, ok := r.Context().Value(tenantContextKey).(*domain.Tenant)
	if !ok || tenant == nil {
		log.Errorf("Tenant not found in context")
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	dataset, ok := r.Context().Value(datasetContextKey).(*domain.Dataset)
	if !ok || dataset == nil {
		log.Errorf("Dataset not found in context")
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	// PostgreSQL locks removed - using spool-based file system approach for concurrency handling

	// Check payload size limits
	maxSize := int64(ws.config.Protocols.Webhook.MaxPayloadSize)
	if maxSize > 0 && r.ContentLength > maxSize {
		log.Warnf("Payload too large for %s/%s: %d > %d bytes", tenantID, datasetID, r.ContentLength, maxSize)

		// Send SOC alert for oversized payload
		if ws.config.SOCAlertClient != nil {
			alertErr := ws.config.SOCAlertClient.SendAlert(
				"medium",
				"Oversized Payload Rejected",
				"Webhook payload exceeded maximum size limit",
				fmt.Sprintf("Tenant: %s, Dataset: %s, Size: %d bytes, Limit: %d bytes",
					tenantID, datasetID, r.ContentLength, maxSize),
			)
			if alertErr != nil {
				log.Warnf("Failed to send oversized payload alert: %v", alertErr)
			}
		}

		http.Error(w, "Payload too large", http.StatusRequestEntityTooLarge)
		return
	}

	// Create limited reader to enforce size limit during reading
	var bodyReader io.Reader = r.Body
	if maxSize > 0 {
		bodyReader = io.LimitReader(r.Body, maxSize+1) // +1 to detect oversized
	}

	// Check for compression - if gzip, keep it compressed
	var body []byte
	var isAlreadyCompressed bool
	contentEncoding := r.Header.Get("Content-Encoding")

	if strings.ToLower(contentEncoding) == "gzip" {
		// Keep the gzip compressed data as-is
		var err error
		body, err = io.ReadAll(bodyReader)
		if err != nil {
			log.Errorf("Failed to read compressed request body: %v", err)
			http.Error(w, "Failed to read request body", http.StatusBadRequest)
			return
		}
		isAlreadyCompressed = true
		log.Debugf("Keeping gzip-compressed payload for %s/%s: %d compressed bytes", tenantID, datasetID, len(body))
	} else {
		// Read uncompressed data
		var err error
		body, err = io.ReadAll(bodyReader)
		if err != nil {
			log.Errorf("Failed to read request body: %v", err)
			http.Error(w, "Failed to read request body", http.StatusBadRequest)
			return
		}
		isAlreadyCompressed = false
		log.Debugf("Received uncompressed payload for %s/%s: %d bytes", tenantID, datasetID, len(body))
	}
	defer r.Body.Close()

	if len(body) == 0 {
		http.Error(w, "Empty request body", http.StatusBadRequest)
		return
	}

	// Track processing start time
	processingStartTime := time.Now()

	// Raw data storage - no format detection needed
	contentType := r.Header.Get("Content-Type")
	log.Debugf("Storing raw data for %s/%s (Content-Type: %s, Size: %d bytes)", tenantID, datasetID, contentType, len(body))

	// Use proxy headers - do not recalculate
	var originalLineCount int64
	proxyLineCountStr := r.Header.Get("X-Proxy-Line-Count")
	if proxyLineCountStr != "" {
		if count, err := strconv.ParseInt(proxyLineCountStr, 10, 64); err == nil {
			originalLineCount = count
			log.Infof("HEADER FIX: Using proxy line count: %d", originalLineCount)
		} else {
			log.Warnf("Invalid X-Proxy-Line-Count header: %s", proxyLineCountStr)
			originalLineCount = 1 // Fallback
		}
	} else {
		log.Warnf("No X-Proxy-Line-Count header found")
		originalLineCount = 1 // Fallback
	}

	// Use proxy original bytes header - do not recalculate
	var originalBytes int64
	proxyOriginalBytesStr := r.Header.Get("X-Proxy-Original-Bytes")
	if proxyOriginalBytesStr != "" {
		if bytes, err := strconv.ParseInt(proxyOriginalBytesStr, 10, 64); err == nil {
			originalBytes = bytes
			log.Debugf("Using proxy original bytes: %d", originalBytes)
		} else {
			log.Warnf("Invalid X-Proxy-Original-Bytes header: %s", proxyOriginalBytesStr)
			originalBytes = int64(len(body)) // Fallback
		}
	} else {
		log.Warnf("No X-Proxy-Original-Bytes header found")
		originalBytes = int64(len(body)) // Fallback
	}

	// Use proxy timestamp header
	var createdAt time.Time
	proxyCreatedAtStr := r.Header.Get("X-Proxy-Created-At")
	if proxyCreatedAtStr != "" {
		if parsedTime, err := time.Parse(time.RFC3339, proxyCreatedAtStr); err == nil {
			createdAt = parsedTime
			log.Debugf("Using proxy created at: %s", createdAt.Format(time.RFC3339))
		} else {
			log.Warnf("Invalid X-Proxy-Created-At header: %s", proxyCreatedAtStr)
			createdAt = time.Now() // Fallback
		}
	} else {
		log.Warnf("No X-Proxy-Created-At header found")
		createdAt = time.Now() // Fallback
	}

	// Use proxy data hint header
	dataHint := r.Header.Get("X-Proxy-Data-Hint")
	if dataHint == "" {
		log.Warnf("No X-Proxy-Data-Hint header found")
		dataHint = "unknown"
	} else {
		log.Debugf("Using proxy data hint: %s", dataHint)
	}

	// Use proxy batch ID header
	batchID := r.Header.Get("X-Proxy-Batch-ID")
	if batchID == "" {
		log.Warnf("No X-Proxy-Batch-ID header found")
		batchID = "unknown"
	} else {
		log.Debugf("Using proxy batch ID: %s", batchID)
	}

	// Check if the received data is actually gzip compressed (magic bytes check)
	var processedBody []byte
	isActuallyGzipCompressed := len(body) >= 2 && body[0] == 0x1f && body[1] == 0x8b

	if isActuallyGzipCompressed {
		// Data is genuinely gzip compressed, store as-is
		processedBody = body
		log.Debugf("Storing gzip compressed data: %s/%s (%d bytes, magic: %x %x)",
			tenantID, datasetID, len(body), body[0], body[1])
	} else {
		// Data is not gzip compressed (even if HTTP header said it was), compress it
		var err error
		processedBody, err = utils.CompressData(body)
		if err != nil {
			log.Errorf("Failed to compress data for %s/%s: %v", tenantID, datasetID, err)
			http.Error(w, "Data compression failed", http.StatusInternalServerError)
			return
		}
		log.Debugf("Compressed uncompressed data for storage: %s/%s (%d -> %d bytes, was HTTP compressed: %v)",
			tenantID, datasetID, len(body), len(processedBody), isAlreadyCompressed)
	}

	// Check if S3 destination client is configured
	if ws.config.S3DestinationClient == nil {
		log.Error("S3 destination client not configured")
		http.Error(w, "Storage not configured", http.StatusInternalServerError)
		return
	}

	// Cache capacity check removed - using spooling service instead

	// Store in queue directory (proxy-like architecture)
	// ONLY accept filename from proxy - no fallbacks or generation
	proxyFilename := r.Header.Get("X-Proxy-Filename")

	// Debug: Log exact filename received
	log.Infof("FILENAME DEBUG: tenant=%s, dataset=%s, X-Proxy-Filename='%s'",
		tenantID, datasetID, proxyFilename)

	if proxyFilename == "" {
		log.Errorf("No X-Proxy-Filename header provided")
		http.Error(w, "X-Proxy-Filename header is required", http.StatusBadRequest)
		return
	}

	// Validate proxy filename matches URL tenant/dataset
	// Expected format: tenant--dataset--timestamp--extension.gz
	if strings.Contains(proxyFilename, "--") {
		parts := strings.Split(proxyFilename, "--")
		if len(parts) >= 4 {
			filenameTenant := parts[0]
			filenameDataset := parts[1]
			// parts[2] is timestamp
			// parts[3] is extension (before .gz)

			// Verify filename matches URL parameters
			if filenameTenant != tenantID || filenameDataset != datasetID {
				log.Errorf("Filename mismatch: URL=%s/%s, Filename=%s/%s",
					tenantID, datasetID, filenameTenant, filenameDataset)
				http.Error(w, "Filename does not match URL tenant/dataset", http.StatusBadRequest)
				return
			}
		} else {
			log.Warnf("Filename format not recognized (expected tenant--dataset--timestamp--extension.gz): %s", proxyFilename)
		}
	}

	// Check for malformed filenames with missing extensions (double dots)
	filename := proxyFilename
	if strings.Contains(filename, "..") {
		log.Errorf("MALFORMED FILENAME DETECTED: %s", filename)
		log.Errorf("Headers: X-Proxy-Filename='%s'", proxyFilename)

		// Store in malformed_proxy directory for investigation
		malformedDir := fmt.Sprintf("%s/malformed_proxy", ws.config.Bytefreezer.SpoolPath)
		if err := os.MkdirAll(malformedDir, 0750); err != nil {
			log.Errorf("Failed to create malformed directory: %v", err)
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		}

		// Use timestamp for unique malformed filename
		malformedFilename := fmt.Sprintf("malformed_%d_%s", time.Now().UnixNano(), filename)
		malformedPath := fmt.Sprintf("%s/%s", malformedDir, malformedFilename)

		// Store the malformed file for investigation
		if err := os.WriteFile(malformedPath, processedBody, 0600); err != nil {
			log.Errorf("Failed to write malformed file: %v", err)
		}

		log.Errorf("Malformed file stored at: %s", malformedPath)
		http.Error(w, "Malformed filename detected - file stored for investigation", http.StatusBadRequest)
		return
	}
	log.Debugf("Using proxy filename: %s", filename)

	// Create queue directory path: /tenant/dataset/queue/
	queueDir := fmt.Sprintf("%s/%s/%s/queue", ws.config.Bytefreezer.SpoolPath, tenantID, datasetID)
	if err := os.MkdirAll(queueDir, 0750); err != nil {
		log.Errorf("Failed to create queue directory %s: %v", queueDir, err)
		http.Error(w, "Failed to create storage directory", http.StatusInternalServerError)
		return
	}

	// Write compressed file to queue directory
	dataFilePath := fmt.Sprintf("%s/%s", queueDir, filename)
	if err := os.WriteFile(dataFilePath, processedBody, 0600); err != nil {
		log.Errorf("Failed to write queue file %s: %v", dataFilePath, err)
		http.Error(w, "Failed to store data", http.StatusInternalServerError)
		return
	}

	// Store proxy metadata alongside the queue file
	metadataFilePath := fmt.Sprintf("%s/%s.meta", queueDir, filename)
	proxyMetadata := map[string]interface{}{
		"line_count":     originalLineCount,
		"original_bytes": originalBytes,
		"created_at":     createdAt.Format(time.RFC3339),
		"data_hint":      dataHint,
		"batch_id":       batchID,
		"filename":       filename,
	}
	metadataJSON, _ := sonic.Marshal(proxyMetadata)
	if err := os.WriteFile(metadataFilePath, metadataJSON, 0600); err != nil {
		log.Warnf("Failed to write metadata file %s: %v", metadataFilePath, err)
		// Don't fail the request if metadata write fails
	}

	// Calculate processing time
	processingTime := time.Since(processingStartTime)

	// Use complete filename as file ID for response
	fileID := filename

	log.Infof("Stored data to queue: tenant=%s, dataset=%s, file=%s, original=%d bytes, compressed=%d bytes, lines=%d",
		tenantID, datasetID, filename, originalBytes, len(processedBody), originalLineCount)

	// Record dataset metrics asynchronously (non-blocking)
	if ws.config.DatasetMetricsClient != nil {
		if metricsClient, ok := ws.config.DatasetMetricsClient.(interface {
			RecordMetric(ctx context.Context, tenantID, datasetID string,
				inputBytes, outputBytes, linesProcessed, errorCount int64,
				customMetrics map[string]interface{}) error
		}); ok {
			go func() {
				// Use background context for async metrics recording
				ctx := context.Background()
				if err := metricsClient.RecordMetric(ctx, tenantID, datasetID,
					int64(originalBytes), int64(len(processedBody)), int64(originalLineCount), 0, nil); err != nil {
					log.Debugf("Dataset metrics recording completed for %s/%s", tenantID, datasetID)
				}
			}()
		}
	}

	// Record receiver throughput asynchronously (non-blocking)
	if ws.config.ControlService.Enabled {
		go ws.recordThroughput("", tenantID, datasetID, originalBytes, originalLineCount, int64(len(processedBody)), true)
	}

	// Return success response
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	response := map[string]interface{}{
		"status":             "success",
		"tenant_id":          tenantID,
		"dataset_id":         datasetID,
		"bytes_received":     originalBytes,
		"bytes_processed":    len(processedBody),
		"storage_mode":       "queue",
		"queue_file":         filename,
		"file_id":            fileID,
		"lines_original":     originalLineCount,
		"lines_processed":    originalLineCount,
		"processing_time_ms": processingTime.Milliseconds(),
		"format_detected":    dataHint,
		"content_type":       contentType,
		"batch_id":           batchID,
		"created_at":         createdAt.Format(time.RFC3339),
		"message":            "Data stored in queue for batch processing",
	}

	// Always from proxy now
	response["filename_source"] = "proxy"

	responseJSON, _ := sonic.Marshal(response)
	w.Write(responseJSON) // #nosec G104 - response writing in HTTP handler
}

// handleIndividualUpload function removed - using spool-based file storage instead

// recordThroughput reports throughput metrics to control service
func (ws *WebhookServer) recordThroughput(accountID, tenantID, datasetID string, bytesReceived, linesReceived, bytesStored int64, success bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	url := ws.config.ControlService.BaseURL + "/api/v1/activity/receiver/throughput"

	payload := map[string]interface{}{
		"account_id":     accountID,
		"tenant_id":      tenantID,
		"dataset_id":     datasetID,
		"bytes_received": bytesReceived,
		"lines_received": linesReceived,
		"bytes_stored":   bytesStored,
		"success":        success,
	}

	jsonData, err := sonic.Marshal(payload)
	if err != nil {
		log.Debugf("Failed to marshal throughput data: %v", err)
		return
	}

	req, err := http.NewRequestWithContext(ctx, "POST", url, strings.NewReader(string(jsonData)))
	if err != nil {
		log.Debugf("Failed to create throughput request: %v", err)
		return
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+ws.config.ControlService.APIKey)

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		log.Debugf("Failed to record throughput: %v", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		log.Debugf("Throughput recording failed with status: %d", resp.StatusCode)
		return
	}

	log.Debugf("Recorded throughput for %s/%s: %d bytes, %d lines", tenantID, datasetID, bytesReceived, linesReceived)
}

// Note: Format detection and text parsing functions removed - raw data storage only
