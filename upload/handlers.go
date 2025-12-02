// Licensed under Elastic License 2.0
// See LICENSE.txt for details

package upload

import (
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"github.com/bytedance/sonic"
	"github.com/bytefreezer/goodies/log"
	"github.com/bytefreezer/receiver/config"
	"github.com/bytefreezer/receiver/domain"
	"github.com/bytefreezer/receiver/services"
	"github.com/bytefreezer/receiver/utils"
	"github.com/go-chi/chi/v5"
)

// UploadServer handles file upload ingestion
type UploadServer struct {
	config *config.Config
}

// NewUploadServer creates a new upload server instance
func NewUploadServer(cfg *config.Config) *UploadServer {
	return &UploadServer{
		config: cfg,
	}
}

// RegisterRoutes registers upload routes with the router
func (us *UploadServer) RegisterRoutes(r chi.Router) {
	if !us.config.Protocols.Upload.Enabled {
		log.Info("File upload endpoint is disabled in configuration")
		return
	}

	r.Post("/upload/{tenantId}/{datasetId}", us.handleFileUpload)
	log.Info("File upload endpoints registered")
}

// handleFileUpload processes file uploads
func (us *UploadServer) handleFileUpload(w http.ResponseWriter, r *http.Request) {
	tenantID := chi.URLParam(r, "tenantId")
	datasetID := chi.URLParam(r, "datasetId")

	log.Debugf("Received file upload request for tenant=%s, dataset=%s", tenantID, datasetID)

	// Check if upload is enabled
	if !us.config.Protocols.Upload.Enabled {
		http.Error(w, "File upload is disabled", http.StatusServiceUnavailable)
		return
	}

	// Get tenant and dataset from context (set by middleware)
	tenant, ok := r.Context().Value("tenant").(*domain.Tenant)
	if !ok || tenant == nil {
		log.Errorf("Tenant not found in context")
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	dataset, ok := r.Context().Value("dataset").(*domain.Dataset)
	if !ok || dataset == nil {
		log.Errorf("Dataset not found in context")
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	// Parse multipart form
	err := r.ParseMultipartForm(int64(us.config.Protocols.Upload.MaxFileSize))
	if err != nil {
		log.Errorf("Failed to parse multipart form: %v", err)
		http.Error(w, "Invalid multipart form", http.StatusBadRequest)
		return
	}
	defer r.MultipartForm.RemoveAll()

	// Get uploaded file
	file, header, err := r.FormFile("file")
	if err != nil {
		log.Errorf("Failed to get uploaded file: %v", err)
		http.Error(w, "No file provided", http.StatusBadRequest)
		return
	}
	defer file.Close()

	// Validate file extension
	filename := header.Filename
	ext := strings.ToLower(filepath.Ext(filename))
	if !us.isAllowedExtension(ext) {
		log.Warnf("Disallowed file extension %s for file %s", ext, filename)
		http.Error(w, "File extension not allowed", http.StatusBadRequest)
		return
	}

	// Check file size
	if header.Size > int64(us.config.Protocols.Upload.MaxFileSize) {
		log.Warnf("File too large: %d > %d bytes", header.Size, us.config.Protocols.Upload.MaxFileSize)
		http.Error(w, "File too large", http.StatusRequestEntityTooLarge)
		return
	}

	log.Debugf("Processing uploaded file %s (%d bytes)", filename, header.Size)

	// Read file content
	content, err := io.ReadAll(file)
	if err != nil {
		log.Errorf("Failed to read uploaded file: %v", err)
		http.Error(w, "Failed to read file", http.StatusInternalServerError)
		return
	}

	// Track processing start time
	processingStartTime := time.Now()

	// Raw data upload - no format detection
	contentType := header.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "application/octet-stream" // Default for raw data
	}

	log.Debugf("Processing raw file upload %s (Content-Type: %s, Size: %d bytes)", filename, contentType, len(content))

	// Simple line count estimation (count newlines)
	lineCount := int64(1) // Default to 1 if no newlines
	if len(content) > 0 {
		lineCount = int64(strings.Count(string(content), "\n"))
		if lineCount == 0 {
			lineCount = 1 // At least one line if there's data
		}
	}

	// Compress file content
	compressedData, err := utils.CompressData(content)
	if err != nil {
		log.Errorf("Failed to compress uploaded file %s: %v", filename, err)
		http.Error(w, "Data compression failed", http.StatusInternalServerError)
		return
	}

	// Check if S3 destination client is configured
	if us.config.S3DestinationClient == nil {
		log.Error("S3 destination client not configured")
		http.Error(w, "Storage not configured", http.StatusInternalServerError)
		return
	}

	// Use original filename as-is - no renaming
	s3Filename := filename

	// Generate S3 key - simplified structure for transient files
	s3Key := fmt.Sprintf("%s/%s/%s",
		tenantID,
		datasetID,
		s3Filename)

	// Submit to upload pool
	uploadPool, ok := us.config.UploadWorkerPool.(*services.UploadWorkerPool)
	if !ok || uploadPool == nil {
		log.Error("Upload worker pool not configured")
		http.Error(w, "Upload service not configured", http.StatusInternalServerError)
		return
	}

	// Calculate processing time
	processingTime := time.Since(processingStartTime)

	// Create upload job with file metadata
	uploadJob := uploadPool.CreateUploadJob(tenant, dataset, compressedData, s3Key, lineCount, int64(len(content)), processingTime)

	// Submit job to pool
	if err := uploadPool.SubmitJob(uploadJob); err != nil {
		log.Errorf("Failed to submit upload job: %v", err)
		http.Error(w, "Upload queue full, try again later", http.StatusServiceUnavailable)
		return
	}

	// Wait for upload result (with timeout)
	select {
	case result := <-uploadJob.ResultChan:
		if result.Success {
			log.Infof("Successfully processed uploaded file %s for tenant=%s, dataset=%s, original=%d bytes, processed=%d bytes, lines=%d, location=%s (cached: %v)",
				filename, tenantID, datasetID, len(content), len(compressedData), lineCount, result.Location, result.CachedLocally)

			// Return success response
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)

			response := map[string]interface{}{
				"status":             "success",
				"tenant_id":          tenantID,
				"dataset_id":         datasetID,
				"filename":           filename,
				"bytes_received":     len(content),
				"bytes_processed":    len(compressedData),
				"upload_id":          uploadJob.ID,
				"lines_processed":    lineCount,
				"processing_time_ms": processingTime.Milliseconds(),
				"format_detected":    "raw",
				"content_type":       contentType,
				"s3_location":        result.Location,
				"cached":             result.CachedLocally,
			}

			responseJSON, _ := sonic.Marshal(response)
			w.Write(responseJSON) // #nosec G104 - upload response
		} else {
			log.Errorf("Failed to process upload job for file %s: %v", filename, result.Error)
			http.Error(w, "Failed to store file", http.StatusInternalServerError)
			return
		}
	case <-time.After(60 * time.Second): // Longer timeout for file uploads
		log.Warnf("Upload job %s timed out for file %s, tenant=%s, dataset=%s", uploadJob.ID, filename, tenantID, datasetID)
		http.Error(w, "Upload timeout", http.StatusRequestTimeout)
		return
	}
}

// isAllowedExtension checks if the file extension is allowed
// For raw data mode, accept any file type
func (us *UploadServer) isAllowedExtension(ext string) bool {
	// In raw data mode, accept any file extension
	return true
}

// Note: Line counting and format-specific functions removed - raw data only
