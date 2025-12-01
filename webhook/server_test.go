package webhook

import (
	"bytes"
	"compress/gzip"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/bytefreezer/receiver/config"
	"github.com/bytefreezer/receiver/domain"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

// Test server creation and routing
func TestWebhookServerRouting(t *testing.T) {
	// Test that the server sets up routes correctly

	// Create a test server with similar setup to the real server
	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)
	r.Use(middleware.Timeout(60 * time.Second))

	// Add health endpoint
	r.Get("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status": "ok", "service": "webhook"}`))
	})

	testServer := httptest.NewServer(r)
	defer testServer.Close()

	// Test health endpoint
	resp, err := http.Get(testServer.URL + "/health")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200, got %d", resp.StatusCode)
	}
}

// Test webhook endpoint behavior without full middleware stack
func TestReceiveDataHandlerBasics(t *testing.T) {
	conf := &config.Config{
		Protocols: config.Protocols{
			Webhook: config.WebhookProtocol{
				MaxPayloadSize: 100, // Small limit for testing
			},
		},
	}
	server := NewWebhookServer(conf)

	// Test case 1: Missing tenant in context
	t.Run("MissingTenant", func(t *testing.T) {
		req := httptest.NewRequest("POST", "/webhook/tenant1/dataset1", strings.NewReader(`{"test": "data"}`))
		req.Header.Set("Content-Type", "application/x-ndjson")

		// Add URL params but no tenant context
		rctx := chi.NewRouteContext()
		rctx.URLParams.Add("tenantId", "tenant1")
		rctx.URLParams.Add("datasetId", "dataset1")
		req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

		rr := httptest.NewRecorder()
		server.receiveDataHandler(rr, req)

		if rr.Code != http.StatusInternalServerError {
			t.Errorf("Expected status 500, got %d", rr.Code)
		}
	})

	// Test case 2: Missing dataset in context
	t.Run("MissingDataset", func(t *testing.T) {
		tenant := &domain.Tenant{ID: "tenant1", Name: "Test Tenant"}

		req := httptest.NewRequest("POST", "/webhook/tenant1/dataset1", strings.NewReader(`{"test": "data"}`))
		req.Header.Set("Content-Type", "application/x-ndjson")

		// Add tenant but not dataset
		ctx := context.WithValue(req.Context(), tenantContextKey, tenant)
		req = req.WithContext(ctx)

		rctx := chi.NewRouteContext()
		rctx.URLParams.Add("tenantId", "tenant1")
		rctx.URLParams.Add("datasetId", "dataset1")
		req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

		rr := httptest.NewRecorder()
		server.receiveDataHandler(rr, req)

		if rr.Code != http.StatusInternalServerError {
			t.Errorf("Expected status 500, got %d", rr.Code)
		}
	})

	// Test case 3: Payload size validation by Content-Length
	t.Run("PayloadTooLarge", func(t *testing.T) {
		tenant := &domain.Tenant{ID: "tenant1", Name: "Test Tenant"}
		dataset := &domain.Dataset{ID: "dataset1", Name: "Test Dataset", TenantID: "tenant1"}

		largeData := strings.Repeat("a", 200) // Larger than MaxPayloadSize
		req := httptest.NewRequest("POST", "/webhook/tenant1/dataset1", strings.NewReader(largeData))
		req.Header.Set("Content-Type", "application/x-ndjson")
		req.ContentLength = int64(len(largeData))

		ctx := context.WithValue(req.Context(), tenantContextKey, tenant)
		ctx = context.WithValue(ctx, datasetContextKey, dataset)
		req = req.WithContext(ctx)

		rctx := chi.NewRouteContext()
		rctx.URLParams.Add("tenantId", "tenant1")
		rctx.URLParams.Add("datasetId", "dataset1")
		req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

		rr := httptest.NewRecorder()
		server.receiveDataHandler(rr, req)

		if rr.Code != http.StatusRequestEntityTooLarge {
			t.Errorf("Expected status 413, got %d", rr.Code)
		}
	})

	// Test case 4: Empty body
	t.Run("EmptyBody", func(t *testing.T) {
		tenant := &domain.Tenant{ID: "tenant1", Name: "Test Tenant"}
		dataset := &domain.Dataset{ID: "dataset1", Name: "Test Dataset", TenantID: "tenant1"}

		req := httptest.NewRequest("POST", "/webhook/tenant1/dataset1", strings.NewReader(""))
		req.Header.Set("Content-Type", "application/x-ndjson")

		ctx := context.WithValue(req.Context(), tenantContextKey, tenant)
		ctx = context.WithValue(ctx, datasetContextKey, dataset)
		req = req.WithContext(ctx)

		rctx := chi.NewRouteContext()
		rctx.URLParams.Add("tenantId", "tenant1")
		rctx.URLParams.Add("datasetId", "dataset1")
		req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

		rr := httptest.NewRecorder()
		server.receiveDataHandler(rr, req)

		if rr.Code != http.StatusBadRequest {
			t.Errorf("Expected status 400, got %d", rr.Code)
		}
	})
}

// Test gzip handling
func TestGzipHandling(t *testing.T) {
	conf := &config.Config{
		Protocols: config.Protocols{
			Webhook: config.WebhookProtocol{
				MaxPayloadSize: 1024,
			},
		},
	}
	server := NewWebhookServer(conf)

	tenant := &domain.Tenant{ID: "tenant1", Name: "Test Tenant"}
	dataset := &domain.Dataset{ID: "dataset1", Name: "Test Dataset", TenantID: "tenant1"}

	t.Run("ValidGzip", func(t *testing.T) {
		originalData := `{"event": "test", "data": "compressed"}`

		// Compress the data
		var buf bytes.Buffer
		gzipWriter := gzip.NewWriter(&buf)
		gzipWriter.Write([]byte(originalData))
		gzipWriter.Close()

		req := httptest.NewRequest("POST", "/webhook/tenant1/dataset1", &buf)
		req.Header.Set("Content-Type", "application/x-ndjson")
		// No Content-Encoding header - the payload itself is gzipped

		ctx := context.WithValue(req.Context(), tenantContextKey, tenant)
		ctx = context.WithValue(ctx, datasetContextKey, dataset)
		req = req.WithContext(ctx)

		rctx := chi.NewRouteContext()
		rctx.URLParams.Add("tenantId", "tenant1")
		rctx.URLParams.Add("datasetId", "dataset1")
		req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

		rr := httptest.NewRecorder()
		server.receiveDataHandler(rr, req)

		// Should fail later due to missing S3 client, but gzip processing should work
		// We expect either 500 (missing S3 client) or other later-stage errors
		if rr.Code == http.StatusBadRequest {
			t.Errorf("Gzip processing failed with 400 - should have processed gzip correctly")
		}
	})

	t.Run("InvalidGzip", func(t *testing.T) {
		invalidGzipData := "not gzip data"

		req := httptest.NewRequest("POST", "/webhook/tenant1/dataset1", strings.NewReader(invalidGzipData))
		req.Header.Set("Content-Type", "application/x-ndjson")
		req.Header.Set("Content-Encoding", "gzip")

		ctx := context.WithValue(req.Context(), tenantContextKey, tenant)
		ctx = context.WithValue(ctx, datasetContextKey, dataset)
		req = req.WithContext(ctx)

		rctx := chi.NewRouteContext()
		rctx.URLParams.Add("tenantId", "tenant1")
		rctx.URLParams.Add("datasetId", "dataset1")
		req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

		rr := httptest.NewRecorder()
		server.receiveDataHandler(rr, req)

		// With new logic, "invalid gzip" with Content-Encoding header is treated as raw compressed data
		// It should fail later due to missing S3 client (500), not gzip validation (400)
		if rr.Code != http.StatusInternalServerError {
			t.Errorf("Expected status 500 for missing S3 client, got %d", rr.Code)
		}
	})
}

// Test middleware creation and basic functionality
func TestMiddlewareBasics(t *testing.T) {
	conf := &config.Config{}

	t.Run("CreateSecurityMiddleware", func(t *testing.T) {
		middleware := NewSecurityMiddleware(conf)

		if middleware == nil {
			t.Error("Expected middleware to be created")
			return
		}

		if middleware.config != conf {
			t.Error("Expected middleware config to match")
		}
	})
}

// Test URL parameter extraction
func TestURLParameterHandling(t *testing.T) {
	// Test various URL parameter scenarios that the middleware needs to handle

	testCases := []struct {
		name       string
		url        string
		tenantID   string
		datasetID  string
		shouldPass bool
	}{
		{
			name:       "ValidParams",
			url:        "/webhook/tenant1/dataset1",
			tenantID:   "tenant1",
			datasetID:  "dataset1",
			shouldPass: true,
		},
		{
			name:       "EmptyTenant",
			url:        "/webhook//dataset1",
			tenantID:   "",
			datasetID:  "dataset1",
			shouldPass: false,
		},
		{
			name:       "EmptyDataset",
			url:        "/webhook/tenant1/",
			tenantID:   "tenant1",
			datasetID:  "",
			shouldPass: false,
		},
		{
			name:       "SpecialCharacters",
			url:        "/webhook/tenant-1/dataset_1",
			tenantID:   "tenant-1",
			datasetID:  "dataset_1",
			shouldPass: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create a simple handler that checks URL params
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				tenantID := chi.URLParam(r, "tenantId")
				datasetID := chi.URLParam(r, "datasetId")

				if tenantID != tc.tenantID {
					t.Errorf("Expected tenantID %s, got %s", tc.tenantID, tenantID)
				}

				if datasetID != tc.datasetID {
					t.Errorf("Expected datasetID %s, got %s", tc.datasetID, datasetID)
				}

				if tc.shouldPass && (tenantID == "" || datasetID == "") {
					t.Errorf("Expected non-empty parameters but got tenantID='%s', datasetID='%s'", tenantID, datasetID)
				}

				w.WriteHeader(http.StatusOK)
			})

			// Set up router with the same pattern as the real server
			r := chi.NewRouter()
			r.Post("/{tenantId}/{datasetId}", handler)

			req := httptest.NewRequest("POST", tc.url, nil)
			rr := httptest.NewRecorder()

			r.ServeHTTP(rr, req)

			// The router itself should handle the URL parsing
			// We're mainly testing that our pattern works
		})
	}
}
