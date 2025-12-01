package webhook

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/n0needt0/bytefreezer-receiver/config"
	"github.com/n0needt0/bytefreezer-receiver/domain"
)

func TestSimpleHealthHandler(t *testing.T) {
	ws := &WebhookServer{
		config: &config.Config{},
	}

	req := httptest.NewRequest("GET", "/health", nil)
	w := httptest.NewRecorder()

	ws.healthHandler(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200, got %d", resp.StatusCode)
	}

	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "webhook") {
		t.Error("Health response should contain 'webhook'")
	}
}

func TestSimplePayloadSizeLimit(t *testing.T) {
	// Create test configuration with small payload limit
	cfg := &config.Config{
		Protocols: config.Protocols{
			Webhook: config.WebhookProtocol{
				MaxPayloadSize: 10, // 10 bytes limit
			},
		},
	}

	ws := &WebhookServer{
		config: cfg,
	}

	// Create request with payload exceeding limit
	largePayload := strings.Repeat("x", 20) // 20 bytes
	req := httptest.NewRequest("POST", "/data/test-tenant/test-dataset", strings.NewReader(largePayload))
	req.Header.Set("Content-Type", "application/x-ndjson")

	w := httptest.NewRecorder()

	// Add tenant and dataset to context
	tenant := &domain.Tenant{ID: "test-tenant"}
	dataset := &domain.Dataset{ID: "test-dataset"}

	ctx := context.WithValue(req.Context(), tenantContextKey, tenant)
	ctx = context.WithValue(ctx, datasetContextKey, dataset)
	req = req.WithContext(ctx)

	ws.receiveDataHandler(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Errorf("Expected status 413, got %d", resp.StatusCode)
	}
}

func TestSimpleEmptyBody(t *testing.T) {
	cfg := &config.Config{
		Protocols: config.Protocols{
			Webhook: config.WebhookProtocol{
				MaxPayloadSize: 1000,
			},
		},
	}

	ws := &WebhookServer{
		config: cfg,
	}

	req := httptest.NewRequest("POST", "/data/test-tenant/test-dataset", strings.NewReader(""))
	w := httptest.NewRecorder()

	// Add tenant and dataset to context
	tenant := &domain.Tenant{ID: "test-tenant"}
	dataset := &domain.Dataset{ID: "test-dataset"}

	ctx := context.WithValue(req.Context(), tenantContextKey, tenant)
	ctx = context.WithValue(ctx, datasetContextKey, dataset)
	req = req.WithContext(ctx)

	ws.receiveDataHandler(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("Expected status 400, got %d", resp.StatusCode)
	}
}

func TestSimpleMissingContext(t *testing.T) {
	ws := &WebhookServer{
		config: &config.Config{},
	}

	req := httptest.NewRequest("POST", "/data/test-tenant/test-dataset", strings.NewReader("test data"))
	w := httptest.NewRecorder()

	ws.receiveDataHandler(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusInternalServerError {
		t.Errorf("Expected status 500, got %d", resp.StatusCode)
	}
}
