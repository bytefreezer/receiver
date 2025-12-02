// Licensed under Elastic License 2.0
// See LICENSE.txt for details

package webhook

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/bytefreezer/receiver/config"
)

// Test basic webhook functionality without complex dependencies
func TestHealthEndpoint(t *testing.T) {
	// Test the health endpoint works
	conf := &config.Config{}
	server := NewWebhookServer(conf)

	req := httptest.NewRequest("GET", "/health", nil)
	rr := httptest.NewRecorder()

	server.healthHandler(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", rr.Code)
	}

	expected := `{"status": "ok", "service": "webhook"}`
	actual := strings.TrimSpace(rr.Body.String())
	if actual != expected {
		t.Errorf("Expected body '%s', got '%s'", expected, actual)
	}
}

func TestWebhookServerCreation(t *testing.T) {
	// Test that webhook server can be created
	conf := &config.Config{
		Protocols: config.Protocols{
			Webhook: config.WebhookProtocol{
				Port:           8080,
				MaxPayloadSize: 1024 * 1024,
			},
		},
	}

	server := NewWebhookServer(conf)

	if server == nil {
		t.Error("Expected server to be created")
		return
	}

	if server.config != conf {
		t.Error("Expected server config to match provided config")
	}
}

func TestSecurityMiddlewareCreation(t *testing.T) {
	// Test that security middleware can be created
	conf := &config.Config{}

	middleware := NewSecurityMiddleware(conf)

	if middleware == nil {
		t.Error("Expected middleware to be created")
		return
	}

	if middleware.config != conf {
		t.Error("Expected middleware config to match provided config")
	}
}
