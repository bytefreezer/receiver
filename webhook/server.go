// Licensed under Elastic License 2.0
// See LICENSE.txt for details

package webhook

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/bytefreezer/goodies/log"
	"github.com/bytefreezer/receiver/config"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

type WebhookServer struct {
	config             *config.Config
	httpServer         *http.Server
	listener           net.Listener
	securityMiddleware *SecurityMiddleware
}

func NewWebhookServer(conf *config.Config) *WebhookServer {
	return &WebhookServer{
		config: conf,
	}
}

// Batch accumulator removed - using spool-based file storage

// Start starts the webhook server
func (ws *WebhookServer) Start() {
	r := chi.NewRouter()

	// Add middleware
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)
	r.Use(middleware.Timeout(60 * time.Second))

	// Health check endpoint
	r.Get("/health", ws.healthHandler)

	// Initialize security middleware
	ws.securityMiddleware = NewSecurityMiddleware(ws.config)

	// Apply security middleware to webhook endpoints
	// Support both /webhook/{tenant}/{dataset} (legacy) and /{tenant}/{dataset} (proxy format)
	r.Route("/webhook", func(r chi.Router) {
		r.Use(ws.securityMiddleware.BearerTokenAuth)
		r.Use(ws.securityMiddleware.DatasetValidationMiddleware)
		r.Post("/{tenantId}/{datasetId}", ws.receiveDataHandler)
	})

	// Direct tenant/dataset routing for proxy integration (new)
	r.With(ws.securityMiddleware.BearerTokenAuth).
		With(ws.securityMiddleware.DatasetValidationMiddleware).
		Post("/{tenantId}/{datasetId}", ws.receiveDataHandler)

	address := fmt.Sprintf("0.0.0.0:%d", ws.config.Protocols.Webhook.Port)
	log.Infof("webhook server starting on %s (tcp4)", address)

	// Explicitly use tcp4 to ensure IPv4 binding
	var err error
	ws.listener, err = net.Listen("tcp4", address)
	if err != nil {
		log.Errorf("failed to create tcp4 listener: %v", err)
		return
	}
	log.Infof("webhook listener created successfully on %s", ws.listener.Addr().String())

	ws.httpServer = &http.Server{
		Handler:        r,
		ReadTimeout:    30 * time.Second,
		WriteTimeout:   30 * time.Second,
		IdleTimeout:    120 * time.Second,
		MaxHeaderBytes: 10 << 20, // 10MB
	}

	log.Infof("starting http server on webhook listener")
	if err := ws.httpServer.Serve(ws.listener); err != nil && err != http.ErrServerClosed {
		log.Errorf("webhook server failed: %v", err)
	}
	log.Info("webhook Serve() returned")
}

// Stop stops the webhook server
func (ws *WebhookServer) Stop() {
	if ws.httpServer == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := ws.httpServer.Shutdown(ctx); err != nil {
		log.Errorf("webhook server shutdown error: %v", err)
	} else {
		log.Info("webhook server shut down gracefully")
	}
}
