package api

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"time"

	"github.com/bytefreezer/receiver/config"
	"github.com/bytefreezer/receiver/services"
	"github.com/bytefreezer/goodies/log"
	"github.com/swaggest/openapi-go/openapi3"
	"github.com/swaggest/rest/web"
	swgui "github.com/swaggest/swgui/v5emb"
	"go.opentelemetry.io/otel/metric"
)

type API struct {
	Services   *services.Services
	ApiMetrics map[string]metric.Int64Counter
	HttpServer *http.Server
	sync.RWMutex
	Config *config.Config
}

// new api
func NewAPI(services *services.Services, conf *config.Config) *API {

	//here we set map of metrics
	//for now counter only
	//each function can have its own metric
	//each metric can have its own label and is created and kept as a global map of counters
	//this is not the best way to do it but it is the simplest way to do it

	return &API{
		Services:   services,
		ApiMetrics: make(map[string]metric.Int64Counter),
		Config:     conf,
	}
}

// NewRouter returns a new router serving API endpoints
func (api *API) NewRouter() *web.Service {
	service := web.NewService(openapi3.NewReflector())

	// Configure OpenAPI schema
	service.OpenAPISchema().SetTitle("ByteFreezer Receiver API")
	service.OpenAPISchema().SetDescription("ByteFreezer Receiver API for data ingestion and tenant management")
	service.OpenAPISchema().SetVersion("v2.0.0")

	// Tags will be added individually when defining endpoints
	// The new API requires tags to be set on individual use cases rather than globally

	// Apply defaults for decoder factory
	service.DecoderFactory.ApplyDefaults = true

	// Wrap to finalize middleware setup
	service.Wrap()

	// Health check endpoint
	service.Get("/api/v1/health", api.HealthCheck())

	// Tenant endpoints
	service.Get("/api/v1/tenants", api.GetTenants())
	service.Get("/api/v1/tenants/count", api.GetTenantCount())
	service.Get("/api/v1/tenants/{tenantId}", api.GetTenant())


	// Configuration endpoints
	service.Get("/api/v1/config", api.GetConfig())

	// DLQ management endpoints
	service.Get("/api/v1/dlq/stats", GetDLQStats())
	service.Get("/api/v1/dlq/files", ListDLQFiles())
	service.Post("/api/v1/dlq/retry", RetryDLQFiles())

	// API documentation
	service.Docs("/v2/docs", swgui.New)

	// Root redirect to documentation
	service.Router.Get("/", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/v2/docs", http.StatusFound)
	})

	return service
}

// Serve serves http endpoints
func (api *API) Serve(address string, router http.Handler) {
	log.Infof("api server started: on %s", address)

	api.HttpServer = &http.Server{
		Addr:           address,
		Handler:        router,
		ReadTimeout:    30 * time.Second,
		WriteTimeout:   30 * time.Second,
		IdleTimeout:    120 * time.Second,
		MaxHeaderBytes: 1 << 20, // 1MB
	}
	err := api.HttpServer.ListenAndServe()
	if errors.Is(err, http.ErrServerClosed) {
		log.Info("api server closed")
	} else {
		log.Errorf("api server failed and closed: %v", err)
	}
}

// Stop stops the server
func (api *API) Stop() {

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer func() {
		api.HttpServer = nil
		cancel()
	}()

	if api.HttpServer != nil {
		if err := api.HttpServer.Shutdown(ctx); err != nil {
			log.Errorf("error shutting down api server: %v", err)
		}
	}

	log.Info("API server shut down gracefully")
}
