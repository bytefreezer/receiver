// Licensed under Elastic License 2.0
// See LICENSE.txt for details

package main

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/bytefreezer/goodies/log"
	"github.com/bytefreezer/receiver/config"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/prometheus"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
)

// initOTEL initializes the OpenTelemetry provider with multi-mode support
// Modes:
//   - "prometheus": Expose /metrics endpoint for Prometheus to scrape (pull)
//   - "otlp_http": Push metrics to Prometheus OTLP receiver via HTTP
//   - "otlp_grpc": Push metrics to OpenTelemetry Collector via gRPC
func initOTEL(cfg *config.Config) (func(), error) {
	if !cfg.Otel.Enabled {
		log.Info("OpenTelemetry metrics disabled")
		return func() {}, nil
	}

	ctx := context.Background()

	// Determine service name
	serviceName := cfg.Otel.ServiceName
	if serviceName == "" {
		serviceName = cfg.App.Name
	}

	// Create a resource
	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName(serviceName),
			semconv.ServiceVersion(cfg.App.Version),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create resource: %w", err)
	}

	var meterProvider *sdkmetric.MeterProvider
	var httpServer *http.Server

	// Determine mode - support backwards compatibility with PrometheusMode
	mode := cfg.Otel.Mode
	if mode == "" {
		// Backwards compatibility
		if cfg.Otel.PrometheusMode {
			mode = "prometheus"
		} else if cfg.Otel.Endpoint != "" || cfg.Otel.OTLPEndpoint != "" {
			mode = "otlp_grpc" // Default to gRPC for backwards compatibility
		} else {
			mode = "prometheus"
		}
	}

	// Get OTLP endpoint - support backwards compatibility
	otlpEndpoint := cfg.Otel.OTLPEndpoint
	if otlpEndpoint == "" {
		otlpEndpoint = cfg.Otel.Endpoint // Backwards compatibility
	}

	// Get push interval - support backwards compatibility
	pushIntervalSec := cfg.Otel.PushIntervalSeconds
	if pushIntervalSec <= 0 {
		pushIntervalSec = cfg.Otel.ScrapeIntervalSeconds // Backwards compatibility
	}
	if pushIntervalSec <= 0 {
		pushIntervalSec = 15
	}
	pushInterval := time.Duration(pushIntervalSec) * time.Second

	switch mode {
	case "prometheus":
		// Prometheus pull mode - expose /metrics endpoint
		prometheusExporter, err := prometheus.New()
		if err != nil {
			return nil, fmt.Errorf("failed to create prometheus exporter: %w", err)
		}

		meterProvider = sdkmetric.NewMeterProvider(
			sdkmetric.WithResource(res),
			sdkmetric.WithReader(prometheusExporter),
		)

		// Start HTTP server for metrics endpoint
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())

		metricsHost := cfg.Otel.MetricsHost
		if metricsHost == "" {
			metricsHost = "0.0.0.0"
		}

		httpServer = &http.Server{
			Addr:              fmt.Sprintf("%s:%d", metricsHost, cfg.Otel.MetricsPort),
			Handler:           mux,
			ReadHeaderTimeout: 10 * time.Second,
			ReadTimeout:       30 * time.Second,
			WriteTimeout:      30 * time.Second,
			IdleTimeout:       120 * time.Second,
		}

		go func() {
			log.Infof("Starting Prometheus metrics server on %s", httpServer.Addr)
			if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				log.Errorf("Failed to start metrics server: %v", err)
			}
		}()

		log.Infof("OpenTelemetry initialized in Prometheus mode on %s:%d/metrics",
			metricsHost, cfg.Otel.MetricsPort)

	case "otlp_http":
		// OTLP HTTP push mode - push to Prometheus OTLP receiver
		if otlpEndpoint == "" {
			return nil, fmt.Errorf("OTLP HTTP mode requires otlp_endpoint to be set")
		}

		exporter, err := otlpmetrichttp.New(ctx,
			otlpmetrichttp.WithEndpoint(otlpEndpoint),
			otlpmetrichttp.WithInsecure(),
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create OTLP HTTP exporter: %w", err)
		}

		meterProvider = sdkmetric.NewMeterProvider(
			sdkmetric.WithResource(res),
			sdkmetric.WithReader(sdkmetric.NewPeriodicReader(
				exporter,
				sdkmetric.WithInterval(pushInterval),
			)),
		)

		log.Infof("OpenTelemetry initialized in OTLP HTTP push mode to %s (interval: %v)",
			otlpEndpoint, pushInterval)

	case "otlp_grpc":
		// OTLP gRPC push mode - push to OpenTelemetry Collector
		if otlpEndpoint == "" {
			return nil, fmt.Errorf("OTLP gRPC mode requires otlp_endpoint to be set")
		}

		exporter, err := otlpmetricgrpc.New(ctx,
			otlpmetricgrpc.WithEndpoint(otlpEndpoint),
			otlpmetricgrpc.WithInsecure(),
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create OTLP gRPC exporter: %w", err)
		}

		meterProvider = sdkmetric.NewMeterProvider(
			sdkmetric.WithResource(res),
			sdkmetric.WithReader(sdkmetric.NewPeriodicReader(
				exporter,
				sdkmetric.WithInterval(pushInterval),
			)),
		)

		log.Infof("OpenTelemetry initialized in OTLP gRPC push mode to %s (interval: %v)",
			otlpEndpoint, pushInterval)

	default:
		return nil, fmt.Errorf("unknown monitoring mode: %s (valid: prometheus, otlp_http, otlp_grpc)", mode)
	}

	// Set the global meter provider
	otel.SetMeterProvider(meterProvider)

	log.Infof("OTEL initialized with mode: %s", mode)

	// Return shutdown function
	return func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// Shutdown HTTP server if running (prometheus mode)
		if httpServer != nil {
			if err := httpServer.Shutdown(ctx); err != nil {
				log.Errorf("Failed to shutdown metrics server: %v", err)
			}
		}

		// Shutdown meter provider
		if err := meterProvider.Shutdown(ctx); err != nil {
			log.Errorf("Failed to shutdown metric provider: %v", err)
		}

		log.Info("OTEL cleanup completed")
	}, nil
}
