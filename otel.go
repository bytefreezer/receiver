package main

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/n0needt0/bytefreezer-receiver/config"
	"github.com/n0needt0/go-goodies/log"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/sdk/instrumentation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
)

func initOTEL(cfg *config.Config) (func(), error) {
	ctx := context.Background()

	// Create resource with service information
	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceNameKey.String(cfg.Otel.ServiceName),
			semconv.ServiceVersionKey.String(cfg.App.Version),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create resource: %w", err)
	}

	// Set up trace provider
	traceCleanup, err := setupTracing(ctx, cfg, res)
	if err != nil {
		return nil, fmt.Errorf("failed to setup tracing: %w", err)
	}

	// Set up metric provider
	metricCleanup, err := setupMetrics(ctx, cfg, res)
	if err != nil {
		traceCleanup()
		return nil, fmt.Errorf("failed to setup metrics: %w", err)
	}

	log.Infof("OTEL initialized with endpoint: %s", cfg.Otel.Endpoint)

	return func() {
		metricCleanup()
		traceCleanup()
		log.Info("OTEL cleanup completed")
	}, nil
}

func setupTracing(ctx context.Context, cfg *config.Config, res *resource.Resource) (func(), error) {
	// Create trace exporter
	traceExporter, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(cfg.Otel.Endpoint),
		otlptracegrpc.WithInsecure(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create trace exporter: %w", err)
	}

	// Create trace provider
	traceProvider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(traceExporter),
		sdktrace.WithResource(res),
	)

	otel.SetTracerProvider(traceProvider)

	return func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := traceProvider.Shutdown(ctx); err != nil {
			log.Errorf("Failed to shutdown trace provider: %v", err)
		}
	}, nil
}

func setupMetrics(ctx context.Context, cfg *config.Config, res *resource.Resource) (func(), error) {
	var metricProvider *sdkmetric.MeterProvider
	var httpServer *http.Server

	if cfg.Otel.PrometheusMode {
		// Prometheus HTTP metrics endpoint
		prometheusExporter, err := prometheus.New()
		if err != nil {
			return nil, fmt.Errorf("failed to create prometheus exporter: %w", err)
		}

		metricProvider = sdkmetric.NewMeterProvider(
			sdkmetric.WithReader(prometheusExporter),
			sdkmetric.WithResource(res),
		)

		// Start HTTP server for metrics endpoint
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())

		metricsHost := cfg.Otel.MetricsHost
		if metricsHost == "" {
			metricsHost = "localhost"
		}

		httpServer = &http.Server{
			Addr:              fmt.Sprintf("%s:%d", metricsHost, cfg.Otel.MetricsPort),
			Handler:           mux,
			ReadHeaderTimeout: 30 * time.Second, // Prevent Slowloris attacks
		}

		go func() {
			log.Infof("Starting Prometheus metrics server on port %d", cfg.Otel.MetricsPort)
			if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				log.Errorf("Prometheus metrics server failed: %v", err)
			}
		}()

		log.Infof("Prometheus metrics endpoint: http://localhost:%d/metrics", cfg.Otel.MetricsPort)
	} else {
		// OTLP gRPC exporter (original behavior)
		metricExporter, err := otlpmetricgrpc.New(ctx,
			otlpmetricgrpc.WithEndpoint(cfg.Otel.Endpoint),
			otlpmetricgrpc.WithInsecure(),
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create OTLP metric exporter: %w", err)
		}

		metricProvider = sdkmetric.NewMeterProvider(
			sdkmetric.WithReader(sdkmetric.NewPeriodicReader(
				metricExporter,
				sdkmetric.WithInterval(time.Duration(cfg.Otel.ScrapeIntervalSeconds)*time.Second),
			)),
			sdkmetric.WithResource(res),
			sdkmetric.WithView(
				// Add custom views if needed
				sdkmetric.NewView(
					sdkmetric.Instrument{
						Name: "bytefreezer_receiver_*",
						Scope: instrumentation.Scope{
							Name: cfg.Otel.ServiceName,
						},
					},
					sdkmetric.Stream{
						Aggregation: sdkmetric.AggregationDefault{},
					},
				),
			),
		)

		log.Infof("OTLP metrics configured for endpoint: %s", cfg.Otel.Endpoint)
	}

	otel.SetMeterProvider(metricProvider)

	return func() {
		// Shutdown HTTP server if running
		if httpServer != nil {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := httpServer.Shutdown(shutdownCtx); err != nil {
				log.Errorf("Failed to shutdown Prometheus metrics server: %v", err)
			}
		}

		// Shutdown metric provider
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := metricProvider.Shutdown(ctx); err != nil {
			log.Errorf("Failed to shutdown metric provider: %v", err)
		}
	}, nil
}

