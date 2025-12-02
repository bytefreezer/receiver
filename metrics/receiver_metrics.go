// Licensed under Elastic License 2.0
// See LICENSE.txt for details

package metrics

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// ReceiverMetrics provides comprehensive OTEL metrics for ByteFreezer receiver
// Focused on bytes/lines per tenant/dataset tracking as requested
type ReceiverMetrics struct {
	// Core data ingestion metrics
	bytesReceived  metric.Int64Counter
	bytesProcessed metric.Int64Counter
	linesReceived  metric.Int64Counter
	linesProcessed metric.Int64Counter
	requestsTotal  metric.Int64Counter

	// System performance metrics
	processingDuration metric.Float64Histogram
	uploadDuration     metric.Float64Histogram
	cacheHits          metric.Int64Counter

	// Error tracking
	uploadFailures metric.Int64Counter
	authFailures   metric.Int64Counter
}

// NewReceiverMetrics creates a new ReceiverMetrics instance with all required counters
func NewReceiverMetrics() (*ReceiverMetrics, error) {
	meter := otel.Meter("bytefreezer-receiver")

	bytesReceived, err := meter.Int64Counter(
		"bytefreezer_bytes_received_total",
		metric.WithDescription("Total bytes received per tenant/dataset"),
		metric.WithUnit("bytes"),
	)
	if err != nil {
		return nil, err
	}

	bytesProcessed, err := meter.Int64Counter(
		"bytefreezer_bytes_processed_total",
		metric.WithDescription("Total bytes processed per tenant/dataset"),
		metric.WithUnit("bytes"),
	)
	if err != nil {
		return nil, err
	}

	linesReceived, err := meter.Int64Counter(
		"bytefreezer_lines_received_total",
		metric.WithDescription("Total NDJSON lines received per tenant/dataset"),
		metric.WithUnit("1"),
	)
	if err != nil {
		return nil, err
	}

	linesProcessed, err := meter.Int64Counter(
		"bytefreezer_lines_processed_total",
		metric.WithDescription("Total NDJSON lines ingested per tenant/dataset"),
		metric.WithUnit("1"),
	)
	if err != nil {
		return nil, err
	}

	requestsTotal, err := meter.Int64Counter(
		"bytefreezer_requests_total",
		metric.WithDescription("Total webhook requests per tenant/dataset"),
		metric.WithUnit("1"),
	)
	if err != nil {
		return nil, err
	}

	processingDuration, err := meter.Float64Histogram(
		"bytefreezer_processing_duration_seconds",
		metric.WithDescription("Processing duration per tenant/dataset"),
		metric.WithUnit("s"),
	)
	if err != nil {
		return nil, err
	}

	uploadDuration, err := meter.Float64Histogram(
		"bytefreezer_upload_duration_seconds",
		metric.WithDescription("Upload duration per tenant/dataset"),
		metric.WithUnit("s"),
	)
	if err != nil {
		return nil, err
	}

	cacheHits, err := meter.Int64Counter(
		"bytefreezer_cache_hits_total",
		metric.WithDescription("Total cache hits (local storage) per tenant/dataset"),
		metric.WithUnit("1"),
	)
	if err != nil {
		return nil, err
	}

	uploadFailures, err := meter.Int64Counter(
		"bytefreezer_upload_failures_total",
		metric.WithDescription("Total upload failures per tenant/dataset"),
		metric.WithUnit("1"),
	)
	if err != nil {
		return nil, err
	}

	authFailures, err := meter.Int64Counter(
		"bytefreezer_auth_failures_total",
		metric.WithDescription("Total authentication failures per tenant"),
		metric.WithUnit("1"),
	)
	if err != nil {
		return nil, err
	}

	return &ReceiverMetrics{
		bytesReceived:      bytesReceived,
		bytesProcessed:     bytesProcessed,
		linesReceived:      linesReceived,
		linesProcessed:     linesProcessed,
		requestsTotal:      requestsTotal,
		processingDuration: processingDuration,
		uploadDuration:     uploadDuration,
		cacheHits:          cacheHits,
		uploadFailures:     uploadFailures,
		authFailures:       authFailures,
	}, nil
}

// RecordDataIngestion records the core data ingestion metrics per tenant/dataset
func (m *ReceiverMetrics) RecordDataIngestion(ctx context.Context, tenantID, datasetID string,
	originalBytes, processedBytes int64, originalLines, processedLines int64,
	processingDurationSecs float64, success bool) {

	baseAttrs := []attribute.KeyValue{
		attribute.String("tenant_id", tenantID),
		attribute.String("dataset_id", datasetID),
		attribute.Bool("success", success),
	}

	// Always record request
	m.requestsTotal.Add(ctx, 1, metric.WithAttributes(baseAttrs...))

	// Record processing duration
	m.processingDuration.Record(ctx, processingDurationSecs, metric.WithAttributes(baseAttrs...))

	// Only record data metrics on success
	if success {
		// Record original data received
		m.bytesReceived.Add(ctx, originalBytes, metric.WithAttributes(baseAttrs...))
		m.linesReceived.Add(ctx, originalLines, metric.WithAttributes(baseAttrs...))

		// Record ingested data (raw data stored to S3)
		m.bytesProcessed.Add(ctx, processedBytes, metric.WithAttributes(baseAttrs...))
		m.linesProcessed.Add(ctx, processedLines, metric.WithAttributes(baseAttrs...))
	}
}

// RecordUploadMetrics records upload-specific metrics per tenant/dataset
func (m *ReceiverMetrics) RecordUploadMetrics(ctx context.Context, tenantID, datasetID string,
	uploadDurationSecs float64, cached, success bool, failureReason string) {

	baseAttrs := []attribute.KeyValue{
		attribute.String("tenant_id", tenantID),
		attribute.String("dataset_id", datasetID),
		attribute.Bool("cached", cached),
	}

	// Record upload duration
	m.uploadDuration.Record(ctx, uploadDurationSecs, metric.WithAttributes(baseAttrs...))

	if success {
		if cached {
			m.cacheHits.Add(ctx, 1, metric.WithAttributes(baseAttrs...))
		}
	} else {
		// Record failure with reason
		failureAttrs := append(baseAttrs, attribute.String("reason", failureReason))
		m.uploadFailures.Add(ctx, 1, metric.WithAttributes(failureAttrs...))
	}
}

// RecordAuthFailure records authentication failure per tenant
func (m *ReceiverMetrics) RecordAuthFailure(ctx context.Context, tenantID, reason string) {
	attrs := []attribute.KeyValue{
		attribute.String("tenant_id", tenantID),
		attribute.String("reason", reason),
	}
	m.authFailures.Add(ctx, 1, metric.WithAttributes(attrs...))
}
