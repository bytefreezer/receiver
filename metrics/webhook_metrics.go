// Licensed under Elastic License 2.0
// See LICENSE.txt for details

package metrics

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

type WebhookMetrics struct {
	requestsCounter       metric.Int64Counter
	bytesCounter          metric.Int64Counter
	linesCounter          metric.Int64Counter
	authFailuresCounter   metric.Int64Counter
	uploadFailuresCounter metric.Int64Counter
	cachingCounter        metric.Int64Counter
}

func NewWebhookMetrics() (*WebhookMetrics, error) {
	meter := otel.Meter("bytefreezer-receiver")

	requestsCounter, err := meter.Int64Counter(
		"webhook_requests_total",
		metric.WithDescription("Total number of webhook requests"),
		metric.WithUnit("1"),
	)
	if err != nil {
		return nil, err
	}

	bytesCounter, err := meter.Int64Counter(
		"webhook_bytes_total",
		metric.WithDescription("Total bytes received through webhooks"),
		metric.WithUnit("bytes"),
	)
	if err != nil {
		return nil, err
	}

	linesCounter, err := meter.Int64Counter(
		"webhook_lines_total",
		metric.WithDescription("Total NDJSON lines received through webhooks"),
		metric.WithUnit("1"),
	)
	if err != nil {
		return nil, err
	}

	authFailuresCounter, err := meter.Int64Counter(
		"webhook_auth_failures_total",
		metric.WithDescription("Total authentication failures"),
		metric.WithUnit("1"),
	)
	if err != nil {
		return nil, err
	}

	uploadFailuresCounter, err := meter.Int64Counter(
		"webhook_upload_failures_total",
		metric.WithDescription("Total upload failures"),
		metric.WithUnit("1"),
	)
	if err != nil {
		return nil, err
	}

	cachingCounter, err := meter.Int64Counter(
		"webhook_cached_uploads_total",
		metric.WithDescription("Total uploads cached locally"),
		metric.WithUnit("1"),
	)
	if err != nil {
		return nil, err
	}

	return &WebhookMetrics{
		requestsCounter:       requestsCounter,
		bytesCounter:          bytesCounter,
		linesCounter:          linesCounter,
		authFailuresCounter:   authFailuresCounter,
		uploadFailuresCounter: uploadFailuresCounter,
		cachingCounter:        cachingCounter,
	}, nil
}

// RecordRequest records a webhook request with bytes and lines processed
func (m *WebhookMetrics) RecordRequest(ctx context.Context, tenantID, datasetID string, bytes int64, lines int64, success bool) {
	labels := []attribute.KeyValue{
		attribute.String("tenant_id", tenantID),
		attribute.String("dataset_id", datasetID),
		attribute.Bool("success", success),
	}

	m.requestsCounter.Add(ctx, 1, metric.WithAttributes(labels...))

	if success {
		m.bytesCounter.Add(ctx, bytes, metric.WithAttributes(labels...))
		m.linesCounter.Add(ctx, lines, metric.WithAttributes(labels...))
	}
}

// RecordAuthFailure records an authentication failure
func (m *WebhookMetrics) RecordAuthFailure(ctx context.Context, tenantID, reason string) {
	labels := []attribute.KeyValue{
		attribute.String("tenant_id", tenantID),
		attribute.String("reason", reason),
	}

	m.authFailuresCounter.Add(ctx, 1, metric.WithAttributes(labels...))
}

// RecordUploadFailure records an upload failure
func (m *WebhookMetrics) RecordUploadFailure(ctx context.Context, tenantID, datasetID, reason string) {
	labels := []attribute.KeyValue{
		attribute.String("tenant_id", tenantID),
		attribute.String("dataset_id", datasetID),
		attribute.String("reason", reason),
	}

	m.uploadFailuresCounter.Add(ctx, 1, metric.WithAttributes(labels...))
}

// RecordCachedUpload records an upload that was cached locally
func (m *WebhookMetrics) RecordCachedUpload(ctx context.Context, tenantID, datasetID string, bytes int64) {
	labels := []attribute.KeyValue{
		attribute.String("tenant_id", tenantID),
		attribute.String("dataset_id", datasetID),
	}

	m.cachingCounter.Add(ctx, 1, metric.WithAttributes(labels...))
	m.bytesCounter.Add(ctx, bytes, metric.WithAttributes(
		append(labels, attribute.String("destination", "cache"))...))
}
