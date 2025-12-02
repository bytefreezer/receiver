// Licensed under Elastic License 2.0
// See LICENSE.txt for details

package webhook

// contextKey is a custom type for context keys to avoid collisions
type contextKey string

const (
	tenantContextKey  contextKey = "tenant"
	datasetContextKey contextKey = "dataset"
)
