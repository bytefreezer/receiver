package webhook

// contextKey is a custom type for context keys to avoid collisions
type contextKey string

const (
	tenantContextKey  contextKey = "tenant"
	datasetContextKey contextKey = "dataset"
)
