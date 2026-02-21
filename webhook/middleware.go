// Licensed under Elastic License 2.0
// See LICENSE.txt for details

package webhook

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/bytefreezer/goodies/log"
	"github.com/bytefreezer/receiver/config"
	"github.com/bytefreezer/receiver/domain"
	"github.com/go-chi/chi/v5"
)

type SecurityMiddleware struct {
	config *config.Config
}

func NewSecurityMiddleware(conf *config.Config) *SecurityMiddleware {
	return &SecurityMiddleware{
		config: conf,
	}
}

// BearerTokenAuth middleware validates Bearer token for tenant
func (sm *SecurityMiddleware) BearerTokenAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tenantID := chi.URLParam(r, "tenantId")
		if tenantID == "" {
			http.Error(w, "Tenant ID required", http.StatusBadRequest)
			return
		}

		// Validate tenant exists and is active
		tenant := sm.config.TenantDatabase.GetTenantByID(tenantID)
		if tenant == nil {
			log.Warnf("Unknown or inactive tenant: %s", tenantID)
			http.Error(w, "Tenant not found or inactive", http.StatusGone)
			return
		}

		// If authentication is disabled, skip token validation
		if !sm.config.Authentication.Enabled {
			log.Debugf("Authentication disabled - allowing request for tenant %s", tenantID)
			ctx := context.WithValue(r.Context(), tenantContextKey, tenant)
			next.ServeHTTP(w, r.WithContext(ctx))
			return
		}

		// Authentication is enabled - validate bearer token
		authHeader := r.Header.Get("Authorization")
		if authHeader == "" {
			log.Warnf("Missing Authorization header for tenant %s", tenantID)
			http.Error(w, "Authorization header required", http.StatusUnauthorized)
			return
		}

		// Extract Bearer token
		parts := strings.Split(authHeader, " ")
		if len(parts) != 2 || strings.ToLower(parts[0]) != "bearer" {
			log.Warnf("Invalid Authorization header format for tenant %s", tenantID)
			http.Error(w, "Invalid Authorization header format", http.StatusUnauthorized)
			return
		}

		token := parts[1]

		// If no bearer token is configured for the tenant, reject the request (security requirement)
		if tenant.BearerToken == "" {
			log.Warnf("No bearer token configured for tenant %s - rejecting request (security policy)", tenantID)

			// Send SOC alert for misconfigured tenant
			if sm.config.SOCAlertClient != nil {
				_ = sm.config.SOCAlertClient.SendAlert( // #nosec G104 - alert delivery is best effort
					"high",
					"Tenant Misconfiguration",
					"Tenant has no bearer token configured",
					fmt.Sprintf("Tenant ID: %s - rejecting data ingestion due to missing authentication", tenantID),
				)
			}

			http.Error(w, "Tenant authentication not configured", http.StatusUnauthorized)
			return
		}

		// If bearer token is configured, validate it
		if tenant.BearerToken != token {
			log.Warnf("Invalid Bearer token for tenant %s", tenantID)

			// Send SOC alert
			if sm.config.SOCAlertClient != nil {
				_ = sm.config.SOCAlertClient.SendAlert( // #nosec G104 - alert delivery is best effort
					"medium",
					"Authentication Failure",
					"Invalid bearer token used for webhook access",
					fmt.Sprintf("Tenant ID: %s, Token: %s", tenantID, token[:10]+"..."),
				)
			}

			http.Error(w, "Invalid Bearer token", http.StatusUnauthorized)
			return
		}

		log.Debugf("Bearer token validated successfully for tenant %s", tenantID)

		// Store tenant in context for later use
		ctx := context.WithValue(r.Context(), tenantContextKey, tenant)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// DatasetValidationMiddleware validates tenant has access to dataset
func (sm *SecurityMiddleware) DatasetValidationMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tenantID := chi.URLParam(r, "tenantId")
		datasetID := chi.URLParam(r, "datasetId")

		// Remove trailing dots that might be added by proxy URL construction
		tenantID = strings.TrimSuffix(tenantID, ".")
		datasetID = strings.TrimSuffix(datasetID, ".")

		log.Debugf("Extracted tenantID='%s', datasetID='%s'", tenantID, datasetID)

		if tenantID == "" || datasetID == "" {
			http.Error(w, "Tenant ID and Dataset ID required", http.StatusBadRequest)
			return
		}

		// Get tenant from context (should be set by BearerTokenAuth middleware)
		tenant, ok := r.Context().Value(tenantContextKey).(*domain.Tenant)
		if !ok || tenant == nil {
			log.Errorf("Tenant not found in context for dataset validation")
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		}

		// Find the dataset
		var targetDataset *domain.Dataset
		for _, dataset := range tenant.Datasets {
			if dataset.ID == datasetID {
				targetDataset = dataset
				break
			}
		}

		if targetDataset == nil {
			log.Warnf("Unknown dataset %s for tenant %s", datasetID, tenantID)
			http.Error(w, "Unknown dataset for tenant", http.StatusBadRequest)
			return
		}

		log.Debugf("Dataset validation passed for tenant %s, dataset %s", tenantID, datasetID)

		// Store dataset in context for later use
		ctx := context.WithValue(r.Context(), datasetContextKey, targetDataset)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
