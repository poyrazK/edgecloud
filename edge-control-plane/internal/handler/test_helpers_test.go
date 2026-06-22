package handler_test

import (
	"context"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/middleware"
)

// withTenantID seeds the request context with a tenant ID so handler
// tests can call `middleware.GetTenantID(ctx)` and get a deterministic
// value. Mirrors what the production `middleware.Authenticate` does on
// a real authenticated request.
//
// Lives in `_test.go` so it doesn't bloat the production binary.
func withTenantID(parent context.Context, tenantID string) context.Context {
	return context.WithValue(parent, middleware.TenantIDKey, tenantID)
}
