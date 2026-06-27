package domain

import "time"

// Domain represents a tenant-owned FQDN bound to a specific app.
//
// `Status` is server-driven and updated only by `POST /api/internal/domains/{id}/status`
// (a stub in v1; a Caddy event webhook in v2). The `pending` state is the
// default: a row is created with `pending`, the ingress poller picks it up
// on the next 30s tick, and the row is rendered into Caddy's config so that
// ACME issuance can begin. Successful ACME issuance does NOT flip the
// status to `active` in v1 — that requires a Caddy event hook, deferred.
//
// `LastError` and `VerifiedAt` are intentionally nullable. They will be
// populated by the v2 webhook; their columns are present in v1 so the v2
// work doesn't need a schema migration.
type Domain struct {
	ID         string       `db:"id"          json:"id"`
	TenantID   string       `db:"tenant_id"   json:"tenant_id"`
	AppName    string       `db:"app_name"    json:"app_name"`
	FQDN       string       `db:"fqdn"        json:"fqdn"`
	Status     DomainStatus `db:"status"      json:"status"`
	LastError  *string      `db:"last_error"  json:"last_error,omitempty"`
	CreatedAt  time.Time    `db:"created_at"  json:"created_at"`
	VerifiedAt *time.Time   `db:"verified_at" json:"verified_at,omitempty"`
}

// DomainStatus is the server-driven state of a domain row. See the Domain
// doc comment for the lifecycle.
type DomainStatus string

const (
	// DomainStatusPending is the default state for a new row. The ingress
	// renders the FQDN route and allows ACME issuance; we don't wait for
	// any signal to consider the row "active enough to route".
	DomainStatusPending DomainStatus = "pending"
	// DomainStatusActive is set by the (v2) Caddy event webhook when a
	// cert is successfully issued. Not used in v1.
	DomainStatusActive DomainStatus = "active"
	// DomainStatusFailed is set by the (v2) Caddy event webhook when ACME
	// issuance fails permanently. Not used in v1.
	DomainStatusFailed DomainStatus = "failed"
)
