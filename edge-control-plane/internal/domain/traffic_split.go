package domain

import "time"

// TrafficSplit represents one weighted deployment entry in the traffic split
// table for an app. Sum of all weights for a (tenant_id, app_name) = 100.
// A weight of 0 means the deployment is draining (no new connections).
type TrafficSplit struct {
	TenantID     string    `db:"tenant_id"`
	AppName      string    `db:"app_name"`
	DeploymentID string    `db:"deployment_id"`
	Weight       int       `db:"weight"`
	CreatedAt    time.Time `db:"created_at"`
}

// TrafficSplitRequest is the JSON body for PUT /api/v1/apps/{appName}/traffic.
type TrafficSplitRequest struct {
	Splits []TrafficSplitEntry `json:"splits"`
}

// TrafficSplitEntry describes one deployment's weight in a traffic split.
type TrafficSplitEntry struct {
	DeploymentID string `json:"deployment_id"`
	Weight       int    `json:"weight"`
}
