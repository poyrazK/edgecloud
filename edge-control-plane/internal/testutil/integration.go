// Package testutil provides shared helpers for Go tests.
//
// Currently it hosts ShouldSkipIntegration, the guard used by every
// build-tag-gated test that needs Docker (e.g. testcontainers-based
// integration tests). The same logic used to live locally in
// internal/autoscale/regression_test.go:shouldSkipRegression; extracting
// it here lets the new migrations/roundtrip_test.go reuse the same
// skip semantics without duplicating the env-var + docker.sock stat
// dance. Mirror the autoscale convention so existing local-dev habits
// (`SKIP_INTEGRATION_TESTS=1 go test ./...`) keep working unchanged.
package testutil

import "os"

// ShouldSkipIntegration reports whether a build-tag-gated integration
// test should call t.Skip. Skips when envName is set OR
// /var/run/docker.sock is absent. Returns ("reason", true) when skipping;
// callers pass the reason to t.Skipf so the skip shows up in test output.
//
// Use this only from files gated by `//go:build integration` so the
// default `go test ./...` does not pull in testcontainers and friends.
func ShouldSkipIntegration(envName string) (string, bool) {
	if _, ok := os.LookupEnv(envName); ok {
		return envName + " set", true
	}
	if _, err := os.Stat("/var/run/docker.sock"); err != nil {
		return "/var/run/docker.sock not present (set " + envName + "=1 to skip)", true
	}
	return "", false
}
