//go:build ignore

// openapi_check validates that every route registered in main.go
// has a corresponding entry in docs/api/openapi.yaml, and vice versa.
// Run with: go run ./scripts/openapi_check.go
package main

import (
	"fmt"
	"os"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"
)

// route represents a single HTTP route extracted from main.go.
type route struct {
	method string
	path   string
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println("All routes covered by spec.")
}

func run() error {
	// Parse routes from main.go
	routes, err := parseRoutes("cmd/api/main.go")
	if err != nil {
		return fmt.Errorf("parse routes: %w", err)
	}

	// Parse routes from openapi.yaml
	specRoutes, err := parseSpecRoutes("docs/api/openapi.yaml")
	if err != nil {
		return fmt.Errorf("parse spec: %w", err)
	}

	// Build sets for comparison
	routeSet := make(map[string]bool)
	for _, r := range routes {
		routeSet[r.method+" "+r.path] = true
	}

	specSet := make(map[string]bool)
	for _, r := range specRoutes {
		specSet[r.method+" "+r.path] = true
	}

	// Check for routes in mux but not in spec
	// Exclude /openapi.yaml and /docs* — these are the spec/docs infrastructure,
	// not API routes, and are intentionally absent from the OpenAPI spec.
	var missing []string
	for route := range routeSet {
		if !specSet[route] && !isInfrastructure(route) {
			missing = append(missing, route)
		}
	}

	// Check for routes in spec but not in mux (typos or removed routes)
	var extra []string
	for specRoute := range specSet {
		if !routeSet[specRoute] && !isInfrastructure(specRoute) {
			extra = append(extra, specRoute)
		}
	}

	if len(missing) > 0 {
		fmt.Fprintln(os.Stderr, "Routes in mux but missing from spec:")
		for _, r := range missing {
			fmt.Fprintf(os.Stderr, "  %s\n", r)
		}
	}
	if len(extra) > 0 {
		fmt.Fprintln(os.Stderr, "Routes in spec but not in mux (check for typos or removed routes):")
		for _, r := range extra {
			fmt.Fprintf(os.Stderr, "  %s\n", r)
		}
	}

	if len(missing) > 0 || len(extra) > 0 {
		return fmt.Errorf("%d routes missing, %d extra", len(missing), len(extra))
	}

	return nil
}

// isInfrastructure returns true for:
//   - /openapi.yaml, /docs, /docs/ (spec/doc serving infrastructure)
//   - /api/... redirect routes (deprecated old paths that redirect to /api/v1/)
//     These are not part of the OpenAPI contract because they are not real endpoints.
func isInfrastructure(route string) bool {
	if route == "GET /openapi.yaml" || route == "GET /docs" || route == "GET /docs/" {
		return true
	}
	// Anything under /api/ that is NOT /api/v1/ is a deprecated redirect.
	// Extract the path portion (after "METHOD ").
	methodAndPath := route
	if idx := strings.Index(route, " "); idx != -1 {
		methodAndPath = route[idx+1:]
	}
	// Redirect routes: /api/... but not /api/v1/...
	hasPrefix := strings.HasPrefix(methodAndPath, "/api/")
	notVersioned := !strings.HasPrefix(methodAndPath, "/api/v1/")
	return hasPrefix && notVersioned
}

// parseRoutes extracts all mux.HandleFunc("METHOD /path", ...) from main.go.
func parseRoutes(filename string) ([]route, error) {
	// Read the file
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	// Match patterns like:
	// mux.HandleFunc("POST /api/v1/deploy/{appName}", ...)
	// api.HandleFunc("GET /api/v1/apps", ...)
	// admin.HandleFunc("DELETE /api/v1/admin/tenants/{tenantID}", ...)
	// internalMux.HandleFunc("GET /api/v1/internal/download/{deploymentID}", ...)
	pattern := regexp.MustCompile(`\.HandleFunc\s*\(\s*"([A-Z]+)\s+([^"]+)"`)
	matches := pattern.FindAllSubmatch(data, -1)

	var routes []route
	seen := make(map[string]bool)
	for _, m := range matches {
		method := string(m[1])
		path := string(m[2])
		// Normalize: strip the version prefix for comparison
		// Both spec and mux use /api/v1/... so we compare as-is
		key := method + " " + path
		if !seen[key] {
			seen[key] = true
			routes = append(routes, route{method: method, path: path})
		}
	}

	return routes, nil
}

// parseSpecRoutes extracts all operation paths from an OpenAPI 3.0 YAML file.
func parseSpecRoutes(filename string) ([]route, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	var doc map[string]interface{}
	if err := yaml.Unmarshal(data, &doc); err != nil {
		return nil, err
	}

	paths, ok := doc["paths"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("no 'paths' section found in spec")
	}

	var routes []route
	seen := make(map[string]bool)
	for path, pathItem := range paths {
		pathMethods, ok := pathItem.(map[string]interface{})
		if !ok {
			continue
		}
		for method := range pathMethods {
			// OpenAPI allows non-HTTP methods like "parameters", "summary", "description"
			method = strings.ToUpper(method)
			if method != "GET" && method != "POST" && method != "PUT" && method != "DELETE" && method != "PATCH" && method != "OPTIONS" && method != "HEAD" {
				continue
			}
			key := method + " " + path
			if !seen[key] {
				seen[key] = true
				routes = append(routes, route{method: method, path: path})
			}
		}
	}

	return routes, nil
}
