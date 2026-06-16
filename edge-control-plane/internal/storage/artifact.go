package storage

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// ArtifactStore handles Wasm artifact storage on the filesystem.
type ArtifactStore struct {
	basePath string
}

func NewArtifactStore(basePath string) *ArtifactStore {
	return &ArtifactStore{basePath: basePath}
}

// validatePathComponent checks that a path component doesn't contain traversal
// sequences or absolute paths. Returns an error if invalid.
func validatePathComponent(name, value string) error {
	if value == "" {
		return fmt.Errorf("%s cannot be empty", name)
	}
	if strings.ContainsAny(value, "/\\") {
		return fmt.Errorf("%s contains invalid characters", name)
	}
	if value == ".." || strings.Contains(value, "..") {
		return fmt.Errorf("%s cannot contain '..'", name)
	}
	return nil
}

// Path returns the filesystem path for a deployment artifact.
// Returns an error if any component is invalid.
func (s *ArtifactStore) Path(tenantID, appName, deploymentID string) (string, error) {
	if err := validatePathComponent("tenantID", tenantID); err != nil {
		return "", err
	}
	if err := validatePathComponent("appName", appName); err != nil {
		return "", err
	}
	if err := validatePathComponent("deploymentID", deploymentID); err != nil {
		return "", err
	}

	path := filepath.Join(s.basePath, tenantID, appName, deploymentID+".wasm")
	// Verify the resolved path is still under basePath (defense-in-depth)
	clean := filepath.Clean(path)
	if !strings.HasPrefix(clean, filepath.Clean(s.basePath)) {
		return "", fmt.Errorf("path traversal detected")
	}
	return path, nil
}

// Save writes a Wasm artifact to disk.
func (s *ArtifactStore) Save(tenantID, appName, deploymentID string, r io.Reader) error {
	path, err := s.Path(tenantID, appName, deploymentID)
	if err != nil {
		return fmt.Errorf("invalid artifact path: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return fmt.Errorf("creating artifact dir: %w", err)
	}

	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("creating artifact file: %w", err)
	}
	defer f.Close()

	if _, err := io.Copy(f, r); err != nil {
		return fmt.Errorf("writing artifact: %w", err)
	}
	return nil
}

// Open reads a Wasm artifact from disk.
func (s *ArtifactStore) Open(tenantID, appName, deploymentID string) (io.ReadCloser, error) {
	path, err := s.Path(tenantID, appName, deploymentID)
	if err != nil {
		return nil, fmt.Errorf("invalid artifact path: %w", err)
	}
	return os.Open(path)
}

// Delete removes a Wasm artifact from disk.
func (s *ArtifactStore) Delete(tenantID, appName, deploymentID string) error {
	path, err := s.Path(tenantID, appName, deploymentID)
	if err != nil {
		return fmt.Errorf("invalid artifact path: %w", err)
	}
	return os.Remove(path)
}
