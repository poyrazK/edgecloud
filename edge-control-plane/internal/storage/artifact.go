package storage

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// ArtifactStore handles Wasm artifact storage on the filesystem.
type ArtifactStore struct {
	basePath string
}

func NewArtifactStore(basePath string) *ArtifactStore {
	return &ArtifactStore{basePath: basePath}
}

// Path returns the filesystem path for a deployment artifact.
func (s *ArtifactStore) Path(tenantID, appName, deploymentID string) string {
	return filepath.Join(s.basePath, tenantID, appName, deploymentID+".wasm")
}

// Save writes a Wasm artifact to disk.
func (s *ArtifactStore) Save(tenantID, appName, deploymentID string, r io.Reader) error {
	path := s.Path(tenantID, appName, deploymentID)
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
	path := s.Path(tenantID, appName, deploymentID)
	return os.Open(path)
}

// Delete removes a Wasm artifact from disk.
func (s *ArtifactStore) Delete(tenantID, appName, deploymentID string) error {
	path := s.Path(tenantID, appName, deploymentID)
	return os.Remove(path)
}
