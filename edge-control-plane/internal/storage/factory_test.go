package storage

import (
	"context"
	"testing"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/config"
)

func TestNew_EmptyBackendDefaultsFS(t *testing.T) {
	store, err := New(context.Background(), config.StorageConfig{ArtifactBackend: ""})
	if err != nil {
		t.Fatalf("expected no error for empty backend, got: %v", err)
	}
	if store == nil {
		t.Fatal("expected non-nil store")
	}
}

func TestNew_FSExplicit(t *testing.T) {
	store, err := New(context.Background(), config.StorageConfig{ArtifactBackend: "fs"})
	if err != nil {
		t.Fatalf("fs: %v", err)
	}
	if store == nil {
		t.Fatal("fs: expected non-nil store")
	}
}

func TestNew_S3(t *testing.T) {
	cfg := config.StorageConfig{
		ArtifactBackend: "s3",
		S3Bucket:        "test-bucket",
		S3Region:        "us-east-1",
	}
	store, err := New(context.Background(), cfg)
	if err != nil {
		t.Logf("s3: got error (expected without AWS creds): %v", err)
		return
	}
	if store == nil {
		t.Fatal("s3: expected non-nil store when no error")
	}
}

func TestNew_Remote(t *testing.T) {
	cfg := config.StorageConfig{
		ArtifactBackend:               "remote",
		PeerControlPlaneURL:           "https://peer.example.com",
		PeerControlPlaneInternalToken: "test-token",
		ArtifactPath:                  "/tmp/edgecloud-artifacts",
	}
	store, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatalf("remote: %v", err)
	}
	if store == nil {
		t.Fatal("remote: expected non-nil store")
	}
}

func TestNew_UnknownBackend(t *testing.T) {
	_, err := New(context.Background(), config.StorageConfig{ArtifactBackend: "bogus"})
	if err == nil {
		t.Fatal("expected error for unknown backend")
	}
	if err.Error() == "" {
		t.Error("expected non-empty error message")
	}
}
