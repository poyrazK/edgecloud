package storage

import (
	"context"
	"fmt"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/config"
)

// New selects and constructs the ArtifactStore for the configured
// backend. An empty `ArtifactBackend` defaults to "fs" so existing
// deployments need no config change. An unrecognized backend name
// returns an error so a typo in config fails at startup, not silently
// on first deploy.
//
// Takes `config.StorageConfig` directly so the storage package
// doesn't maintain a hand-rolled duplicate of every field — drift
// risk between the two structs is zero because there is only one
// struct. (`config` itself imports only stdlib + yaml; no cycle.)
//
// `ctx` is forwarded to backends that need it at construction time
// (today: only S3ArtifactStore, which uses it to load AWS config).
func New(ctx context.Context, cfg config.StorageConfig) (ArtifactStore, error) {
	backend := cfg.ArtifactBackend
	if backend == "" {
		backend = "fs"
	}
	switch backend {
	case "fs":
		return NewFSArtifactStore(cfg.ArtifactPath), nil
	case "s3":
		return NewS3ArtifactStore(ctx, cfg)
	case "remote":
		return NewRemoteArtifactStore(cfg)
	default:
		return nil, fmt.Errorf("unknown artifact backend %q (want fs|s3|remote)", backend)
	}
}
