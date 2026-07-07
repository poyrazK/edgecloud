package service

import (
	"context"
	"errors"
	"io"
	"os"
	"testing"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
)

type mockRollbackDeploymentRepo struct {
	deleteByIDFn func(ctx context.Context, id string) error
}

func (m *mockRollbackDeploymentRepo) Create(ctx context.Context, d *domain.Deployment) error {
	return nil
}
func (m *mockRollbackDeploymentRepo) UpdateHashAndSignature(ctx context.Context, d *domain.Deployment) error {
	return nil
}
func (m *mockRollbackDeploymentRepo) DeleteByID(ctx context.Context, id string) error {
	return m.deleteByIDFn(ctx, id)
}

type mockRollbackArtifactStore struct {
	deleteFn func(ctx context.Context, tenantID, appName, deploymentID string) error
}

func (m *mockRollbackArtifactStore) Save(ctx context.Context, tenantID, appName, deploymentID string, r io.Reader) error {
	return nil
}
func (m *mockRollbackArtifactStore) SaveAndHash(ctx context.Context, tenantID, appName, deploymentID string, r io.Reader) ([]byte, error) {
	return nil, nil
}
func (m *mockRollbackArtifactStore) SaveFormat(ctx context.Context, tenantID, appName, deploymentID, format string, r io.Reader) error {
	return nil
}
func (m *mockRollbackArtifactStore) Delete(ctx context.Context, tenantID, appName, deploymentID string) error {
	return m.deleteFn(ctx, tenantID, appName, deploymentID)
}

func TestRollbackArtifactSave_AllCleanupSucceedsReturnsSaveErr(t *testing.T) {
	saveErr := errors.New("io error during save")
	repo := &mockRollbackDeploymentRepo{deleteByIDFn: func(ctx context.Context, id string) error { return nil }}
	store := &mockRollbackArtifactStore{deleteFn: func(ctx context.Context, tenantID, appName, deploymentID string) error { return nil }}

	got := rollbackArtifactSave(context.Background(), repo, store, "t_1", "hello", "d_1", saveErr)
	if !errors.Is(got, saveErr) {
		t.Errorf("error = %v, want %v", got, saveErr)
	}
}

func TestRollbackArtifactSave_RepoDeleteFailsReturnsSaveErr(t *testing.T) {
	saveErr := errors.New("disk full")
	repo := &mockRollbackDeploymentRepo{deleteByIDFn: func(ctx context.Context, id string) error { return errors.New("db down") }}
	store := &mockRollbackArtifactStore{deleteFn: func(ctx context.Context, tenantID, appName, deploymentID string) error { return nil }}

	got := rollbackArtifactSave(context.Background(), repo, store, "t_1", "hello", "d_1", saveErr)
	if !errors.Is(got, saveErr) {
		t.Errorf("error = %v, want original saveErr %v", got, saveErr)
	}
}

func TestRollbackArtifactSave_StoreDeleteNotFoundOk(t *testing.T) {
	saveErr := errors.New("timed out")
	repo := &mockRollbackDeploymentRepo{deleteByIDFn: func(ctx context.Context, id string) error { return nil }}
	store := &mockRollbackArtifactStore{deleteFn: func(ctx context.Context, tenantID, appName, deploymentID string) error { return os.ErrNotExist }}

	got := rollbackArtifactSave(context.Background(), repo, store, "t_1", "hello", "d_1", saveErr)
	if !errors.Is(got, saveErr) {
		t.Errorf("error = %v, want %v (os.ErrNotExist treated as success)", got, saveErr)
	}
}

func TestRollbackArtifactSave_StoreDeleteOtherErrorLogged(t *testing.T) {
	saveErr := errors.New("crash")
	repo := &mockRollbackDeploymentRepo{deleteByIDFn: func(ctx context.Context, id string) error { return nil }}
	store := &mockRollbackArtifactStore{deleteFn: func(ctx context.Context, tenantID, appName, deploymentID string) error {
		return errors.New("permission denied")
	}}

	got := rollbackArtifactSave(context.Background(), repo, store, "t_1", "hello", "d_1", saveErr)
	if !errors.Is(got, saveErr) {
		t.Errorf("error = %v, want original saveErr %v", got, saveErr)
	}
}

func TestRollbackArtifactSave_PreservesSentinel(t *testing.T) {
	saveErr := os.ErrNotExist
	repo := &mockRollbackDeploymentRepo{deleteByIDFn: func(ctx context.Context, id string) error { return nil }}
	store := &mockRollbackArtifactStore{deleteFn: func(ctx context.Context, tenantID, appName, deploymentID string) error { return nil }}

	got := rollbackArtifactSave(context.Background(), repo, store, "t_1", "hello", "d_1", saveErr)
	if !errors.Is(got, os.ErrNotExist) {
		t.Errorf("errors.Is(got, os.ErrNotExist) = false; sentinel not preserved")
	}
}
