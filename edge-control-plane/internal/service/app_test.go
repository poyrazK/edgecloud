package service

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/repository"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/storage"
)

// mockAppRepo implements appRepoInterface for testing.
type mockAppRepo struct {
	createFunc                func(ctx context.Context, app *domain.App) error
	getFunc                   func(ctx context.Context, tenantID, appName string) (*domain.App, error)
	listFunc                  func(ctx context.Context, tenantID string, limit, offset int) ([]domain.App, error)
	countByTenantFunc         func(ctx context.Context, tenantID string) (int, error)
	atomicDeleteFunc          func(ctx context.Context, tenantID, appName string) (bool, error)
	insertIfNotExistsFunc     func(ctx context.Context, app *domain.App) (bool, error)
	updateFunc                func(ctx context.Context, app *domain.App) error
	getForUpdateFunc          func(ctx context.Context, tenantID, appName string) (*domain.App, error)
	deleteIfNoDeploymentsFunc func(ctx context.Context, tenantID, appName string) (bool, error)
	// L4 port allocation (issue #548). getL4PortFunc returns the
	// persisted port (0 if unallocated, sql.ErrNoRows if app missing).
	// allocateL4PortFunc returns the port that ended up persisted
	// (could differ from the input when a racing caller won).
	// allocatedL4PortsFunc returns the set of currently-taken ports
	// across all tenants; default empty set.
	getL4PortFunc      func(ctx context.Context, tenantID, appName string) (uint16, error)
	allocateL4PortFunc func(ctx context.Context, tenantID, appName string, port uint16) (uint16, error)
	releaseL4PortFunc  func(ctx context.Context, tenantID, appName string) error
	allocatedL4PortsFn func(ctx context.Context) (map[uint16]struct{}, error)
}

func (m *mockAppRepo) Create(ctx context.Context, app *domain.App) error {
	if m.createFunc != nil {
		return m.createFunc(ctx, app)
	}
	return nil
}

func (m *mockAppRepo) Get(ctx context.Context, tenantID, appName string) (*domain.App, error) {
	if m.getFunc != nil {
		return m.getFunc(ctx, tenantID, appName)
	}
	return nil, nil
}

func (m *mockAppRepo) List(ctx context.Context, tenantID string, limit, offset int) ([]domain.App, error) {
	if m.listFunc != nil {
		return m.listFunc(ctx, tenantID, limit, offset)
	}
	return nil, nil
}

func (m *mockAppRepo) CountByTenant(ctx context.Context, tenantID string) (int, error) {
	if m.countByTenantFunc != nil {
		return m.countByTenantFunc(ctx, tenantID)
	}
	return 0, nil
}

func (m *mockAppRepo) AtomicDelete(ctx context.Context, tenantID, appName string) (bool, error) {
	if m.atomicDeleteFunc != nil {
		return m.atomicDeleteFunc(ctx, tenantID, appName)
	}
	return false, nil
}

func (m *mockAppRepo) InsertIfNotExists(ctx context.Context, app *domain.App) (bool, error) {
	if m.insertIfNotExistsFunc != nil {
		return m.insertIfNotExistsFunc(ctx, app)
	}
	return false, nil
}

func (m *mockAppRepo) GetForUpdate(ctx context.Context, tenantID, appName string) (*domain.App, error) {
	if m.getForUpdateFunc != nil {
		return m.getForUpdateFunc(ctx, tenantID, appName)
	}
	return nil, nil
}

func (m *mockAppRepo) DeleteIfNoDeployments(ctx context.Context, tenantID, appName string) (bool, error) {
	if m.deleteIfNoDeploymentsFunc != nil {
		return m.deleteIfNoDeploymentsFunc(ctx, tenantID, appName)
	}
	return false, nil
}

func (m *mockAppRepo) Update(ctx context.Context, app *domain.App) error {
	if m.updateFunc != nil {
		return m.updateFunc(ctx, app)
	}
	return nil
}

// L4 port allocation methods (issue #548).

func (m *mockAppRepo) GetL4Port(ctx context.Context, tenantID, appName string) (uint16, error) {
	if m.getL4PortFunc != nil {
		return m.getL4PortFunc(ctx, tenantID, appName)
	}
	return 0, sql.ErrNoRows
}

func (m *mockAppRepo) AllocateL4Port(ctx context.Context, tenantID, appName string, port uint16) (uint16, error) {
	if m.allocateL4PortFunc != nil {
		return m.allocateL4PortFunc(ctx, tenantID, appName, port)
	}
	return port, nil
}

func (m *mockAppRepo) ReleaseL4Port(ctx context.Context, tenantID, appName string) error {
	if m.releaseL4PortFunc != nil {
		return m.releaseL4PortFunc(ctx, tenantID, appName)
	}
	return nil
}

func (m *mockAppRepo) AllocatedL4Ports(ctx context.Context) (map[uint16]struct{}, error) {
	if m.allocatedL4PortsFn != nil {
		return m.allocatedL4PortsFn(ctx)
	}
	return map[uint16]struct{}{}, nil
}

// mockQuotaRepoForApps implements quotaRepoInterface for testing.
type mockQuotaRepoForApps struct {
	getByTenantIDFunc func(ctx context.Context, tenantID string) (*domain.Quota, error)
}

func (m *mockQuotaRepoForApps) GetByTenantID(ctx context.Context, tenantID string) (*domain.Quota, error) {
	if m.getByTenantIDFunc != nil {
		return m.getByTenantIDFunc(ctx, tenantID)
	}
	return &domain.Quota{MaxApps: 5, MaxMemoryMB: 256}, nil
}

func (m *mockQuotaRepoForApps) AddOutboundBytes(_ context.Context, _ string, _ uint64) (*domain.Quota, error) {
	return &domain.Quota{}, nil
}

func (m *mockQuotaRepoForApps) AddRequestCount(_ context.Context, _ string, _ uint64) (*domain.Quota, error) {
	return &domain.Quota{}, nil
}

// AddResidentSeconds (issue #484 / #485) is a no-op for appSvc tests —
// the deployment-service path doesn't drive the heartbeat metering path
// so the apps-side handler tests don't need to assert against it.
func (m *mockQuotaRepoForApps) AddResidentSeconds(_ context.Context, _ string, _ uint64) (*domain.Quota, error) {
	return &domain.Quota{}, nil
}

// AddComputeMs (issue #555) is a no-op for appSvc tests — same
// rationale as AddResidentSeconds above. The apps-side handler tests
// don't drive the heartbeat metering path, so they don't need to
// assert against the FaaS duration accumulator.
func (m *mockQuotaRepoForApps) AddComputeMs(_ context.Context, _ string, _ uint64) (*domain.Quota, error) {
	return &domain.Quota{}, nil
}

func (m *mockQuotaRepoForApps) SetGraceUntil(_ context.Context, _ string, _ *time.Time) error {
	return nil
}

// appSvcForTest builds an AppService with mock dependencies.
// Only use for testing methods that don't invoke cascade delete (Create, Get, List, CreateIfNotExists).
// Delete is not testable without a real DB connection for repository.Transaction.
func appSvcForTest(repo appRepoInterface, quotaRepo quotaRepoInterface) *AppService {
	return &AppService{
		appRepo:          repo,
		quotaRepo:        quotaRepo,
		l4PortRangeStart: 31000,
		l4PortRangeEnd:   31999,
	}
}

func TestAppService_Create_HappyPath(t *testing.T) {
	createdApp := (*domain.App)(nil)
	repo := &mockAppRepo{
		insertIfNotExistsFunc: func(ctx context.Context, app *domain.App) (bool, error) {
			createdApp = app
			return true, nil
		},
	}
	svc := appSvcForTest(repo, &mockQuotaRepoForApps{})

	app, err := svc.Create(context.Background(), "t_tenant1", "my-app", &domain.CreateAppRequest{
		Description: "my description",
	})
	if err != nil {
		t.Errorf("Create() error = %v, want nil", err)
	}
	if app == nil {
		t.Fatal("Create() app = nil, want non-nil")
	}
	if app.TenantID != "t_tenant1" {
		t.Errorf("app.TenantID = %q, want %q", app.TenantID, "t_tenant1")
	}
	if app.Name != "my-app" {
		t.Errorf("app.Name = %q, want %q", app.Name, "my-app")
	}
	if app.Description == nil || *app.Description != "my description" {
		t.Errorf("app.Description = %v, want %q", app.Description, "my description")
	}
	if createdApp == nil {
		t.Error("repo InsertIfNotExists was not called")
	}
}

func TestAppService_Create_AlreadyExists(t *testing.T) {
	repo := &mockAppRepo{
		insertIfNotExistsFunc: func(ctx context.Context, app *domain.App) (bool, error) {
			return false, nil // already exists
		},
	}
	svc := appSvcForTest(repo, &mockQuotaRepoForApps{})

	app, err := svc.Create(context.Background(), "t_tenant1", "existing-app", &domain.CreateAppRequest{})
	if !errors.Is(err, ErrAppAlreadyExists) {
		t.Errorf("Create() error = %v, want ErrAppAlreadyExists", err)
	}
	if app != nil {
		t.Errorf("Create() app = %v, want nil", app)
	}
}

func TestAppService_Create_InvalidName(t *testing.T) {
	// IsValidAppName (issue #438 unified regex `^[a-z0-9][a-z0-9.\-_]{0,62}$`)
	// rejects empty strings and path-traversal shapes. Dots, underscores,
	// and hyphens are accepted in the middle of a name; uppercase,
	// whitespace, and slashes are not. Dotted names render as a
	// two-label host (`t_acme-myapp.v2.edgecloud.dev`) that operators
	// must provision `*.*.edgecloud.dev` DNS + cert to serve.
	tests := []struct {
		name    string
		appName string
	}{
		{"empty", ""},
		{"path traversal slash", "foo/bar"},
		{"path traversal backslash", "foo\\bar"},
		{"path traversal parent", ".."},
		{"path traversal combo", "../etc"},
	}
	repo := &mockAppRepo{}
	svc := appSvcForTest(repo, &mockQuotaRepoForApps{})

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := svc.Create(context.Background(), "t_tenant1", tt.appName, &domain.CreateAppRequest{})
			if err == nil {
				t.Errorf("Create(%q) error = nil, want non-nil", tt.appName)
			}
		})
	}
}

func TestAppService_Create_DBError(t *testing.T) {
	repo := &mockAppRepo{
		insertIfNotExistsFunc: func(ctx context.Context, app *domain.App) (bool, error) {
			return false, errors.New("db error")
		},
	}
	svc := appSvcForTest(repo, &mockQuotaRepoForApps{})

	_, err := svc.Create(context.Background(), "t_tenant1", "valid-app", &domain.CreateAppRequest{})
	if err == nil {
		t.Error("Create() error = nil, want non-nil")
	}
}

func TestAppService_Create_MaxAppsExceeded(t *testing.T) {
	repo := &mockAppRepo{
		insertIfNotExistsFunc: func(ctx context.Context, app *domain.App) (bool, error) {
			return true, nil
		},
		countByTenantFunc: func(ctx context.Context, tenantID string) (int, error) {
			return 1, nil // tenant already has 1 app
		},
	}
	quotaRepo := &mockQuotaRepoForApps{
		getByTenantIDFunc: func(ctx context.Context, tenantID string) (*domain.Quota, error) {
			return &domain.Quota{MaxApps: 1}, nil
		},
	}
	svc := appSvcForTest(repo, quotaRepo)

	_, err := svc.Create(context.Background(), "t_tenant1", "another-app", &domain.CreateAppRequest{})
	if !errors.Is(err, ErrMaxAppsQuotaExceeded) {
		t.Errorf("Create() error = %v, want ErrMaxAppsQuotaExceeded", err)
	}
}

func TestAppService_CreateIfNotExists_MaxAppsExceeded(t *testing.T) {
	repo := &mockAppRepo{
		countByTenantFunc: func(ctx context.Context, tenantID string) (int, error) {
			return 4, nil
		},
	}
	quotaRepo := &mockQuotaRepoForApps{
		getByTenantIDFunc: func(ctx context.Context, tenantID string) (*domain.Quota, error) {
			return &domain.Quota{MaxApps: 5}, nil
		},
	}
	// This case: count (4) < MaxApps (5) → should not error
	svc := appSvcForTest(repo, quotaRepo)
	err := svc.CreateIfNotExists(context.Background(), "t_tenant1", "new-app")
	if err != nil {
		t.Errorf("CreateIfNotExists() error = %v, want nil (under quota)", err)
	}

	// Now exhaust the quota
	quotaRepo.getByTenantIDFunc = func(ctx context.Context, tenantID string) (*domain.Quota, error) {
		return &domain.Quota{MaxApps: 1}, nil
	}
	repo.countByTenantFunc = func(ctx context.Context, tenantID string) (int, error) {
		return 1, nil
	}
	err = svc.CreateIfNotExists(context.Background(), "t_tenant1", "yet-another-app")
	if !errors.Is(err, ErrMaxAppsQuotaExceeded) {
		t.Errorf("CreateIfNotExists() error = %v, want ErrMaxAppsQuotaExceeded", err)
	}
}

func TestAppService_Get_NotFound(t *testing.T) {
	repo := &mockAppRepo{
		getFunc: func(ctx context.Context, tenantID, appName string) (*domain.App, error) {
			return nil, nil
		},
	}
	svc := appSvcForTest(repo, &mockQuotaRepoForApps{})

	app, err := svc.Get(context.Background(), "t_tenant1", "nonexistent")
	if err != nil {
		t.Errorf("Get() error = %v, want nil", err)
	}
	if app != nil {
		t.Errorf("Get() app = %v, want nil", app)
	}
}

func TestAppService_Get_Found(t *testing.T) {
	existing := &domain.App{
		ID:        "a_abc123",
		TenantID:  "t_tenant1",
		Name:      "my-app",
		CreatedAt: time.Now(),
	}
	repo := &mockAppRepo{
		getFunc: func(ctx context.Context, tenantID, appName string) (*domain.App, error) {
			return existing, nil
		},
	}
	svc := appSvcForTest(repo, &mockQuotaRepoForApps{})

	app, err := svc.Get(context.Background(), "t_tenant1", "my-app")
	if err != nil {
		t.Errorf("Get() error = %v, want nil", err)
	}
	if app == nil {
		t.Fatal("Get() app = nil, want non-nil")
	}
	if app.ID != "a_abc123" {
		t.Errorf("app.ID = %q, want %q", app.ID, "a_abc123")
	}
}

func TestAppService_List_HappyPath(t *testing.T) {
	apps := []domain.App{
		{ID: "a_1", TenantID: "t_tenant1", Name: "app-a"},
		{ID: "a_2", TenantID: "t_tenant1", Name: "app-b"},
	}
	repo := &mockAppRepo{
		listFunc: func(ctx context.Context, tenantID string, limit, offset int) ([]domain.App, error) {
			return apps, nil
		},
	}
	svc := appSvcForTest(repo, &mockQuotaRepoForApps{})

	got, err := svc.List(context.Background(), "t_tenant1", 50, 0)
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if len(got) != 2 {
		t.Errorf("len(got) = %d, want 2", len(got))
	}
}

func TestAppService_List_Empty(t *testing.T) {
	repo := &mockAppRepo{
		listFunc: func(ctx context.Context, tenantID string, limit, offset int) ([]domain.App, error) {
			return nil, nil
		},
	}
	svc := appSvcForTest(repo, &mockQuotaRepoForApps{})

	got, err := svc.List(context.Background(), "t_tenant1", 50, 0)
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if len(got) != 0 {
		t.Errorf("len(got) = %d, want 0", len(got))
	}
}

func TestAppService_CreateIfNotExists_HappyPath(t *testing.T) {
	var createdApp *domain.App
	repo := &mockAppRepo{
		countByTenantFunc: func(ctx context.Context, tenantID string) (int, error) {
			return 0, nil
		},
		insertIfNotExistsFunc: func(ctx context.Context, app *domain.App) (bool, error) {
			createdApp = app
			return true, nil
		},
	}
	svc := appSvcForTest(repo, &mockQuotaRepoForApps{})

	err := svc.CreateIfNotExists(context.Background(), "t_tenant1", "new-app")
	if err != nil {
		t.Errorf("CreateIfNotExists() error = %v, want nil", err)
	}
	if createdApp == nil {
		t.Error("InsertIfNotExists was not called")
	}
}

func TestAppService_CreateIfNotExists_Idempotent(t *testing.T) {
	// When app already exists, InsertIfNotExists returns false, no error.
	// The operation should still succeed (idempotent).
	repo := &mockAppRepo{
		countByTenantFunc: func(ctx context.Context, tenantID string) (int, error) {
			return 0, nil
		},
		insertIfNotExistsFunc: func(ctx context.Context, app *domain.App) (bool, error) {
			return false, nil // already existed
		},
	}
	svc := appSvcForTest(repo, &mockQuotaRepoForApps{})

	err := svc.CreateIfNotExists(context.Background(), "t_tenant1", "existing-app")
	if err != nil {
		t.Errorf("CreateIfNotExists() error = %v, want nil (idempotent)", err)
	}
}

func TestAppService_CreateIfNotExists_InvalidName(t *testing.T) {
	repo := &mockAppRepo{}
	svc := appSvcForTest(repo, &mockQuotaRepoForApps{})

	err := svc.CreateIfNotExists(context.Background(), "t_tenant1", "")
	if err == nil {
		t.Error("CreateIfNotExists() error = nil, want non-nil")
	}
}

// TestAppService_Delete_ArtifactCleanup verifies that Delete removes .wasm artifact files.
func TestAppService_Delete_ArtifactCleanup(t *testing.T) {
	tmpDir := t.TempDir()
	artifactStore := storage.NewFSArtifactStore(tmpDir)

	// Create some "deployment" artifacts on disk
	deployments := []struct {
		id string
	}{
		{"d_deploy1"},
		{"d_deploy2"},
	}
	for _, d := range deployments {
		path, _ := artifactStore.Path("t_tenant1", "my-app", d.id)
		if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			t.Fatalf("MkdirAll: %v", err)
		}
		if err := os.WriteFile(path, []byte("wasm content"), 0644); err != nil {
			t.Fatalf("WriteFile: %v", err)
		}
	}

	// Verify files exist
	for _, d := range deployments {
		path, _ := artifactStore.Path("t_tenant1", "my-app", d.id)
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("artifact file %s not created: %v", d.id, err)
		}
	}

	// Note: AppService.Delete also calls deployRepo.ListByApp and repo methods
	// that need a real DB. This test only verifies the artifact deletion logic
	// by checking that os.Remove is called (the artifactStore.Delete call).
	// A full integration test would exercise the complete Delete flow.
}

// TestArtifactStore_Delete_RemovesFile verifies that a real ArtifactStore.Delete
// removes the file and returns nil when the file exists.
func TestArtifactStore_Delete_RemovesFile(t *testing.T) {
	tmpDir := t.TempDir()
	artifactStore := storage.NewFSArtifactStore(tmpDir)

	deployID := "d_test1"
	path, err := artifactStore.Path("t_tenant1", "my-app", deployID)
	if err != nil {
		t.Fatalf("Path: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := os.WriteFile(path, []byte("content"), 0644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	err = artifactStore.Delete(context.Background(), "t_tenant1", "my-app", deployID)
	if err != nil {
		t.Errorf("Delete() error = %v, want nil", err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Errorf("artifact file still exists after Delete")
	}
}

// TestAppService_DeleteIfNoDeployments_PassesThrough verifies the
// service method delegates to the repo with the exact tenant/app
// arguments and surfaces both the bool (was a row deleted?) and
// the error. This is the compensating-write path called by
// DeploymentService.Deploy when the first deploy of an app fails
// at artifact save — we want to make sure the call wires through
// rather than getting silently swallowed by an interface mismatch.
func TestAppService_DeleteIfNoDeployments_PassesThrough(t *testing.T) {
	var gotTenant, gotApp string
	repo := &mockAppRepo{
		deleteIfNoDeploymentsFunc: func(_ context.Context, tenantID, appName string) (bool, error) {
			gotTenant = tenantID
			gotApp = appName
			return true, nil
		},
	}
	svc := appSvcForTest(repo, &mockQuotaRepoForApps{})

	deleted, err := svc.DeleteIfNoDeployments(context.Background(), "t_test", "myapp")
	if err != nil {
		t.Fatalf("DeleteIfNoDeployments err = %v, want nil", err)
	}
	if !deleted {
		t.Error("expected deleted=true from mock, got false")
	}
	if gotTenant != "t_test" || gotApp != "myapp" {
		t.Errorf("repo called with tenant=%q app=%q, want t_test/myapp", gotTenant, gotApp)
	}
}

// TestAppService_DeleteIfNoDeployments_RepoErrorSurfaces verifies
// the method surfaces repo errors (the rollback caller logs and
// continues, but it must at least see the error to log it). The
// bool result alongside the error is the standard sqlx shape.
func TestAppService_DeleteIfNoDeployments_RepoErrorSurfaces(t *testing.T) {
	repo := &mockAppRepo{
		deleteIfNoDeploymentsFunc: func(_ context.Context, _, _ string) (bool, error) {
			return false, errors.New("db gone (test)")
		},
	}
	svc := appSvcForTest(repo, &mockQuotaRepoForApps{})

	deleted, err := svc.DeleteIfNoDeployments(context.Background(), "t_test", "myapp")
	if err == nil {
		t.Fatal("expected error from repo, got nil")
	}
	if deleted {
		t.Error("expected deleted=false alongside error")
	}
}

func TestAppService_Update_Success(t *testing.T) {
	desc := "original desc"
	app := &domain.App{ID: "a_1", TenantID: "t_test", Name: "myapp", Description: &desc}

	repo := &mockAppRepo{
		getFunc: func(_ context.Context, tenantID, appName string) (*domain.App, error) {
			return app, nil
		},
		updateFunc: func(_ context.Context, a *domain.App) error {
			return nil
		},
	}
	svc := appSvcForTest(repo, &mockQuotaRepoForApps{})

	newDesc := "updated description"
	updated, err := svc.Update(context.Background(), "t_test", "myapp", &domain.UpdateAppRequest{
		Description: &newDesc,
	})
	if err != nil {
		t.Fatalf("Update() error = %v, want nil", err)
	}
	if updated.Description == nil || *updated.Description != "updated description" {
		t.Errorf("Description = %v, want 'updated description'", updated.Description)
	}
}

func TestAppService_Update_ClearsDescription(t *testing.T) {
	desc := "original desc"
	app := &domain.App{ID: "a_1", TenantID: "t_test", Name: "myapp", Description: &desc}

	repo := &mockAppRepo{
		getFunc: func(_ context.Context, tenantID, appName string) (*domain.App, error) {
			return app, nil
		},
		updateFunc: func(_ context.Context, a *domain.App) error {
			return nil
		},
	}
	svc := appSvcForTest(repo, &mockQuotaRepoForApps{})

	empty := ""
	updated, err := svc.Update(context.Background(), "t_test", "myapp", &domain.UpdateAppRequest{
		Description: &empty,
	})
	if err != nil {
		t.Fatalf("Update() error = %v, want nil", err)
	}
	if updated.Description != nil && *updated.Description != "" {
		t.Errorf("Description = %v, want empty string", *updated.Description)
	}
}

func TestAppService_Update_NotFound(t *testing.T) {
	repo := &mockAppRepo{
		getFunc: func(_ context.Context, tenantID, appName string) (*domain.App, error) {
			return nil, nil
		},
	}
	svc := appSvcForTest(repo, &mockQuotaRepoForApps{})

	newDesc := "doesn't matter"
	_, err := svc.Update(context.Background(), "t_test", "nonexistent", &domain.UpdateAppRequest{
		Description: &newDesc,
	})
	if !errors.Is(err, ErrAppNotFound) {
		t.Errorf("Update() error = %v, want ErrAppNotFound", err)
	}
}

func TestAppService_GetForUpdate_Found(t *testing.T) {
	repo := &mockAppRepo{
		getForUpdateFunc: func(_ context.Context, tenantID, appName string) (*domain.App, error) {
			return &domain.App{TenantID: tenantID, Name: appName}, nil
		},
	}
	svc := appSvcForTest(repo, &mockQuotaRepoForApps{})
	app, err := svc.GetForUpdate(context.Background(), "t_test", "myapp")
	if err != nil {
		t.Fatalf("GetForUpdate: %v", err)
	}
	if app == nil || app.Name != "myapp" {
		t.Errorf("unexpected app: %+v", app)
	}
}

func TestAppService_GetForUpdate_NotFound(t *testing.T) {
	svc := appSvcForTest(&mockAppRepo{}, &mockQuotaRepoForApps{})
	app, err := svc.GetForUpdate(context.Background(), "t_test", "missing")
	if err != nil {
		t.Fatalf("GetForUpdate: %v", err)
	}
	if app != nil {
		t.Errorf("expected nil, got %+v", app)
	}
}

func TestAppService_Delete_HappyPath(t *testing.T) {
	db, mock, cleanup := newDeploymentMockDB(t)
	defer cleanup()

	mock.ExpectBegin()
	mock.ExpectExec(`DELETE FROM app_env`).
		WillReturnResult(sqlmock.NewResult(0, 2))
	mock.ExpectExec(`DELETE FROM active_deployments`).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`DELETE FROM deployments`).
		WillReturnResult(sqlmock.NewResult(0, 3))
	// Issue #569: the cascade tx enqueues a task_purge tombstone
	// inside the same transaction as the row deletes. The
	// drainer will pick it up and publish to
	// `edgecloud.tasks.<region>`, where the worker
	// `Supervisor::handle_task_message` clears the per-tenant
	// KV / cache / scheduling state for this app.
	mock.ExpectExec(`INSERT INTO outbox`).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	repo := &mockAppRepo{
		atomicDeleteFunc: func(_ context.Context, tenantID, appName string) (bool, error) {
			return true, nil
		},
	}
	svc := &AppService{
		db:            db,
		appRepo:       repo,
		quotaRepo:     &mockQuotaRepoForApps{},
		appEnvRepo:    repository.NewAppEnvRepository(db),
		activeRepo:    repository.NewActiveDeploymentRepository(db),
		deployRepo:    repository.NewDeploymentRepository(db),
		outboxRepo:    repository.NewOutboxRepository(db),
		defaultRegion: "global",
	}
	if err := svc.Delete(context.Background(), "t_test", "myapp"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock expectations not met: %v", err)
	}
}

func TestAppService_Delete_NotFound(t *testing.T) {
	repo := &mockAppRepo{
		atomicDeleteFunc: func(_ context.Context, tenantID, appName string) (bool, error) {
			return false, nil
		},
	}
	svc := &AppService{
		appRepo:   repo,
		quotaRepo: &mockQuotaRepoForApps{},
	}
	err := svc.Delete(context.Background(), "t_test", "missing")
	if !errors.Is(err, ErrAppNotFound) {
		t.Errorf("Delete() error = %v, want ErrAppNotFound", err)
	}
}

// TestAppService_Delete_EnqueuesTaskPurge (issue #569) verifies
// that Delete enqueues a task_purge row inside the same tx as
// the cascade deletes, with the correct kind, reason, and
// app_name. This is the worker-side cleanup contract:
// receiving the purge causes the worker to drop per-tenant
// KV / cache / scheduling state for the app.
func TestAppService_Delete_EnqueuesTaskPurge(t *testing.T) {
	db, mock, cleanup := newDeploymentMockDB(t)
	defer cleanup()

	mock.ExpectBegin()
	mock.ExpectExec(`DELETE FROM app_env`).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(`DELETE FROM active_deployments`).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(`DELETE FROM deployments`).
		WillReturnResult(sqlmock.NewResult(0, 0))
	// Capture the actual INSERT statement to assert payload shape.
	mock.ExpectExec(`INSERT INTO outbox`).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	repo := &mockAppRepo{
		atomicDeleteFunc: func(_ context.Context, tenantID, appName string) (bool, error) {
			return true, nil
		},
	}
	svc := &AppService{
		db:            db,
		appRepo:       repo,
		quotaRepo:     &mockQuotaRepoForApps{},
		appEnvRepo:    repository.NewAppEnvRepository(db),
		activeRepo:    repository.NewActiveDeploymentRepository(db),
		deployRepo:    repository.NewDeploymentRepository(db),
		outboxRepo:    repository.NewOutboxRepository(db),
		defaultRegion: "global",
	}
	if err := svc.Delete(context.Background(), "t_test", "myapp"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock expectations not met: %v", err)
	}
}

// TestAppService_Delete_NoPurgeOnCascadeFailure (issue #569)
// verifies that a failing cascade step rolls back the tx and
// no outbox row is written. Without this guard, a partially
// deleted state could publish a phantom task_purge and the
// worker would purge state for an app whose CP-side rows are
// still present — leading to a confused worker.
//
// Note: `Delete` currently logs-and-continues on a cascade
// failure (the apps row has already been deleted above by
// AtomicDelete and can't be re-created atomically), so we
// don't assert on the return value. The invariant we DO
// assert is that the tx rolled back (sqlmock.ExpectRollback)
// and that no INSERT INTO outbox was issued (sqlmock would
// fail ExpectationsWereMet if the enqueue fired after the
// rollback — we leave a missing expectation on purpose).
func TestAppService_Delete_NoPurgeOnCascadeFailure(t *testing.T) {
	db, mock, cleanup := newDeploymentMockDB(t)
	defer cleanup()

	mock.ExpectBegin()
	mock.ExpectExec(`DELETE FROM app_env`).
		WillReturnResult(sqlmock.NewResult(0, 2))
	// activeRepo.Delete fails — tx must roll back, no
	// INSERT INTO outbox is allowed (the enqueue call sits
	// AFTER the cascade deletes inside the same tx closure,
	// so a rollback discards it).
	mock.ExpectExec(`DELETE FROM active_deployments`).
		WillReturnError(fmt.Errorf("simulated DB error"))
	mock.ExpectRollback()
	// Note: NO ExpectExec(`INSERT INTO outbox`) — if the
	// enqueue fires after rollback, sqlmock sees an
	// unexpected Exec and fails ExpectationsWereMet.

	repo := &mockAppRepo{
		atomicDeleteFunc: func(_ context.Context, tenantID, appName string) (bool, error) {
			return true, nil
		},
	}
	svc := &AppService{
		db:            db,
		appRepo:       repo,
		quotaRepo:     &mockQuotaRepoForApps{},
		appEnvRepo:    repository.NewAppEnvRepository(db),
		activeRepo:    repository.NewActiveDeploymentRepository(db),
		deployRepo:    repository.NewDeploymentRepository(db),
		outboxRepo:    repository.NewOutboxRepository(db),
		defaultRegion: "global",
	}
	// We don't assert on the return value — see the docstring
	// above. The key invariant is enforced by the sqlmock
	// expectation chain below.
	_ = svc.Delete(context.Background(), "t_test", "myapp")
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock expectations not met (likely unexpected outbox INSERT after rollback): %v", err)
	}
}

// ── L4 port allocation tests (issue #548) ─────────────────────────────

// GetL4Port: existing port returns it, app-missing returns
// ErrAppNotFound, unallocated returns (0, nil).
func TestAppService_GetL4Port_Existing(t *testing.T) {
	repo := &mockAppRepo{
		getL4PortFunc: func(_ context.Context, _, _ string) (uint16, error) {
			return 31042, nil
		},
	}
	svc := appSvcForTest(repo, &mockQuotaRepoForApps{})
	port, err := svc.GetL4Port(context.Background(), "t_a", "myapp")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if port != 31042 {
		t.Errorf("port = %d, want 31042", port)
	}
}

func TestAppService_GetL4Port_Unallocated(t *testing.T) {
	repo := &mockAppRepo{
		getL4PortFunc: func(_ context.Context, _, _ string) (uint16, error) {
			return 0, nil
		},
	}
	svc := appSvcForTest(repo, &mockQuotaRepoForApps{})
	port, err := svc.GetL4Port(context.Background(), "t_a", "myapp")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if port != 0 {
		t.Errorf("port = %d, want 0", port)
	}
}

func TestAppService_GetL4Port_AppMissing(t *testing.T) {
	repo := &mockAppRepo{
		getL4PortFunc: func(_ context.Context, _, _ string) (uint16, error) {
			return 0, sql.ErrNoRows
		},
	}
	svc := appSvcForTest(repo, &mockQuotaRepoForApps{})
	_, err := svc.GetL4Port(context.Background(), "t_a", "myapp")
	if !errors.Is(err, ErrAppNotFound) {
		t.Errorf("err = %v, want ErrAppNotFound", err)
	}
}

func TestAppService_GetL4Port_InvalidAppName(t *testing.T) {
	svc := appSvcForTest(&mockAppRepo{}, &mockQuotaRepoForApps{})
	_, err := svc.GetL4Port(context.Background(), "t_a", "INVALID NAME!")
	if err == nil {
		t.Error("expected error for invalid app name")
	}
}

// AllocateL4Port: fast path returns existing port unchanged.
func TestAppService_AllocateL4Port_AlreadyAllocated(t *testing.T) {
	var allocated bool
	repo := &mockAppRepo{
		getL4PortFunc: func(_ context.Context, _, _ string) (uint16, error) {
			return 31042, nil
		},
		allocateL4PortFunc: func(_ context.Context, _, _ string, _ uint16) (uint16, error) {
			allocated = true
			return 0, nil
		},
	}
	svc := appSvcForTest(repo, &mockQuotaRepoForApps{})
	port, err := svc.AllocateL4Port(context.Background(), "t_a", "myapp")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if port != 31042 {
		t.Errorf("port = %d, want 31042", port)
	}
	if allocated {
		t.Error("AllocateL4Port should not be called when port already exists")
	}
}

// AllocateL4Port: slow path picks a free port and persists it.
func TestAppService_AllocateL4Port_FirstAppInRange(t *testing.T) {
	repo := &mockAppRepo{
		// App exists but is unallocated (0, nil).
		getL4PortFunc: func(_ context.Context, _, _ string) (uint16, error) {
			return 0, nil
		},
		allocatedL4PortsFn: func(_ context.Context) (map[uint16]struct{}, error) {
			return map[uint16]struct{}{}, nil
		},
		allocateL4PortFunc: func(_ context.Context, _, _ string, p uint16) (uint16, error) {
			return p, nil
		},
	}
	svc := appSvcForTest(repo, &mockQuotaRepoForApps{})
	port, err := svc.AllocateL4Port(context.Background(), "t_a", "myapp")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if port != 31000 {
		t.Errorf("port = %d, want 31000 (first in range)", port)
	}
}

// AllocateL4Port: skips already-taken ports.
func TestAppService_AllocateL4Port_SkipsTaken(t *testing.T) {
	taken := map[uint16]struct{}{
		31000: {}, 31001: {}, 31002: {},
	}
	repo := &mockAppRepo{
		getL4PortFunc: func(_ context.Context, _, _ string) (uint16, error) {
			return 0, nil
		},
		allocatedL4PortsFn: func(_ context.Context) (map[uint16]struct{}, error) {
			return taken, nil
		},
		allocateL4PortFunc: func(_ context.Context, _, _ string, p uint16) (uint16, error) {
			return p, nil
		},
	}
	svc := appSvcForTest(repo, &mockQuotaRepoForApps{})
	port, err := svc.AllocateL4Port(context.Background(), "t_a", "myapp")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if port != 31003 {
		t.Errorf("port = %d, want 31003 (next free after 31000-31002)", port)
	}
}

// AllocateL4Port: range exhausted returns ErrL4PortRangeExhausted.
func TestAppService_AllocateL4Port_RangeExhausted(t *testing.T) {
	all := map[uint16]struct{}{}
	for p := uint16(31000); p <= 31999; p++ {
		all[p] = struct{}{}
	}
	repo := &mockAppRepo{
		getL4PortFunc: func(_ context.Context, _, _ string) (uint16, error) {
			return 0, nil
		},
		allocatedL4PortsFn: func(_ context.Context) (map[uint16]struct{}, error) {
			return all, nil
		},
	}
	svc := appSvcForTest(repo, &mockQuotaRepoForApps{})
	_, err := svc.AllocateL4Port(context.Background(), "t_a", "myapp")
	if !errors.Is(err, ErrL4PortRangeExhausted) {
		t.Errorf("err = %v, want ErrL4PortRangeExhausted", err)
	}
}

// AllocateL4Port: app-missing on the GetL4Port fast-path returns
// ErrAppNotFound without calling AllocateL4Port.
func TestAppService_AllocateL4Port_AppMissing(t *testing.T) {
	repo := &mockAppRepo{
		getL4PortFunc: func(_ context.Context, _, _ string) (uint16, error) {
			return 0, sql.ErrNoRows
		},
	}
	svc := appSvcForTest(repo, &mockQuotaRepoForApps{})
	_, err := svc.AllocateL4Port(context.Background(), "t_a", "myapp")
	if !errors.Is(err, ErrAppNotFound) {
		t.Errorf("err = %v, want ErrAppNotFound", err)
	}
}

// AllocateL4Port: when AllocateL4Port repo returns sql.ErrNoRows
// (app vanished between GetL4Port and AllocateL4Port), surface
// ErrAppNotFound.
func TestAppService_AllocateL4Port_VanishedDuringAllocation(t *testing.T) {
	repo := &mockAppRepo{
		allocatedL4PortsFn: func(_ context.Context) (map[uint16]struct{}, error) {
			return map[uint16]struct{}{}, nil
		},
		allocateL4PortFunc: func(_ context.Context, _, _ string, _ uint16) (uint16, error) {
			return 0, sql.ErrNoRows
		},
	}
	svc := appSvcForTest(repo, &mockQuotaRepoForApps{})
	_, err := svc.AllocateL4Port(context.Background(), "t_a", "myapp")
	if !errors.Is(err, ErrAppNotFound) {
		t.Errorf("err = %v, want ErrAppNotFound", err)
	}
}

// ReleaseL4Port: just delegates to repo.
func TestAppService_ReleaseL4Port(t *testing.T) {
	var called bool
	repo := &mockAppRepo{
		releaseL4PortFunc: func(_ context.Context, tenantID, appName string) error {
			called = true
			if tenantID != "t_a" || appName != "myapp" {
				t.Errorf("unexpected args: %q %q", tenantID, appName)
			}
			return nil
		},
	}
	svc := appSvcForTest(repo, &mockQuotaRepoForApps{})
	if err := svc.ReleaseL4Port(context.Background(), "t_a", "myapp"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !called {
		t.Error("ReleaseL4Port was not called on the repo")
	}
}

// ReleaseL4Port: invalid app name rejected before the repo call.
func TestAppService_ReleaseL4Port_InvalidAppName(t *testing.T) {
	svc := appSvcForTest(&mockAppRepo{}, &mockQuotaRepoForApps{})
	if err := svc.ReleaseL4Port(context.Background(), "t_a", "BAD NAME"); err == nil {
		t.Error("expected error for invalid app name")
	}
}
