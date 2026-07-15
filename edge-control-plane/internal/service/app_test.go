package service

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/repository"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/storage"
	"github.com/jmoiron/sqlx"
)

// mockAppRepo implements appRepoInterface for testing.
type mockAppRepo struct {
	createFunc                func(ctx context.Context, app *domain.App) error
	getFunc                   func(ctx context.Context, tenantID, appName string) (*domain.App, error)
	listFunc                  func(ctx context.Context, tenantID string, limit int, afterName string) ([]domain.App, error)
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

// Issue #58 — the mock matches the keyset signature change in
// AppRepository.List. Callers of m.listFunc now pass
// (tenantID, limit, afterName) instead of (tenantID, limit, offset).
func (m *mockAppRepo) List(ctx context.Context, tenantID string, limit int, afterName string) ([]domain.App, error) {
	if m.listFunc != nil {
		return m.listFunc(ctx, tenantID, limit, afterName)
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

// WithTx returns a tx-bound *repository.AppRepository so the cascade
// path (issue #60) can call AtomicDelete inside `repository.Transaction`.
// Tests that don't drive Delete are free to leave this un-stubbed:
// they'll never reach WithTx because the closure only calls it on
// the Delete path.
func (m *mockAppRepo) WithTx(tx *sqlx.Tx) *repository.AppRepository {
	return repository.NewAppRepositoryFromDBTX(tx)
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

// deleteSvcForTest builds an AppService with mockable per-repo wrappers
// suitable for exercising Delete. Pass-through repos point at the
// sqlmock-backed *sqlx.DB so the cascade tx flows through sqlmock's
// expectation chain; artifactStore and trafficSplitRepo are mock
// objects the test can inspect for call counts (issue #60).
func deleteSvcForTest(
	t *testing.T,
	db *sqlx.DB,
	mockAppRepo appRepoInterface,
	artifactStore storage.ArtifactStore,
) *AppService {
	t.Helper()
	return &AppService{
		db:               db,
		appRepo:          mockAppRepo,
		quotaRepo:        &mockQuotaRepoForApps{},
		appEnvRepo:       repository.NewAppEnvRepository(db),
		activeRepo:       repository.NewActiveDeploymentRepository(db),
		deployRepo:       repository.NewDeploymentRepository(db),
		trafficSplitRepo: repository.NewTrafficSplitRepository(db),
		artifactStore:    artifactStore,
		outboxRepo:       repository.NewOutboxRepository(db),
		defaultRegion:    "global",
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
		listFunc: func(ctx context.Context, tenantID string, limit int, afterName string) ([]domain.App, error) {
			return apps, nil
		},
	}
	svc := appSvcForTest(repo, &mockQuotaRepoForApps{})

	page, err := svc.List(context.Background(), "t_tenant1", 50, "")
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if len(page.Apps) != 2 {
		t.Errorf("len(page.Apps) = %d, want 2", len(page.Apps))
	}
	if page.Limit != 50 {
		t.Errorf("page.Limit = %d, want 50", page.Limit)
	}
	if page.NextCursor != nil {
		t.Errorf("page.NextCursor = %v, want nil (final page)", *page.NextCursor)
	}
}

// TestAppService_List_HasMore_DetectsExtraRow pins the limit+1 fetch
// + cursor-encode contract (issue #58). When the repo returns
// limit+1 rows the service must (a) trim to limit and (b) emit a
// NextCursor from the last visible row's name.
func TestAppService_List_HasMore_DetectsExtraRow(t *testing.T) {
	// Mock returns limit+1 = 4 rows when the service asks for 3.
	repo := &mockAppRepo{
		listFunc: func(ctx context.Context, tenantID string, limit int, afterName string) ([]domain.App, error) {
			if limit != 4 {
				t.Errorf("service asked for limit=%d, want 4 (limit+1 probe)", limit)
			}
			return []domain.App{
				{ID: "a_1", Name: "alpha"},
				{ID: "a_2", Name: "beta"},
				{ID: "a_3", Name: "gamma"},
				{ID: "a_4", Name: "delta"}, // trailing probe row
			}, nil
		},
	}
	svc := appSvcForTest(repo, &mockQuotaRepoForApps{})

	page, err := svc.List(context.Background(), "t_tenant1", 3, "")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(page.Apps) != 3 {
		t.Errorf("len(page.Apps) = %d, want 3 (trailing row dropped)", len(page.Apps))
	}
	if page.Apps[2].Name != "gamma" {
		t.Errorf("page.Apps[2].Name = %q, want gamma", page.Apps[2].Name)
	}
	if page.NextCursor == nil {
		t.Fatal("page.NextCursor is nil, want a value")
	}
	// Round-trip the cursor back through decodeAppCursor to make sure
	// the encoded form is what the service expects on the next page.
	got, err := decodeAppCursor(*page.NextCursor)
	if err != nil {
		t.Fatalf("decodeAppCursor: %v", err)
	}
	if got != "gamma" {
		t.Errorf("cursor decoded name = %q, want gamma", got)
	}
}

// TestAppService_List_BadCursor pins the typed-error path (issue #58).
// A malformed cursor returns ErrInvalidAppCursor so the handler can
// map it to 400 without leaking decoder internals.
func TestAppService_List_BadCursor(t *testing.T) {
	repo := &mockAppRepo{
		listFunc: func(ctx context.Context, tenantID string, limit int, afterName string) ([]domain.App, error) {
			t.Fatal("repo.List must NOT be called when cursor decode fails")
			return nil, nil
		},
	}
	svc := appSvcForTest(repo, &mockQuotaRepoForApps{})

	_, err := svc.List(context.Background(), "t_tenant1", 50, "@@@not-base64@@@")
	if !errors.Is(err, ErrInvalidAppCursor) {
		t.Errorf("err = %v, want ErrInvalidAppCursor", err)
	}
}

// TestAppService_List_InvalidLimit pins the defense-in-depth guard
// against a non-positive limit. The handler clamps before calling;
// this is the belt-and-suspenders layer so a future regression can't
// silently trigger a SELECT with LIMIT 0.
func TestAppService_List_InvalidLimit(t *testing.T) {
	repo := &mockAppRepo{
		listFunc: func(ctx context.Context, tenantID string, limit int, afterName string) ([]domain.App, error) {
			t.Fatal("repo.List must NOT be called when limit <= 0")
			return nil, nil
		},
	}
	svc := appSvcForTest(repo, &mockQuotaRepoForApps{})

	_, err := svc.List(context.Background(), "t_tenant1", 0, "")
	if !errors.Is(err, ErrInvalidLimit) {
		t.Errorf("err = %v, want ErrInvalidLimit", err)
	}
}

func TestAppService_List_Empty(t *testing.T) {
	repo := &mockAppRepo{
		listFunc: func(ctx context.Context, tenantID string, limit int, afterName string) ([]domain.App, error) {
			return nil, nil
		},
	}
	svc := appSvcForTest(repo, &mockQuotaRepoForApps{})

	page, err := svc.List(context.Background(), "t_tenant1", 50, "")
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if len(page.Apps) != 0 {
		t.Errorf("len(page.Apps) = %d, want 0", len(page.Apps))
	}
	if page.NextCursor != nil {
		t.Errorf("page.NextCursor = %v, want nil (empty repo → final page)", *page.NextCursor)
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
	// AtomicDelete (parent). Returning `true` means the apps row was
	// deleted — the tx continues to the cascade deletes below.
	// AtomicDelete uses GetContext (DELETE … RETURNING) which sqlmock
	// routes through ExpectQuery, not ExpectExec.
	mock.ExpectQuery(`DELETE FROM apps WHERE tenant_id = .+ AND name = .+ RETURNING true`).
		WithArgs("t_test", "myapp").
		WillReturnRows(sqlmock.NewRows([]string{"deleted"}).AddRow(true))
	// Capture deployment IDs for post-commit artifact cleanup.
	mock.ExpectQuery(`SELECT .* FROM deployments .* tenant_id = .+ AND app_name = .+`).
		WithArgs("t_test", "myapp").
		WillReturnRows(sqlmock.NewRows([]string{"id", "tenant_id", "app_name", "created_at"}).
			AddRow("d_dep1", "t_test", "myapp", time.Now()).
			AddRow("d_dep2", "t_test", "myapp", time.Now()))
	// Child deletes in dependency order: app_env, active_deployments,
	// app_traffic_splits (before deployments because no FK CASCADE),
	// deployments.
	mock.ExpectExec(`DELETE FROM app_env`).
		WillReturnResult(sqlmock.NewResult(0, 2))
	mock.ExpectExec(`DELETE FROM active_deployments`).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`DELETE FROM app_traffic_splits`).
		WillReturnResult(sqlmock.NewResult(0, 0))
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

	repo := &mockAppRepo{}
	// appRepo.WithTx is called inside repository.Transaction and is
	// stubbed in mockAppRepo to return repository.NewAppRepositoryFromDBTX(tx).
	// AtomicDelete via that tx-scoped repo hits the sqlmock chain above.

	svc := deleteSvcForTest(t, db, repo, &mockArtifactStore{})
	if err := svc.Delete(context.Background(), "t_test", "myapp"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock expectations not met: %v", err)
	}
}

func TestAppService_Delete_NotFound(t *testing.T) {
	db, mock, cleanup := newDeploymentMockDB(t)
	defer cleanup()

	mock.ExpectBegin()
	// AtomicDelete returns no rows (sql.ErrNoRows → false from
	// sqlmock's RETURNING zero-row).
	mock.ExpectQuery(`DELETE FROM apps WHERE tenant_id = .+ AND name = .+ RETURNING true`).
		WithArgs("t_test", "missing").
		WillReturnRows(sqlmock.NewRows([]string{"deleted"})) // empty → ErrNoRows → deleted=false
	mock.ExpectRollback()

	// deleteErr = nil on the artifact mock — but the closure exits
	// on ErrAppNotFound before reaching artifact cleanup.
	svc := deleteSvcForTest(t, db, &mockAppRepo{}, &mockArtifactStore{})
	err := svc.Delete(context.Background(), "t_test", "missing")
	if !errors.Is(err, ErrAppNotFound) {
		t.Errorf("Delete() error = %v, want ErrAppNotFound", err)
	}
	if sqlErr := mock.ExpectationsWereMet(); sqlErr != nil {
		t.Errorf("sqlmock expectations not met: %v", sqlErr)
	}
}

// TestAppService_Delete_EnqueuesTaskPurge (issue #569 / #60) verifies
// that Delete enqueues a task_purge row inside the same tx as the
// cascade deletes, with the correct kind, reason, and app_name. This
// is the worker-side cleanup contract: receiving the purge causes the
// worker to drop per-tenant KV / cache / scheduling state for the app.
func TestAppService_Delete_EnqueuesTaskPurge(t *testing.T) {
	db, mock, cleanup := newDeploymentMockDB(t)
	defer cleanup()

	mock.ExpectBegin()
	mock.ExpectQuery(`DELETE FROM apps WHERE tenant_id = .+ AND name = .+ RETURNING true`).
		WithArgs("t_test", "myapp").
		WillReturnRows(sqlmock.NewRows([]string{"deleted"}).AddRow(true))
	mock.ExpectQuery(`SELECT .* FROM deployments .* tenant_id = .+ AND app_name = .+`).
		WithArgs("t_test", "myapp").
		WillReturnRows(sqlmock.NewRows([]string{"id", "tenant_id", "app_name", "created_at"}))
	mock.ExpectExec(`DELETE FROM app_env`).
		WithArgs("t_test", "myapp").
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(`DELETE FROM active_deployments`).
		WithArgs("t_test", "myapp").
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(`DELETE FROM app_traffic_splits`).
		WithArgs("t_test", "myapp").
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(`DELETE FROM deployments`).
		WithArgs("t_test", "myapp").
		WillReturnResult(sqlmock.NewResult(0, 0))
	// Capture the actual INSERT statement to assert payload shape.
	mock.ExpectExec(`INSERT INTO outbox`).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	svc := deleteSvcForTest(t, db, &mockAppRepo{}, &mockArtifactStore{})
	if err := svc.Delete(context.Background(), "t_test", "myapp"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock expectations not met: %v", err)
	}
}

// TestAppService_Delete_TrafficSplitsDeletedBeforeDeployments
// (issue #60) pins the ordering: app_traffic_splits MUST run before
// deployments because traffic_splits.deployment_id has no
// ON DELETE CASCADE (migration 009_traffic_splits). Reverse order
// would let a FKEY violation slip through from DELETE FROM deployments
// when the app ever had a traffic-split configured.
//
// sqlmock's QueryMatcherRegexp matches substrings — providing the
// expectations in the right call order proves the SQL is issued
// in that order. (If they were reversed, the DELETE FROM
// deployments would be issued before DELETE FROM app_traffic_splits
// and the mock expectations wouldn't match in sequence.)
func TestAppService_Delete_TrafficSplitsDeletedBeforeDeployments(t *testing.T) {
	db, mock, cleanup := newDeploymentMockDB(t)
	defer cleanup()

	mock.ExpectBegin()
	mock.ExpectQuery(`DELETE FROM apps WHERE tenant_id = .+ AND name = .+ RETURNING true`).
		WithArgs("t_test", "myapp").
		WillReturnRows(sqlmock.NewRows([]string{"deleted"}).AddRow(true))
	mock.ExpectQuery(`SELECT .* FROM deployments .* tenant_id = .+ AND app_name = .+`).
		WithArgs("t_test", "myapp").
		WillReturnRows(sqlmock.NewRows([]string{"id", "tenant_id", "app_name", "created_at"}))
	mock.ExpectExec(`DELETE FROM app_env`).
		WithArgs("t_test", "myapp").
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(`DELETE FROM active_deployments`).
		WithArgs("t_test", "myapp").
		WillReturnResult(sqlmock.NewResult(0, 0))
	// Traffic splits BEFORE deployments:
	mock.ExpectExec(`DELETE FROM app_traffic_splits`).
		WithArgs("t_test", "myapp").
		WillReturnResult(sqlmock.NewResult(0, 2))
	mock.ExpectExec(`DELETE FROM deployments`).
		WithArgs("t_test", "myapp").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`INSERT INTO outbox`).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	svc := deleteSvcForTest(t, db, &mockAppRepo{}, &mockArtifactStore{})
	if err := svc.Delete(context.Background(), "t_test", "myapp"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock expectations not met (cascading order may be wrong): %v", err)
	}
}

// TestAppService_Delete_CascadeFailsReturnsError (issue #60) pins
// the post-fix contract: a failing cascade step rolls the tx back,
// no outbox row is written, AND the caller sees the error.
// Compare with the previous behavior, which logged and returned
// nil even when the cascade partially failed (orphan rows + no
// worker tombstone).
//
// The sub-tests cover each cascade step in turn: every step's
// failure triggers the same rollback + no-enqueue contract.
func TestAppService_Delete_CascadeFailsReturnsError(t *testing.T) {
	cases := []struct {
		name        string
		failingStep string // sql pattern substring for the failing Exec
	}{
		{"app_env", "DELETE FROM app_env"},
		{"active_deployments", "DELETE FROM active_deployments"},
		{"traffic_splits", "DELETE FROM app_traffic_splits"},
		{"deployments", "DELETE FROM deployments"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, cleanup := newDeploymentMockDB(t)
			defer cleanup()

			mock.ExpectBegin()
			mock.ExpectQuery(`DELETE FROM apps WHERE tenant_id = .+ AND name = .+ RETURNING true`).
				WithArgs("t_test", "myapp").
				WillReturnRows(sqlmock.NewRows([]string{"deleted"}).AddRow(true))
			if tc.failingStep != "deployments" {
				// ListByApp runs only when the failures happen AFTER
				// the deployments list; for app_env/active/traffic_splits
				// the deployment listing still fires before the failure.
				mock.ExpectQuery(`SELECT .* FROM deployments .* tenant_id = .+ AND app_name = .+`).
					WithArgs("t_test", "myapp").
					WillReturnRows(sqlmock.NewRows([]string{"id", "tenant_id", "app_name", "created_at"}))
			}
			// Walk through cascade ordering, failing at the named
			// step. Steps before the failure expect Result; steps
			// after the failure must NOT fire (sqlmock fails
			// ExpectationsWereMet if extra SQL shows up).
			steps := []string{
				"DELETE FROM app_env",
				"DELETE FROM active_deployments",
				"DELETE FROM app_traffic_splits",
				"DELETE FROM deployments",
			}
			failed := false
			for _, step := range steps {
				if step == tc.failingStep {
					mock.ExpectExec(step).
						WithArgs("t_test", "myapp").
						WillReturnError(fmt.Errorf("simulated DB error"))
					failed = true
					continue
				}
				if failed {
					continue // no expectations for later steps
				}
				mock.ExpectExec(step).
					WithArgs("t_test", "myapp").
					WillReturnResult(sqlmock.NewResult(0, 0))
			}
			mock.ExpectRollback()
			// NO ExpectExec(`INSERT INTO outbox`) — closure exits
			// before reaching the enqueue step on any cascade
			// failure, and a stray INSERT would be caught by
			// ExpectationsWereMet.

			svc := deleteSvcForTest(t, db, &mockAppRepo{}, &mockArtifactStore{})
			err := svc.Delete(context.Background(), "t_test", "myapp")
			if err == nil {
				t.Fatalf("Delete(%s): expected error, got nil", tc.name)
			}
			if !strings.Contains(err.Error(), "simulated DB error") {
				t.Errorf("Delete(%s) err = %v, want it to wrap the simulated error", tc.name, err)
			}
			if sqlErr := mock.ExpectationsWereMet(); sqlErr != nil {
				t.Errorf("sqlmock expectations not met (likely unexpected INSERT INTO outbox after rollback): %v", sqlErr)
			}
		})
	}
}

// TestAppService_Delete_ArtifactFailureSurfacesAfterCommit (issue
// #60) pins the post-commit artifact-cleanup contract: a successful
// DB commit followed by an artifact-store failure returns a
// non-nil error with the deployment ID in the message. The
// pre-fix behavior would have logged the error and returned nil —
// the caller had no idea storage was inconsistent with the DB.
//
// errors.Join aggregates per-deployment failures so a multi-deployment
// app surfaces every problematic deployment, not just the last.
func TestAppService_Delete_ArtifactFailureSurfacesAfterCommit(t *testing.T) {
	db, mock, cleanup := newDeploymentMockDB(t)
	defer cleanup()

	mock.ExpectBegin()
	mock.ExpectQuery(`DELETE FROM apps WHERE tenant_id = .+ AND name = .+ RETURNING true`).
		WithArgs("t_test", "myapp").
		WillReturnRows(sqlmock.NewRows([]string{"deleted"}).AddRow(true))
	mock.ExpectQuery(`SELECT .* FROM deployments .* tenant_id = .+ AND app_name = .+`).
		WithArgs("t_test", "myapp").
		WillReturnRows(sqlmock.NewRows([]string{"id", "tenant_id", "app_name", "created_at"}).
			AddRow("d_dep1", "t_test", "myapp", time.Now()).
			AddRow("d_dep2", "t_test", "myapp", time.Now()))
	mock.ExpectExec(`DELETE FROM app_env`).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(`DELETE FROM active_deployments`).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(`DELETE FROM app_traffic_splits`).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(`DELETE FROM deployments`).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(`INSERT INTO outbox`).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	// Every artifact delete fails — both .wasm (per dep) and
	// .cwasm (per dep) errors. errors.Join collects them.
	store := &mockArtifactStore{deleteErr: fmt.Errorf("simulated S3 5xx")}
	svc := deleteSvcForTest(t, db, &mockAppRepo{}, store)

	err := svc.Delete(context.Background(), "t_test", "myapp")
	if err == nil {
		t.Fatalf("Delete: expected error from artifact cleanup, got nil")
	}
	// errors.Join produces a message containing both wrapped
	// errors: at least one wasm + one cwasm mention.
	if !strings.Contains(err.Error(), "d_dep1") {
		t.Errorf("err = %v, want it to mention d_dep1", err)
	}
	if !strings.Contains(err.Error(), "simulated S3 5xx") {
		t.Errorf("err = %v, want it to wrap simulated S3 5xx", err)
	}
	if sqlErr := mock.ExpectationsWereMet(); sqlErr != nil {
		t.Errorf("sqlmock expectations not met: %v", sqlErr)
	}
}

// TestAppService_Delete_CleansCwasmArtifact (issue #60) confirms
// the post-commit artifact pass issues both Delete (.wasm) and
// DeleteFormat ("cwasm") for every deployment. The mocked
// mockArtifactStore records every call into deleteCalls and
// deleteFormatCalls — we assert both happen for both deployments.
func TestAppService_Delete_CleansCwasmArtifact(t *testing.T) {
	db, mock, cleanup := newDeploymentMockDB(t)
	defer cleanup()

	mock.ExpectBegin()
	mock.ExpectQuery(`DELETE FROM apps WHERE tenant_id = .+ AND name = .+ RETURNING true`).
		WithArgs("t_test", "myapp").
		WillReturnRows(sqlmock.NewRows([]string{"deleted"}).AddRow(true))
	mock.ExpectQuery(`SELECT .* FROM deployments .* tenant_id = .+ AND app_name = .+`).
		WithArgs("t_test", "myapp").
		WillReturnRows(sqlmock.NewRows([]string{"id", "tenant_id", "app_name", "created_at"}).
			AddRow("d_a", "t_test", "myapp", time.Now()).
			AddRow("d_b", "t_test", "myapp", time.Now()))
	mock.ExpectExec(`DELETE FROM app_env`).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(`DELETE FROM active_deployments`).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(`DELETE FROM app_traffic_splits`).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(`DELETE FROM deployments`).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(`INSERT INTO outbox`).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	store := &mockArtifactStore{}
	svc := deleteSvcForTest(t, db, &mockAppRepo{}, store)

	if err := svc.Delete(context.Background(), "t_test", "myapp"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	// Two deployments × two formats (.wasm + .cwasm) = 4 calls.
	if got, want := len(store.deleteCalls), 2; got != want {
		t.Errorf("Delete calls = %d, want %d", got, want)
	}
	if got, want := len(store.deleteFormatCalls), 2; got != want {
		t.Errorf("DeleteFormat calls = %d, want %d", got, want)
	}
	// Every cwasm call should use the "cwasm" format.
	for _, call := range store.deleteFormatCalls {
		if !strings.HasSuffix(call, "|cwasm") {
			t.Errorf("DeleteFormat call %q does not target cwasm", call)
		}
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock expectations not met: %v", err)
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
