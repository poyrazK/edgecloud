package service

import (
	"context"
	"errors"
	"testing"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
)

type mockEnvRepo struct {
	setFn         func(ctx context.Context, env *domain.AppEnv) error
	listFn        func(ctx context.Context, tenantID, appName string) ([]domain.AppEnv, error)
	listByAppsFn  func(ctx context.Context, tenantID string, appNames []string) ([]domain.AppEnv, error)
	listAllAppsFn func(ctx context.Context) ([]string, []string, error)
	deleteFn      func(ctx context.Context, tenantID, appName, key string) error
	streamAllFn   func(ctx context.Context, fn func(domain.AppEnv) error) error
}

func (m *mockEnvRepo) Set(ctx context.Context, env *domain.AppEnv) error {
	return m.setFn(ctx, env)
}
func (m *mockEnvRepo) List(ctx context.Context, tenantID, appName string) ([]domain.AppEnv, error) {
	return m.listFn(ctx, tenantID, appName)
}
func (m *mockEnvRepo) ListByApps(ctx context.Context, tenantID string, appNames []string) ([]domain.AppEnv, error) {
	if m.listByAppsFn != nil {
		return m.listByAppsFn(ctx, tenantID, appNames)
	}
	return nil, nil
}
func (m *mockEnvRepo) ListAllApps(ctx context.Context) ([]string, []string, error) {
	if m.listAllAppsFn != nil {
		return m.listAllAppsFn(ctx)
	}
	return nil, nil, nil
}
func (m *mockEnvRepo) Delete(ctx context.Context, tenantID, appName, key string) error {
	return m.deleteFn(ctx, tenantID, appName, key)
}
func (m *mockEnvRepo) StreamAll(ctx context.Context, fn func(domain.AppEnv) error) error {
	if m.streamAllFn != nil {
		return m.streamAllFn(ctx, fn)
	}
	return nil
}

func newEnvSvc(repo *mockEnvRepo) *EnvService {
	return NewEnvService(repo)
}

func TestEnvService_SetEnv(t *testing.T) {
	var called bool
	var capturedEnv domain.AppEnv
	repo := &mockEnvRepo{
		setFn: func(ctx context.Context, env *domain.AppEnv) error {
			called = true
			capturedEnv = *env
			return nil
		},
	}
	svc := newEnvSvc(repo)
	sec, _ := NewSecretEncryptor(testMasterKey)
	svc.SetSecretEncryptor(sec)

	if err := svc.SetEnv(context.Background(), "t_1", "hello", "LOG_LEVEL", "debug"); err != nil {
		t.Fatalf("SetEnv: %v", err)
	}
	if !called {
		t.Fatal("repo.Set was not called")
	}
	if capturedEnv.TenantID != "t_1" || capturedEnv.AppName != "hello" || capturedEnv.EnvKey != "LOG_LEVEL" {
		t.Errorf("env = %+v", capturedEnv)
	}
	if capturedEnv.EnvValue == "debug" {
		t.Error("env value should be encrypted, not plaintext")
	}
	if capturedEnv.EnvValue == "" {
		t.Error("encrypted value must not be empty")
	}
}

func TestEnvService_SetEnv_NoEncryptor_StoresPlaintext(t *testing.T) {
	var capturedValue string
	repo := &mockEnvRepo{
		setFn: func(ctx context.Context, env *domain.AppEnv) error {
			capturedValue = env.EnvValue
			return nil
		},
	}
	svc := newEnvSvc(repo) // no encryptor set

	if err := svc.SetEnv(context.Background(), "t_1", "hello", "K", "plaintext"); err != nil {
		t.Fatalf("SetEnv: %v", err)
	}
	if capturedValue != "plaintext" {
		t.Errorf("without encryptor, value must be stored as-is; got %q", capturedValue)
	}
}

func TestEnvService_ListEnv_Decrypts(t *testing.T) {
	sec, _ := NewSecretEncryptor(testMasterKey)
	encrypted, _ := sec.Encrypt("secret-value")

	repo := &mockEnvRepo{
		listFn: func(ctx context.Context, tenantID, appName string) ([]domain.AppEnv, error) {
			return []domain.AppEnv{
				{TenantID: "t_1", AppName: "hello", EnvKey: "API_KEY", EnvValue: encrypted},
			}, nil
		},
	}
	svc := newEnvSvc(repo)
	svc.SetSecretEncryptor(sec)

	envs, err := svc.ListEnv(context.Background(), "t_1", "hello")
	if err != nil {
		t.Fatalf("ListEnv: %v", err)
	}
	if len(envs) != 1 || envs[0].EnvKey != "API_KEY" {
		t.Errorf("envs = %+v", envs)
	}
	if envs[0].EnvValue != "secret-value" {
		t.Errorf("ListEnv should return plaintext; got %q", envs[0].EnvValue)
	}
}

func TestEnvService_ListEnv_LegacyPlaintext_ReturnsErrPlaintextEnvNotAllowed(t *testing.T) {
	sec, _ := NewSecretEncryptor(testMasterKey)

	repo := &mockEnvRepo{
		listFn: func(ctx context.Context, tenantID, appName string) ([]domain.AppEnv, error) {
			return []domain.AppEnv{
				{TenantID: "t_1", AppName: "hello", EnvKey: "K", EnvValue: "legacy-plaintext"},
			}, nil
		},
	}
	svc := newEnvSvc(repo)
	svc.SetSecretEncryptor(sec)

	// Issue #441: legacy plaintext rows now error at Decrypt time.
	// ListEnv wraps the per-row decrypt error.
	_, err := svc.ListEnv(context.Background(), "t_1", "hello")
	if err == nil {
		t.Fatal("ListEnv on legacy plaintext should error (issue #441), got nil")
	}
	if !errors.Is(err, ErrPlaintextEnvNotAllowed) {
		t.Errorf("err = %v, want ErrPlaintextEnvNotAllowed in chain", err)
	}
}

func TestEnvService_DecryptEnvMap(t *testing.T) {
	sec, _ := NewSecretEncryptor(testMasterKey)
	enc, _ := sec.Encrypt("db-pass")

	repo := &mockEnvRepo{
		listFn: func(ctx context.Context, tenantID, appName string) ([]domain.AppEnv, error) {
			return []domain.AppEnv{
				{TenantID: "t_1", AppName: "hello", EnvKey: "DB_PASS", EnvValue: enc},
			}, nil
		},
	}
	svc := newEnvSvc(repo)
	svc.SetSecretEncryptor(sec)

	m, err := svc.DecryptEnvMap(context.Background(), "t_1", "hello")
	if err != nil {
		t.Fatalf("DecryptEnvMap: %v", err)
	}
	if m["DB_PASS"] != "db-pass" {
		t.Errorf("DecryptEnvMap = %v, want {DB_PASS: db-pass}", m)
	}
}

func TestEnvService_DecryptEnvMapBulk(t *testing.T) {
	sec, _ := NewSecretEncryptor(testMasterKey)
	encA, _ := sec.Encrypt("secret-a")
	encB, _ := sec.Encrypt("secret-b")

	repo := &mockEnvRepo{
		listByAppsFn: func(ctx context.Context, tenantID string, appNames []string) ([]domain.AppEnv, error) {
			return []domain.AppEnv{
				{TenantID: "t_1", AppName: "app-a", EnvKey: "KEY_A", EnvValue: encA},
				{TenantID: "t_1", AppName: "app-b", EnvKey: "KEY_B", EnvValue: encB},
			}, nil
		},
	}
	svc := newEnvSvc(repo)
	svc.SetSecretEncryptor(sec)

	m, err := svc.DecryptEnvMapBulk(context.Background(), "t_1", []string{"app-a", "app-b"})
	if err != nil {
		t.Fatalf("DecryptEnvMapBulk: %v", err)
	}
	if m["app-a"]["KEY_A"] != "secret-a" || m["app-b"]["KEY_B"] != "secret-b" {
		t.Errorf("bulk decrypt = %v", m)
	}
}

func TestEnvService_SetEnv_PropagatesError(t *testing.T) {
	want := errors.New("db down")
	repo := &mockEnvRepo{setFn: func(ctx context.Context, env *domain.AppEnv) error { return want }}
	svc := newEnvSvc(repo)

	err := svc.SetEnv(context.Background(), "t_1", "hello", "K", "V")
	if !errors.Is(err, want) {
		t.Errorf("error = %v, want %v", err, want)
	}
}

func TestEnvService_ListEnv_PropagatesError(t *testing.T) {
	want := errors.New("db down")
	repo := &mockEnvRepo{listFn: func(ctx context.Context, tenantID, appName string) ([]domain.AppEnv, error) { return nil, want }}
	svc := newEnvSvc(repo)

	_, err := svc.ListEnv(context.Background(), "t_1", "hello")
	if !errors.Is(err, want) {
		t.Errorf("error = %v, want %v", err, want)
	}
}

// TestEnvService_CountPlaintextRows verifies the issue #441 startup
// sweep: every row whose env_value does NOT match the encrypted shape
// (kid:nonce:ct with a known kid) is counted.
func TestEnvService_CountPlaintextRows(t *testing.T) {
	sec, _ := NewSecretEncryptor(testMasterKey)
	enc, _ := sec.Encrypt("secret")

	repo := &mockEnvRepo{
		streamAllFn: func(ctx context.Context, fn func(domain.AppEnv) error) error {
			rows := []domain.AppEnv{
				{TenantID: "t_1", AppName: "a", EnvKey: "K1", EnvValue: enc},                  // cipher
				{TenantID: "t_1", AppName: "a", EnvKey: "K2", EnvValue: "plaintext-row"},      // plaintext
				{TenantID: "t_1", AppName: "a", EnvKey: "K3", EnvValue: ""},                   // empty (plaintext shape)
				{TenantID: "t_1", AppName: "b", EnvKey: "K1", EnvValue: "k1:hex:hex-unknown"}, // 3-part with unknown kid (plaintext shape)
			}
			for _, r := range rows {
				if err := fn(r); err != nil {
					return err
				}
			}
			return nil
		},
	}
	svc := newEnvSvc(repo)
	svc.SetSecretEncryptor(sec)

	n, err := svc.CountPlaintextRows(context.Background())
	if err != nil {
		t.Fatalf("CountPlaintextRows: %v", err)
	}
	// 1 encrypted + 3 plaintext-shaped (plaintext, empty, 3-part unknown kid)
	if n != 3 {
		t.Errorf("CountPlaintextRows = %d, want 3", n)
	}
}

// TestEnvService_CountPlaintextRows_NoEncryptor: dev mode (no
// encryptor wired) returns 0 — there's nothing to be plaintext "against".
func TestEnvService_CountPlaintextRows_NoEncryptor(t *testing.T) {
	repo := &mockEnvRepo{
		streamAllFn: func(ctx context.Context, fn func(domain.AppEnv) error) error {
			return fn(domain.AppEnv{EnvValue: "anything"})
		},
	}
	svc := newEnvSvc(repo)
	n, err := svc.CountPlaintextRows(context.Background())
	if err != nil {
		t.Fatalf("CountPlaintextRows: %v", err)
	}
	if n != 0 {
		t.Errorf("CountPlaintextRows = %d, want 0 (no encryptor)", n)
	}
}

// TestEnvService_ReEncryptAll_SkipsPlaintext: issue #441 — plaintext
// rows are skipped and counted; only cipher rows are re-encrypted.
func TestEnvService_ReEncryptAll_SkipsPlaintext(t *testing.T) {
	sec, _ := NewSecretEncryptor(testMasterKey)
	enc, _ := sec.Encrypt("secret")

	var setCalls int
	repo := &mockEnvRepo{
		listAllAppsFn: func(ctx context.Context) ([]string, []string, error) {
			return []string{"t_1", "t_1"}, []string{"a", "b"}, nil
		},
		listFn: func(ctx context.Context, tenantID, appName string) ([]domain.AppEnv, error) {
			switch appName {
			case "a":
				return []domain.AppEnv{{TenantID: "t_1", AppName: "a", EnvKey: "K", EnvValue: enc}}, nil
			case "b":
				return []domain.AppEnv{{TenantID: "t_1", AppName: "b", EnvKey: "K", EnvValue: "plaintext-row"}}, nil
			}
			return nil, nil
		},
		setFn: func(ctx context.Context, env *domain.AppEnv) error {
			setCalls++
			if env.EnvValue == "plaintext-row" {
				t.Errorf("plaintext row must not be re-written (would loop forever): %+v", env)
			}
			return nil
		},
	}
	svc := newEnvSvc(repo)
	svc.SetSecretEncryptor(sec)

	reEncrypted, skipped, err := svc.ReEncryptAll(context.Background())
	if err != nil {
		t.Fatalf("ReEncryptAll: %v", err)
	}
	if reEncrypted != 1 {
		t.Errorf("reEncrypted = %d, want 1", reEncrypted)
	}
	if skipped != 1 {
		t.Errorf("plaintextSkipped = %d, want 1", skipped)
	}
	if setCalls != 1 {
		t.Errorf("setCalls = %d, want 1", setCalls)
	}
}
