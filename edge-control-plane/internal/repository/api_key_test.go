package repository

import (
	"context"
	"database/sql"
	"errors"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/jmoiron/sqlx"
)

// newMockRepo wires a sqlmock-backed sqlx.DB into an APIKeyRepository.
func newMockRepo(t *testing.T) (*APIKeyRepository, sqlmock.Sqlmock, func()) {
	t.Helper()
	mockDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	sqlxDB := sqlx.NewDb(mockDB, "postgres")
	return &APIKeyRepository{db: sqlxDB}, mock, func() { _ = mockDB.Close() }
}

func TestAPIKeyRepository_Create_RejectsMissingHashAlgorithm(t *testing.T) {
	repo, mock, cleanup := newMockRepo(t)
	defer cleanup()

	// No HashAlgorithm set — the repo must refuse to issue a query and
	// surface a clear error rather than silently writing argon2id.
	k := &domain.APIKey{
		ID:         "k_abc",
		TenantID:   "t_test",
		Name:       "my-key",
		KeyHash:    "anything",
		LookupHash: "0xdeadbeef",
		Role:       domain.RoleDeveloper,
		CreatedAt:  time.Now(),
	}

	err := repo.Create(context.Background(), k)
	if err == nil {
		t.Fatal("expected error for missing HashAlgorithm, got nil")
	}
	if !strings.Contains(err.Error(), "HashAlgorithm") {
		t.Errorf("error %q should name 'HashAlgorithm'", err.Error())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet sqlmock expectations — should have been zero: %v", err)
	}
}

func TestAPIKeyRepository_Create_RejectsMissingLookupHash(t *testing.T) {
	repo, mock, cleanup := newMockRepo(t)
	defer cleanup()

	// LookupHash empty — the repo must refuse to issue a query and
	// surface a clear error. A row without a lookup hash is invisible
	// to AuthenticateRawKey and (because the partial UNIQUE index
	// tolerates NULLs) would allow duplicates to pile up unnoticed.
	k := &domain.APIKey{
		ID:            "k_abc",
		TenantID:      "t_test",
		Name:          "my-key",
		KeyHash:       "$argon2id$v=19$m=65536,t=1,p=4$AAAA$BBBB",
		LookupHash:    "", // missing
		Role:          domain.RoleDeveloper,
		CreatedAt:     time.Now(),
		HashAlgorithm: domain.HashAlgorithmArgon2ID,
	}

	err := repo.Create(context.Background(), k)
	if err == nil {
		t.Fatal("expected error for missing LookupHash, got nil")
	}
	if !strings.Contains(err.Error(), "LookupHash") {
		t.Errorf("error %q should name 'LookupHash'", err.Error())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet sqlmock expectations — should have been zero: %v", err)
	}
}

func TestAPIKeyRepository_Create_IncludesLookupHash(t *testing.T) {
	repo, mock, cleanup := newMockRepo(t)
	defer cleanup()

	k := &domain.APIKey{
		ID:            "k_abc",
		TenantID:      "t_test",
		Name:          "my-key",
		KeyHash:       "$argon2id$v=19$m=65536,t=1,p=4$AAAA$BBBB",
		LookupHash:    "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		Role:          domain.RoleDeveloper,
		CreatedAt:     time.Now(),
		ExpiresAt:     nil,
		HashAlgorithm: domain.HashAlgorithmArgon2ID,
	}

	mock.ExpectExec(regexp.QuoteMeta(`INSERT INTO api_keys`)).
		WithArgs(
			k.ID, k.TenantID, k.Name, k.KeyHash, k.LookupHash, k.Role,
			sqlmock.AnyArg(), // created_at
			sqlmock.AnyArg(), // expires_at (NULL)
			k.HashAlgorithm,
		).
		WillReturnResult(sqlmock.NewResult(1, 1))

	if err := repo.Create(context.Background(), k); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet sqlmock expectations: %v", err)
	}
}

func TestAPIKeyRepository_GetByLookupHash_DecodesRow(t *testing.T) {
	repo, mock, cleanup := newMockRepo(t)
	defer cleanup()

	lookup := "deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef"
	rows := sqlmock.NewRows([]string{
		"id", "tenant_id", "name", "key_hash", "lookup_hash",
		"role", "created_at", "last_used", "expires_at", "hash_algorithm",
	}).AddRow(
		"k_abc", "t_test", "my-key",
		"$argon2id$v=19$m=65536,t=1,p=4$AAAA$BBBB",
		lookup,
		domain.RoleDeveloper,
		time.Now(), nil, nil,
		domain.HashAlgorithmArgon2ID,
	)

	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, tenant_id, name, key_hash, lookup_hash`)).
		WithArgs(lookup).
		WillReturnRows(rows)

	got, err := repo.GetByLookupHash(context.Background(), lookup)
	if err != nil {
		t.Fatalf("GetByLookupHash: %v", err)
	}
	if got == nil {
		t.Fatal("expected non-nil APIKey")
	}
	if got.ID != "k_abc" {
		t.Errorf("ID = %q, want k_abc", got.ID)
	}
	if got.LookupHash != lookup {
		t.Errorf("LookupHash = %q, want %q", got.LookupHash, lookup)
	}
	if got.HashAlgorithm != domain.HashAlgorithmArgon2ID {
		t.Errorf("HashAlgorithm = %q, want %q", got.HashAlgorithm, domain.HashAlgorithmArgon2ID)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet sqlmock expectations: %v", err)
	}
}

func TestAPIKeyRepository_GetByLookupHash_NoRows_ReturnsNilNil(t *testing.T) {
	repo, mock, cleanup := newMockRepo(t)
	defer cleanup()

	lookup := "missing"
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, tenant_id, name, key_hash, lookup_hash`)).
		WithArgs(lookup).
		WillReturnError(sql.ErrNoRows)

	got, err := repo.GetByLookupHash(context.Background(), lookup)
	if err != nil {
		t.Fatalf("expected nil error for sql.ErrNoRows, got %v", err)
	}
	if got != nil {
		t.Errorf("expected nil APIKey for missing row, got %+v", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet sqlmock expectations: %v", err)
	}
}

func TestAPIKeyRepository_GetByLookupHash_PropagatesDBError(t *testing.T) {
	repo, mock, cleanup := newMockRepo(t)
	defer cleanup()

	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, tenant_id, name, key_hash, lookup_hash`)).
		WithArgs("any").
		WillReturnError(errors.New("connection refused"))

	_, err := repo.GetByLookupHash(context.Background(), "any")
	if err == nil {
		t.Fatal("expected error from repo, got nil")
	}
	if err.Error() != "connection refused" {
		t.Errorf("error = %v, want %q", err, "connection refused")
	}
}

func TestAPIKeyRepository_Update_Success(t *testing.T) {
	repo, mock, cleanup := newMockRepo(t)
	defer cleanup()

	key := &domain.APIKey{ID: "k_1", Name: "renamed", Role: domain.RoleViewer, ExpiresAt: nil}

	mock.ExpectExec(regexp.QuoteMeta(`UPDATE api_keys SET name = $2, role = $3 WHERE id = $1`)).
		WithArgs(key.ID, key.Name, key.Role).
		WillReturnResult(sqlmock.NewResult(0, 1))

	if err := repo.Update(context.Background(), key); err != nil {
		t.Fatalf("Update: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
}

func TestAPIKeyRepository_Update_NameOnly(t *testing.T) {
	repo, mock, cleanup := newMockRepo(t)
	defer cleanup()

	key := &domain.APIKey{ID: "k_1", Name: "renamed", Role: domain.RoleDeveloper, ExpiresAt: nil}

	mock.ExpectExec(regexp.QuoteMeta(`UPDATE api_keys SET name = $2, role = $3 WHERE id = $1`)).
		WithArgs(key.ID, key.Name, key.Role).
		WillReturnResult(sqlmock.NewResult(0, 1))

	if err := repo.Update(context.Background(), key); err != nil {
		t.Fatalf("Update: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet mock expectations: %v", err)
	}
}
