package handler

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/service"
)

// SecretsAdminHandler exposes admin endpoints for secrets key management
// and re-encryption. All endpoints require X-Internal-Token auth.
type SecretsAdminHandler struct {
	encryptor *service.SecretEncryptor
	envSvc    *service.EnvService
}

func NewSecretsAdminHandler(encryptor *service.SecretEncryptor, envSvc *service.EnvService) *SecretsAdminHandler {
	return &SecretsAdminHandler{encryptor: encryptor, envSvc: envSvc}
}

// ListKeys returns the set of key IDs in the active keyring.
// GET /api/v1/admin/secrets/keys
//
// Issue #441: includes plaintext_row_count so operators can see at a
// glance how many legacy plaintext app_env rows still need migration.
// When encryption is disabled (dev mode), the field is omitted.
// The -1 sentinel on plaintext_row_count means "count query errored"
// — see the OpenAPI doc on /api/v1/admin/secrets/keys for the full
// state machine. We intentionally don't fail the response on a count
// error so the keyring info still reaches the operator.
func (h *SecretsAdminHandler) ListKeys(w http.ResponseWriter, r *http.Request) {
	ids := h.encryptor.KeyIDs()
	activeID := h.encryptor.ActiveKeyID()
	resp := map[string]interface{}{
		"key_ids":            ids,
		"active_key":         activeID,
		"encryption_enabled": h.encryptor != nil,
	}
	if h.encryptor != nil && h.envSvc != nil {
		n, err := h.envSvc.CountPlaintextRows(r.Context())
		if err != nil {
			// Don't fail the whole response — surface the count as -1
			// so operators see something is wrong without losing the
			// keyring info.
			log.Printf("secrets ListKeys count plaintext: %v", err)
			resp["plaintext_row_count"] = -1
		} else {
			resp["plaintext_row_count"] = n
		}
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		log.Printf("secrets ListKeys encode: %v", err)
	}
}

// ReEncrypt decrypts every env value and re-encrypts with the active key.
// POST /api/v1/admin/secrets/re-encrypt
//
// Issue #441: plaintext rows are skipped (they're already plaintext —
// re-encrypting is a no-op) and counted in plaintext_skipped. Hard
// decrypt errors (cipher mismatch) abort the sweep.
func (h *SecretsAdminHandler) ReEncrypt(w http.ResponseWriter, r *http.Request) {
	if h.encryptor == nil {
		http.Error(w, `{"error":"encryption is not configured"}`, http.StatusBadRequest)
		return
	}

	reEncrypted, plaintextSkipped, err := h.envSvc.ReEncryptAll(r.Context())
	if err != nil {
		log.Printf("secrets re-encrypt failed: %v", err)
		http.Error(w, `{"error":"re-encryption failed: `+err.Error()+`"}`, http.StatusInternalServerError)
		return
	}

	resp := map[string]interface{}{
		"re_encrypted":      reEncrypted,
		"plaintext_skipped": plaintextSkipped,
		"status":            "ok",
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		log.Printf("secrets ReEncrypt encode: %v", err)
	}
}
