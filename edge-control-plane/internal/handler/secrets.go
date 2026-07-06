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
func (h *SecretsAdminHandler) ListKeys(w http.ResponseWriter, r *http.Request) {
	ids := h.encryptor.KeyIDs()
	activeID := h.encryptor.ActiveKeyID()
	resp := map[string]interface{}{
		"key_ids":            ids,
		"active_key":         activeID,
		"encryption_enabled": h.encryptor != nil,
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		log.Printf("secrets ListKeys encode: %v", err)
	}
}

// ReEncrypt decrypts every env value and re-encrypts with the active key.
// POST /api/v1/admin/secrets/re-encrypt
func (h *SecretsAdminHandler) ReEncrypt(w http.ResponseWriter, r *http.Request) {
	if h.encryptor == nil {
		http.Error(w, `{"error":"encryption is not configured"}`, http.StatusBadRequest)
		return
	}

	total, err := h.envSvc.ReEncryptAll(r.Context())
	if err != nil {
		log.Printf("secrets re-encrypt failed: %v", err)
		http.Error(w, `{"error":"re-encryption failed: `+err.Error()+`"}`, http.StatusInternalServerError)
		return
	}

	resp := map[string]interface{}{
		"re_encrypted": total,
		"status":       "ok",
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}
