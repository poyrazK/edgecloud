package httperror

import (
	"errors"
	"net/http"
)

// MaxBodyBytes writes a JSON error response with the given status
// and message if err is a *http.MaxBytesError (the type returned
// by http.MaxBytesReader when the body cap is exceeded). Returns
// true if it wrote a response (caller should return), false
// otherwise. Use after every ParseMultipartForm (or io.ReadAll of
// a MaxBytesReader-wrapped body) to translate the cap into the
// user-facing response.
//
// The status and message are caller-supplied so per-endpoint
// wording and code (e.g. 413 "request body too large" for
// migration uploads vs. 400 "batch too large" for log ingests)
// can stay distinct without each handler re-implementing the
// errors.As + http.Error dance.
func MaxBodyBytes(w http.ResponseWriter, err error, status int, message string) bool {
	var maxErr *http.MaxBytesError
	if errors.As(err, &maxErr) {
		http.Error(w, `{"error":"`+message+`"}`, status)
		return true
	}
	return false
}
