package service

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

var (
	// ErrInvalidLogCursor is returned when a log cursor cannot be decoded or
	// does not contain a valid timestamp and row ID.
	ErrInvalidLogCursor = errors.New("invalid log cursor")
	// ErrUnsupportedLogCursorVersion is returned when the cursor was produced
	// by a newer, unsupported cursor contract.
	ErrUnsupportedLogCursorVersion = errors.New("unsupported log cursor version")
)

const logCursorVersion = 1

type logCursor struct {
	Version int       `json:"v"`
	TS      time.Time `json:"ts"`
	ID      int64     `json:"id"`
}

func encodeLogCursor(ts time.Time, id int64) (string, error) {
	if ts.IsZero() || id <= 0 {
		return "", ErrInvalidLogCursor
	}
	payload, err := json.Marshal(logCursor{
		Version: logCursorVersion,
		TS:      ts.UTC(),
		ID:      id,
	})
	if err != nil {
		return "", fmt.Errorf("encoding log cursor: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(payload), nil
}

func decodeLogCursor(raw string) (time.Time, int64, error) {
	payload, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return time.Time{}, 0, ErrInvalidLogCursor
	}

	var cursor logCursor
	if err := json.Unmarshal(payload, &cursor); err != nil {
		return time.Time{}, 0, ErrInvalidLogCursor
	}
	if cursor.Version != logCursorVersion {
		return time.Time{}, 0, ErrUnsupportedLogCursorVersion
	}
	if cursor.TS.IsZero() || cursor.ID <= 0 {
		return time.Time{}, 0, ErrInvalidLogCursor
	}
	return cursor.TS.UTC(), cursor.ID, nil
}
