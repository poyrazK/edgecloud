package service

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/domain"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/repository"
)

// ErrInvalidLevel is returned by LogService when the requested minimum level
// is not one of the canonical {trace, debug, info, warn, error}. The handler
// maps this to a 400; tests pin the mapping.
var ErrInvalidLevel = errors.New("invalid log level")

// DefaultLogLimit is the limit applied when the caller does not specify one.
// Picked to be a useful single screenful (≈ one second of verbose output) and
// small enough that a follow-on --follow tick can stream a meaningful new
// batch without immediately re-hitting the default ceiling.
const DefaultLogLimit = 100

// MaxLogLimit caps the limit at the service layer so a hostile or buggy
// client cannot request an unbounded result. 1000 is enough to span a full
// minute of bursty output; tenants needing more should paginate via since
// (issue #77 deferred offset for v1).
const MaxLogLimit = 1000

// DefaultLogSince is the time window applied when the caller omits `since`.
// Five minutes matches the worker's LogForwarder flush interval closely, so
// "edge logs myapp" on a freshly-running app returns the most recent batch.
const DefaultLogSince = 5 * time.Minute

// LogLevelOrdinal maps a level string to its canonical numeric rank. Higher
// = more severe. Unknown levels return -1 — callers should treat that as
// ErrInvalidLevel rather than silently matching "everything".
func LogLevelOrdinal(level string) int {
	switch level {
	case "trace":
		return 0
	case "debug":
		return 1
	case "info":
		return 2
	case "warn":
		return 3
	case "error":
		return 4
	default:
		return -1
	}
}

// canonicalLogLevels is the ordered set of levels LogLevelOrdinal recognizes.
// Exposed as a package var so tests can iterate it without hardcoding the
// list twice.
var canonicalLogLevels = []string{"trace", "debug", "info", "warn", "error"}

// LevelsAtOrAbove returns the subset of canonicalLogLevels whose ordinal is
// >= min's ordinal. An empty result means the caller asked for everything
// (min == "trace" — the lowest severity). minLevel must be one of the
// canonical strings; pass-through of an unknown level is a programming error
// (the handler validates first via LogLevelOrdinal).
func LevelsAtOrAbove(minLevel string) []string {
	min := LogLevelOrdinal(minLevel)
	if min < 0 {
		return nil
	}
	out := make([]string, 0, len(canonicalLogLevels)-min)
	for _, l := range canonicalLogLevels {
		if LogLevelOrdinal(l) >= min {
			out = append(out, l)
		}
	}
	return out
}

// LogQuery is the validated, defaulted shape the handler hands the service.
// All fields are required (no pointers / no zero ambiguity) so the service
// layer can never accidentally query for "everything in the table".
//
// Levels is set by the service as a side-effect of MinLvl and is what the
// repository consumes (level = ANY($N::text[])). Exposed here so callers
// that need to inspect the post-translation filter (tests, observability)
// can do so without re-implementing LevelsAtOrAbove.
//
// #709 / #682 follow-up — `Offset` retired. Cursor is the only pagination
// input.
type LogQuery struct {
	Since  time.Duration
	Until  time.Time
	MinLvl string // canonical level string; "" = no filter
	Levels []string
	Limit  int
	Cursor string
}

// LogListResult wraps the visible entries, effective limit, and the
// single cursor hint. Post-#709 there is no `NextOffset` field.
type LogListResult struct {
	Entries    []domain.LogEntry
	Limit      int
	Since      time.Duration
	NextCursor *string
}

// LogEntryLister is the repository subset LogService consumes. Defined locally
// so tests can stub it without a live DB.
type LogEntryLister interface {
	ListByTenantApp(ctx context.Context, tenantID, appName string, filter repository.LogListFilter) ([]domain.LogEntry, error)
}

// LogService serves the read path for issue #77: a tenant asks for the last N
// log entries of one of their apps since some timestamp. It applies defaults
// and clamps so a malformed client request cannot turn into an unbounded
// DB query.
type LogService struct {
	repo LogEntryLister
}

func NewLogService(repo LogEntryLister) *LogService {
	return &LogService{repo: repo}
}

// ResolveLimit is the canonical limit clamp, exported so the handler can
// echo the post-clamp value in the response envelope without re-implementing
// the policy. Single source of truth — adding a new caller or a new bucket
// means editing exactly one place.
//
//	≤0    → DefaultLogLimit
//	>max  → MaxLogLimit
//	else  → requested unchanged
func ResolveLimit(requested int) int {
	switch {
	case requested <= 0:
		return DefaultLogLimit
	case requested > MaxLogLimit:
		return MaxLogLimit
	default:
		return requested
	}
}

// ListByTenantApp validates and normalizes q, then forwards to the repository.
// It fetches one extra row so pagination hints are emitted only when another
// page actually exists.
func (s *LogService) ListByTenantApp(
	ctx context.Context, tenantID, appName string, q LogQuery,
) (*LogListResult, error) {
	if q.MinLvl != "" && LogLevelOrdinal(q.MinLvl) < 0 {
		return nil, fmt.Errorf("%w: %q", ErrInvalidLevel, q.MinLvl)
	}

	since := q.Since
	if since <= 0 {
		since = DefaultLogSince
	}
	limit := ResolveLimit(q.Limit)

	levels := []string(nil)
	if q.MinLvl != "" {
		levels = LevelsAtOrAbove(q.MinLvl)
	}

	var cursorTS time.Time
	var cursorID int64
	if q.Cursor != "" {
		var err error
		cursorTS, cursorID, err = decodeLogCursor(q.Cursor)
		if err != nil {
			return nil, err
		}
		// Reject the silent-empty case: if the caller supplied
		// `until` AND the cursor points at a row whose ts is
		// strictly after until, the strict-tuple predicate and
		// the `ts <= until` clause will both filter the result
		// down to nothing. The client almost certainly intended
		// either a different cursor or a different `until`;
		// returning a typed error lets the handler surface a
		// friendly 400 instead of an empty page that looks like
		// "no more rows".
		if !q.Until.IsZero() && cursorTS.After(q.Until) {
			return nil, fmt.Errorf("%w: cursor ts %s is after until %s",
				ErrInvalidLogCursor, cursorTS.UTC().Format(time.RFC3339Nano),
				q.Until.UTC().Format(time.RFC3339Nano))
		}
	}

	entries, err := s.repo.ListByTenantApp(ctx, tenantID, appName, repository.LogListFilter{
		Since:    since,
		Until:    q.Until,
		Levels:   levels,
		Limit:    limit + 1,
		CursorTS: cursorTS,
		CursorID: cursorID,
	})
	if err != nil {
		return nil, err
	}

	hasMore := len(entries) > limit
	if hasMore {
		entries = entries[:limit]
	}

	var nextCursor *string
	if hasMore {
		last := entries[len(entries)-1]
		cursor, err := encodeLogCursor(last.TS, last.ID)
		if err != nil {
			return nil, err
		}
		nextCursor = &cursor
	}

	return &LogListResult{
		Entries:    entries,
		Limit:      limit,
		Since:      since,
		NextCursor: nextCursor,
	}, nil
}
