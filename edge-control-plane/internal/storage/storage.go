package storage

import (
	"errors"
	"io"
)

// MaxArtifactSize caps the bytes a single Open can return, mirroring
// the upload cap in the migration handler. Stops a hostile S3
// response or an oversized on-disk file from OOM-ing the control
// plane when streamed into io.Copy.
const MaxArtifactSize = 100 * 1024 * 1024

// ErrArtifactTooLarge is returned by Open when the underlying body
// exceeds MaxArtifactSize. The handler maps it to HTTP 413.
var ErrArtifactTooLarge = errors.New("artifact exceeds maximum size")

// limitReadCloser caps reads from an underlying io.ReadCloser at n
// bytes total. Reads within budget pass through transparently
// (including io.EOF when the underlying reader is exhausted before
// the cap is reached). If the underlying reader still has data after
// n bytes have been read, every subsequent Read returns
// ErrArtifactTooLarge so the caller can distinguish "stream cut off
// because we hit the cap" from "stream ended cleanly".
//
// Implementation: wraps io.LimitReader at n+1, then on each Read
// checks whether the (n+1)th byte ever surfaces data. If we get a
// non-zero Read after n bytes have been consumed, the underlying
// source had more than n bytes and we mark the stream exceeded.
type limitReadCloser struct {
	rc       io.ReadCloser
	limiter  io.Reader
	cap      int64
	consumed int64
	exceeded bool
}

func newLimitReadCloser(rc io.ReadCloser, n int64) *limitReadCloser {
	return &limitReadCloser{
		rc:      rc,
		limiter: io.LimitReader(rc, n+1),
		cap:     n,
	}
}

func (l *limitReadCloser) Read(p []byte) (int, error) {
	if l.exceeded {
		return 0, ErrArtifactTooLarge
	}
	n, err := l.limiter.Read(p)
	l.consumed += int64(n)
	if l.consumed > l.cap && n > 0 {
		// We just read a byte past the cap. Set the flag; subsequent
		// reads return ErrArtifactTooLarge so the caller can stop.
		l.exceeded = true
		return 0, ErrArtifactTooLarge
	}
	return n, err
}

func (l *limitReadCloser) Close() error { return l.rc.Close() }
