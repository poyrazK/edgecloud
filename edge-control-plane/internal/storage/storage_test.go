package storage

import (
	"errors"
	"io"
	"strings"
	"testing"
)

func TestLimitReadCloser_UnderLimit(t *testing.T) {
	rc := io.NopCloser(strings.NewReader("hello"))
	l := newLimitReadCloser(rc, 100)

	buf := make([]byte, 10)
	n, err := l.Read(buf)
	if err != nil && err != io.EOF {
		t.Fatalf("Read: %v", err)
	}
	if n != 5 {
		t.Errorf("n = %d, want 5", n)
	}
	if string(buf[:n]) != "hello" {
		t.Errorf("content = %q, want 'hello'", string(buf[:n]))
	}
}

func TestLimitReadCloser_AtLimit(t *testing.T) {
	content := strings.Repeat("a", 100)
	rc := io.NopCloser(strings.NewReader(content))
	l := newLimitReadCloser(rc, 100)

	buf := make([]byte, 200)
	n, err := l.Read(buf)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if n != 100 {
		t.Errorf("n = %d, want 100", n)
	}
	// Subsequent read should get io.EOF (underlying exhausted at exactly cap)
	n, err = l.Read(buf)
	if n != 0 || err != io.EOF {
		t.Errorf("after exact cap: n=%d err=%v, want 0, EOF", n, err)
	}
}

func TestLimitReadCloser_ExceedsLimit(t *testing.T) {
	content := strings.Repeat("b", 150)
	rc := io.NopCloser(strings.NewReader(content))
	l := newLimitReadCloser(rc, 100)

	buf := make([]byte, 101) // read one more than cap
	n, err := l.Read(buf)
	if !errors.Is(err, ErrArtifactTooLarge) {
		t.Fatalf("err = %v, want ErrArtifactTooLarge", err)
	}
	if n != 0 {
		t.Errorf("n = %d, want 0 after exceed", n)
	}
}

func TestLimitReadCloser_SubsequentReadsAfterExceed(t *testing.T) {
	content := strings.Repeat("c", 200)
	rc := io.NopCloser(strings.NewReader(content))
	l := newLimitReadCloser(rc, 10)

	buf := make([]byte, 11)
	_, _ = l.Read(buf)

	// All subsequent reads must return ErrArtifactTooLarge
	buf2 := make([]byte, 1)
	n, err := l.Read(buf2)
	if !errors.Is(err, ErrArtifactTooLarge) {
		t.Errorf("subsequent read err = %v, want ErrArtifactTooLarge", err)
	}
	if n != 0 {
		t.Errorf("n = %d, want 0", n)
	}
}

func TestLimitReadCloser_ClosePropagates(t *testing.T) {
	closed := false
	rc := &closeTracker{Reader: strings.NewReader("data"), closed: &closed}
	l := newLimitReadCloser(rc, 100)

	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !closed {
		t.Error("underlying reader was not closed")
	}
}

type closeTracker struct {
	io.Reader
	closed *bool
}

func (c *closeTracker) Close() error {
	*c.closed = true
	return nil
}

func TestMaxArtifactSize_Constant(t *testing.T) {
	expected := int64(100 * 1024 * 1024)
	if MaxArtifactSize != expected {
		t.Errorf("MaxArtifactSize = %d, want %d", MaxArtifactSize, expected)
	}
}

func TestErrArtifactTooLarge_IsSentinel(t *testing.T) {
	if ErrArtifactTooLarge.Error() != "artifact exceeds maximum size" {
		t.Errorf("ErrArtifactTooLarge message = %q", ErrArtifactTooLarge.Error())
	}
}
