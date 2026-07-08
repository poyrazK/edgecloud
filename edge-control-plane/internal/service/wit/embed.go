// Package wit embeds the canonical edge-cloud WIT tree so the control
// plane's MigrationService can run `wit_bindgen::generate!` against
// it without depending on a runtime filesystem path. The canonical
// source of truth is the top-level wit/ directory at the repo root
// (the same tree promoted to top-level by PR #414 and consumed by
// samples/hello, edge-js-runtime, and edge-worker/test fixtures).
// This package is the vendored copy; the wit-drift-check CI job
// (see .github/workflows/ci.yml) fails the build if the two diverge.
//
// The MigrationService constructor materializes the embedded FS to a
// per-process tmp dir at startup; the absolute path is the `path:`
// argument rust-bindgen reads when compiling a migrated component.
package wit

import (
	"embed"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
)

//go:embed edge-cloud.wit
//go:embed deps/cli/command.wit
//go:embed deps/cli/environment.wit
//go:embed deps/cli/exit.wit
//go:embed deps/cli/imports.wit
//go:embed deps/cli/run.wit
//go:embed deps/cli/stdio.wit
//go:embed deps/cli/terminal.wit
//go:embed deps/clocks/monotonic-clock.wit
//go:embed deps/clocks/timezone.wit
//go:embed deps/clocks/wall-clock.wit
//go:embed deps/clocks/world.wit
//go:embed deps/filesystem/preopens.wit
//go:embed deps/filesystem/types.wit
//go:embed deps/filesystem/world.wit
//go:embed deps/http/handler.wit
//go:embed deps/http/proxy.wit
//go:embed deps/http/types.wit
//go:embed deps/io/error.wit
//go:embed deps/io/poll.wit
//go:embed deps/io/streams.wit
//go:embed deps/io/world.wit
//go:embed deps/random/insecure-seed.wit
//go:embed deps/random/insecure.wit
//go:embed deps/random/random.wit
//go:embed deps/random/world.wit
//go:embed deps/sockets/instance-network.wit
//go:embed deps/sockets/ip-name-lookup.wit
//go:embed deps/sockets/network.wit
//go:embed deps/sockets/tcp-create-socket.wit
//go:embed deps/sockets/tcp.wit
//go:embed deps/sockets/udp-create-socket.wit
//go:embed deps/sockets/udp.wit
//go:embed deps/sockets/world.wit
var fsys embed.FS

// Materialize writes the embedded WIT tree to a fresh directory and
// returns the absolute path. The caller owns the returned dir and
// must os.RemoveAll(it) on shutdown (the dir is per-process and
// survives for the process lifetime; for a stable operator-known
// path, use the override from EDGE_WIT_DIR).
//
// The tmp dir is created with the pattern "edge-cp-wit-*" in the
// platform's default temp location (TMPDIR on macOS, /tmp on Linux).
// tmpreaper and equivalent utilities leave it alone because the
// process pid is still alive.
func Materialize() (string, error) {
	dir, err := os.MkdirTemp("", "edge-cp-wit-*")
	if err != nil {
		return "", fmt.Errorf("wit.Materialize: mkdir: %w", err)
	}
	if err := copyFS(fsys, dir); err != nil {
		_ = os.RemoveAll(dir)
		return "", fmt.Errorf("wit.Materialize: copy: %w", err)
	}
	return dir, nil
}

// copyFS walks the embedded FS and writes every file under dst.
// It refuses to write outside dst (defense against any future
// embed.FS that includes ".." or absolute paths in its keys).
func copyFS(src fs.FS, dst string) error {
	return fs.WalkDir(src, ".", func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			if path == "." {
				return nil
			}
			return os.MkdirAll(filepath.Join(dst, path), 0o755)
		}
		return copyFile(src, dst, path)
	})
}

func copyFile(src fs.FS, dst, path string) error {
	in, err := src.Open(path)
	if err != nil {
		return fmt.Errorf("open %s: %w", path, err)
	}
	defer in.Close()
	out, err := os.OpenFile(filepath.Join(dst, path), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("create %s: %w", path, err)
	}
	defer out.Close()
	if _, err := io.Copy(out, in); err != nil {
		return fmt.Errorf("write %s: %w", path, err)
	}
	return nil
}
