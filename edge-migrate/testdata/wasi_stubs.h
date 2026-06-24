// Minimal C declarations for every WASI symbol the C `Transformer`
// emits, plus POSIX stubs for symbols the input fixture
// (`testdata/http_client.c`) references. These stubs are NOT linked
// — they exist solely so that `clang -fsyntax-only -Werror` can
// type-check the transformer's output without the wasi-sdk
// toolchain.
//
// The intent is regression-net only: the transformer should produce
// code that references only the WASI symbols declared below. Any
// symbol the transformer emits that is NOT declared here will
// surface as an implicit-function-declaration or
// undeclared-identifier error, and the e2e test will fail.
//
// Keep this file in lockstep with `edge-migrate-lib/src/transformer.rs`
// — every new WASI symbol emitted by the transformer needs a
// matching declaration here. The end-to-end test
// (`test_transform_e2e_wasi_stubs_compile` in `transformer.rs`) is
// what catches drift.
//
// File layout:
//   1. POSIX stubs (input fixture needs these to type-check)
//   2. wasi/sockets.h — regression net for socket-related emits
//   3. wasi/io/streams.h — regression net for stream-related emits
//   4. wasi/filesystem.h — regression net for file I/O emits
//   5. wasi/ip-name-lookup.h — wasi_socket_tcp_accept companion type
#pragma once

// ---------------------------------------------------------------------------
// 1. POSIX stubs (input fixture references, NOT WASI symbols)
// ---------------------------------------------------------------------------
//
// The MVP leaves some POSIX calls verbatim in the source: `accept()`
// (downgraded to NotTransformable per #128), `gethostbyname()`
// (downgraded per G3). The input fixture `testdata/http_client.c`
// also declares variables of POSIX types like `struct sockaddr_in`
// before the transformer rewrites them away. These stubs are here
// so the *input* source type-checks under clang BEFORE the
// transformer replaces the POSIX calls. They are NEVER linked and
// have no runtime behavior.

// Minimal POSIX socket-API stubs.
#define AF_INET 2
#define SOCK_STREAM 1
#define SOCK_DGRAM 2

struct sockaddr {
  unsigned short sa_family;
  char sa_data[14];
};

struct sockaddr_in {
  unsigned short sin_family;
  unsigned short sin_port;
  struct in_addr {
    unsigned int s_addr;
  } sin_addr;
  char sin_zero[8];
};

// POSIX `accept()`. Downgraded to NotTransformable per #128 (the
// poll-loop wrapper was syntactically wrong and referenced an
// undeclared `pollable`). Typed as `void *` because the
// surrounding transformer rewrites `int fd = socket(...)` to
// `wasi_socket_tcp_t *fd = wasi_socket_tcp_create(...)`, so the
// verbatim accept() call now passes a pointer where POSIX would
// have passed an int.
int accept(void *fd, void *addr, int *addrlen);

// POSIX `gethostbyname()`. Downgraded to NotTransformable per G3
// — the runtime's `edge:cloud/networking.resolve(string) ->
// list<string>` shape doesn't match
// `wasi:ip-name-lookup.resolve-address`. Returns NULL; no real
// resolution happens; this is a syntax-check-only stub.
struct hostent {
  char *h_name;
};
struct hostent *gethostbyname(const char *name);

// ---------------------------------------------------------------------------
// 2. wasi/sockets.h — regression net
// ---------------------------------------------------------------------------
//
// Every WASI symbol the transformer's Socket* / Bind / Listen /
// Connect / Accept / Close emits must be declared here. The
// pre-fix Accept emit referenced `wasi_poll_pollable_block` and an
// undeclared `pollable`; neither appears here, and the e2e test
// would fail if a future regression re-introduced them.

#define IP_ADDRESS_FAMILY_IPV4 0

typedef struct wasi_socket_tcp_t wasi_socket_tcp_t;
typedef struct wasi_socket_udp_t wasi_socket_udp_t;

wasi_socket_tcp_t *wasi_socket_tcp_create(int family);
wasi_socket_udp_t *wasi_socket_udp_create(int family);

void wasi_socket_tcp_start_bind(wasi_socket_tcp_t *fd, const void *addr);
void wasi_socket_tcp_finish_bind(wasi_socket_tcp_t *fd);
void wasi_socket_tcp_start_listen(wasi_socket_tcp_t *fd, int backlog);
void wasi_socket_tcp_finish_listen(wasi_socket_tcp_t *fd);
void wasi_socket_tcp_start_connect(wasi_socket_tcp_t *fd, const void *addr);
void wasi_socket_tcp_finish_connect(wasi_socket_tcp_t *fd);
void wasi_socket_tcp_close(wasi_socket_tcp_t *fd);
void wasi_socket_udp_close(wasi_socket_udp_t *fd);

#define WASI_SOCKET_TCP_ACCEPT_ERROR_WOULD_BLOCK 1

typedef struct {
  int tag;
  void *val; /* accepted socket in result.val */
} wasi_socket_tcp_accept_result_t;

wasi_socket_tcp_accept_result_t wasi_socket_tcp_accept(wasi_socket_tcp_t *fd);

// wasi_socket_close dispatches to the typed close based on the call site.
// The transformer always emits the untyped form.
void wasi_socket_close(void *fd);

// ---------------------------------------------------------------------------
// 3. wasi/io/streams.h — regression net
// ---------------------------------------------------------------------------

typedef struct wasi_input_stream_t wasi_input_stream_t;
typedef struct wasi_output_stream_t wasi_output_stream_t;

int wasi_input_stream_read(wasi_input_stream_t *fd, void *buf, int len);
int wasi_output_stream_write(wasi_output_stream_t *fd, const void *buf,
                             int len);

// ---------------------------------------------------------------------------
// 4. wasi/filesystem.h — regression net
// ---------------------------------------------------------------------------

typedef struct wasi_filesystem_file_t wasi_filesystem_file_t;

wasi_filesystem_file_t *wasi_filesystem_open(const char *path, const char *mode);
int wasi_filesystem_read(wasi_filesystem_file_t *fd, void *buf, int len);
int wasi_filesystem_write(wasi_filesystem_file_t *fd, const void *buf, int len);
void wasi_filesystem_close(wasi_filesystem_file_t *fd);

// ---------------------------------------------------------------------------
// 5. wasi/ip-name-lookup.h — regression net
// ---------------------------------------------------------------------------

// Declared for completeness. The pre-G3 gethostbyname emit
// referenced this type. After G3 the emit is suppressed
// (gethostbyname → NotTransformable; see
// `edge-migrate/docs/design.md`), so this declaration is unused
// by the transformer today, but the `<wasi/ip-name-lookup.h>`
// include is still dropped (see `WASI_INCLUDES` in
// `edge-migrate-lib/src/transformer.rs`). Kept here for the case
// where a future host impl lands and the emit shape comes back.
typedef struct wasi_ip_name_lookup_t wasi_ip_name_lookup_t;
