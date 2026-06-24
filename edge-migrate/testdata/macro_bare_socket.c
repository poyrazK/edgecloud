/* Fixture: Bare macro-invoked socket call (no assignment).
 * The preprocessor expands `socket(...)` to `make_socket(...)`,
 * exposing the POSIX call shape to tree-sitter. Without expansion
 * the call is hidden behind the macro and not detected at all.
 *
 * The fixture's call has NO `int fd = ...` assignment, so the
 * match has `bound_var = None` — the call-site refinement (not the
 * bound_var refinement) is the path that takes effect here.
 *
 * NB: no #include <stdio.h> — we run with -nostdinc and stdio.h is
 * not available.
 */
#define socket(family, type, proto) make_socket(family, type, proto)
#define SOCK_STREAM 1
#define AF_INET 2

/* Forward declaration so clang does not warn on the expanded call.
 * The point of this fixture is the macro expansion, not the call. */
int make_socket(int family, int type, int proto);

int main(void) {
    /* Bare: no `int fd = ...` assignment. */
    socket(2, 1, 0);
    return 0;
}