// JS handler fixture — returns JSON for GET /
// Used by edge-worker integration tests (L50+).

export async function handle(request) {
  const url = new URL(request.url);
  if (url.pathname === "/") {
    return new Response(JSON.stringify({ hello: "handler-js", path: "/" }), {
      status: 200,
      headers: { "content-type": "application/json" },
    });
  }
  return new Response(JSON.stringify({ error: "not found" }), {
    status: 404,
    headers: { "content-type": "application/json" },
  });
}
