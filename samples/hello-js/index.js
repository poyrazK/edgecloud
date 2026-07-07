// hello-js — built with edgeCloud (JavaScript via Javy).
//
// The runtime hands you a Fetch-style Request and expects a Response
// back. The `handle` named export is what `wasi:http/incoming-handler`
// calls per inbound request.
//
// For any inbound HTTP request it returns a small JSON document:
//   {"hello":"world","path":"/the/request/path","method":"GET"}

export async function handle(request) {
  const url = new URL(request.url);
  return new Response(JSON.stringify({
    hello: "world",
    path: url.pathname,
    method: request.method,
  }), {
    status: 200,
    headers: { "content-type": "application/json" },
  });
}
