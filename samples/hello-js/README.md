# samples/hello-js

Minimal deployable [edgeCloud](https://github.com/poyrazK/edgecloud) FaaS
handler in JavaScript. For any inbound HTTP request it returns a small JSON
document:

```json
{"hello":"world","path":"/the/request/path","method":"GET"}
```

Built with [Javy](https://github.com/bytecodealliance/javy) v3.x, which
compiles JavaScript to a standard `wasm32-wasip2` Preview 2 component
that drops into the existing edgeCloud runtime — no runtime changes needed.

## Requirements

- [Javy](https://github.com/bytecodealliance/javy/releases) v3.x on `PATH`

## Build

```sh
cd samples/hello-js
javy compile -o target/javy/hello-js.wasm index.js
```

Or use the edgeCLI (which handles the `javy` invocation automatically):

```sh
cd samples/hello-js
edge build
```

## Deploy

```sh
edge deploy
```

## Layout

```
samples/hello-js/
├── edge.toml      # [project] name = "hello-js", language = "js"
├── index.js       # wasi:http/incoming-handler implementation
└── README.md      # this file
```
