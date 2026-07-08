# NATS Authentication

edgeCloud uses NATS as its message bus for task distribution and heartbeats.
This document describes how to configure authentication for production deployments.

## URL-Based Auth

The NATS client library supports embedding credentials in the connection URL:

```
nats://username:password@host:4222
nats://token@host:4222
```

Set via `NATS_URL` env var or `nats.url` in config.yaml:

```yaml
nats:
  url: "nats://my-token@nats-cluster:4222"
  replicas: 3
```

## Token Authentication

Create a NATS token:

```bash
nats server pass --token "my-secret-token"
```

Then configure the control plane and workers:

```yaml
nats:
  url: "nats://my-secret-token@nats-cluster:4222"
```

## Username/Password

Create a NATS user:

```bash
nats server pass --user "edgecloud" --pass "s3cret"
```

## TLS

For TLS connections, ensure the NATS URL uses the `tls` scheme:

```
nats://user:pass@nats-cluster:4222?tls=true
```

For mutual TLS, configure NATS server with certificates and use the URL with
the appropriate credentials.

## Production Recommendations

1. **Do not use open NATS in production** — always configure at least token auth
2. **Use a dedicated NATS user/service account** for the control plane and
   a separate one for workers (if supported by your NATS auth setup)
3. **Rotate NATS credentials** separately from JWT secrets — they are
   independent and should not share the same rotation cycle
