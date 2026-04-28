# zetmq-server

ZetMQ message broker server — high-performance pub/sub with TLS and auth.

Part of [ZetMQ](https://github.com/mzet97/ZetMQ).

```bash
# Run with defaults
zetmq-server

# With config file
zetmq-server --config server.toml

# With TLS and authentication
zetmq-server --tls-cert cert.pem --tls-key key.pem --auth token --auth-token secret
```

Features: subject-based routing, queue groups, TLS, token/userpass auth, RBAC, persistence, graceful shutdown.
