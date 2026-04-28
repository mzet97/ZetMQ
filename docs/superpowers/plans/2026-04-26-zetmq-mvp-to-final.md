# ZetMQ — MVP → Versao Final

> **Objetivo:** Transformar o broker MVP (in-memory, single-node) em um sistema production-ready com persistencia, cluster, seguranca e ecossistema completo de clientes.

**Estado Atual (MVP v0.1.0):**
- Broker in-memory, single-node, protocolo binario proprio (22-byte header)
- Pub/Sub com wildcards (`*`, `>`), Queue Groups (round-robin), Request/Reply
- Client SDK Rust (publish, subscribe, request, headers, unsubscribe)
- Backpressure: bounded channels + drop policy para slow consumers
- Performance: ~1M ops/s, zero-copy parsing, batched writes, subject cache
- Testes: pubsub, wildcard, backpressure, disconnect, client, headers, request-reply
- Observabilidade: tracing + atomic metrics

**Ambiente:** Windows 11 + WSL2, Rust 1.95+, Tokio async runtime

---

## Mapa de Fases

| Fase | Nome | Foco | Depende De | Prioridade |
|------|------|------|------------|------------|
| 1 | Hardening | Estabilidade, configuracao, graceful shutdown | MVP | ALTA |
| 2 | TLS + Auth | Seguranca de transporte e autenticacao | Fase 1 | ALTA |
| 3 | Persistencia (JetStream-like) | Message store, durable subs, replay | Fase 1 | ALTA |
| 4 | Cluster (Routing Mesh) | Multi-node, gossip, partitioning | Fase 2 | MEDIA |
| 5 | Monitoramento + Admin API | Health checks, metrics HTTP, management | Fase 1 | MEDIA |
| 6 | Client Reconnection + Resilience | Auto-reconnect, buffer, dedup | Fase 2 | MEDIA |
| 7 | WebSocket Gateway | Suporte a browsers e linguagens | Fase 2 | MEDIA |
| 8 | Ecosystem Clients | SDKs para Go, Python, JS/TS | Fase 7 | BAIXA |
| 9 | Advanced Features | Flow control, message encryption, schema registry | Fase 4 | BAIXA |
| 10 | Production Polish | Docs, CI/CD, releases, Docker, benchmarks finais | Fase 8 | BAIXA |

---

## Fase 1 — Hardening (Estabilidade)

**Objetivo:** Transformar o MVP em algo que roda 24/7 sem surpresas.

### 1.1 Graceful Shutdown Completo

- [ ] Signal handling robusto (SIGTERM, SIGINT, Ctrl+C)
- [ ] Drain de conexoes ativas: parar accepts, completar in-flight messages
- [ ] Session state machine: `Connected → Draining → Closed` com timeout
- [ ] Frame `DRAIN` (0x40): server sinaliza clients para reconectar
- [ ] Teste: graceful shutdown com N clientes conectados publicando

### 1.2 Sistema de Configuracao

- [ ] Suporte a arquivo TOML (`zetmq.conf`) com fallback para env vars
- [ ] Config: `port`, `host`, `max_connections`, `max_frame_size`, `output_buffer`, `tls`, `auth`, `cluster`, `store`
- [ ] Validacao de configuracao com erros claros
- [ ] Flag `--config` no CLI + `--validate`
- [ ] Reload de configuracao em runtime (SIGHUP) para params nao-destrutivos

### 1.3 Connection Limits e Protecao

- [ ] Limite maximo de conexoes simultaneas (default: 64K)
- [ ] Rate limiting por conexao: max PUB/s, max SUB/s
- [ ] Max subscriptions por conexao (default: 1024)
- [ ] Limite de payload maximo configuravel por conexao
- [ ] Reject de conexoes quando no limite com frame ERROR

### 1.4 Error Handling Robusto

- [ ] Error recovery no read loop: frame malformado nao derruba conexao
- [ ] Protocol error → ERROR frame → close (com grace period)
- [ ] Logging estruturado consistente (tracing spans por conexao)
- [ ] Crash-safe: panic em task nao derruba o servidor (abort on panic apenas em tests)

### 1.5 Testes de Robustez

- [ ] Teste: conexao com frame invalido → ERROR frame + close
- [ ] Teste: max connections atingido → reject com ERROR
- [ ] Teste: shutdown com publishers ativos → drain completo
- [ ] Teste: reconexao rapida do cliente → sem leak de subscriptions
- [ ] Fuzz testing: dados aleatorios no protocolo nao causam panic

**Entregavel:** Servidor estavel que roda sob carga sem crashes, com configuracao flexivel e shutdown limpo.

---

## Fase 2 — TLS + Autenticacao

**Objetivo:** Seguranca de transporte e controle de acesso.

### 2.1 TLS

- [ ] Suporte a TLS via `tokio-rustls` ou `tokio-native-tls`
- [ ] Config: `tls.cert_file`, `tls.key_file`, `tls.ca_file` (mutual TLS)
- [ ] Upgrade transparente: servidor aceita TLS e plain na mesma porta (STARTTLS-like) OU portas separadas
- [ ] Client SDK: opcao `tls: true` com cert verification
- [ ] Teste: publish/subscribe sobre TLS end-to-end
- [ ] Benchmark: overhead de TLS vs plain

### 2.2 Autenticacao

- [ ] **Token auth:** `CONNECT { token: "..." }` — valido contra config ou arquivo
- [ ] **Username/Password:** `CONNECT { user, pass }` — hash verification
- [ ] Config: `auth.users[]` com `username`, `password_hash`, `permissions`
- [ ] Rejeitar comandos antes de auth com ERROR
- [ ] Timeout de auth: se nao recebe CONNECT em N segundos, derruba conexao

### 2.3 Autorizacao (RBAC Basico)

- [ ] Permissions por usuario: `publish: ["subject_prefix.*"]`, `subscribe: ["subject_prefix.>"]`
- [ ] Match de permission com wildcard
- [ ] PUB com subject nao autorizado → ERROR + drop
- [ ] SUB com pattern nao autorizado → SUBACK error ou ERROR
- [ ] Config: `auth.users[].permissions.{publish, subscribe}`

### 2.4 Protocolo — CONNECT Ampliado

- [ ] CONNECT payload: `protocol_version(1) + flags(1) + [name_len(1) + name] + [token_len(2) + token] + [user_len(1) + user + pass_len(1) + pass]`
- [ ] CONNACK payload: `status(1) + [error_len(2) + error_msg]` (status: 0=OK, 1=auth_failed, 2=server_busy)
- [ ] Backward compatibility: CONNECT v1 sem auth campos funciona como antes

**Entregavel:** Broker com TLS + token/user auth + RBAC basico.

---

## Fase 3 — Persistencia (JetStream-like)

**Objetivo:** Durabilidade de mensagens, durable subscriptions, replay.

### 3.1 Storage Engine

- [ ] Abstracao `MessageStore` trait com implementacoes:
  - `MemoryStore` — in-memory (MVP atual, como fallback)
  - `FileStore` — append-only log com index (inspirado em bitcask)
- [ ] Stream: named collection of messages com configuracao (retention, max_msgs, max_bytes, max_age)
- [ ] Append: PUB com stream name → store no log
- [ ] Read por offset/sequence: O(1) lookup via index
- [ ] Retention policies: limits (max_msgs, max_bytes), age (TTL), interest (tem subscriber ativo)
- [ ] Compaction: garbage collect de mensagens expiradas

### 3.2 Durable Subscriptions

- [ ] SUB com flag `durable` → consumer state persistido
- [ ] Consumer: `pending` (ultimo ack), `delivered` (ultimo enviado), `ack_floor`
- [ ] Ack model: explicit ACK (novo frame `ACK 0x50`) ou `AckNone` (auto-ack)
- [ ] Deliver policy: `all`, `last`, `new`, `by_start_sequence`, `by_start_time`
- [ ] Replay: consumer reconecta e resume de onde parou
- [ ] Redelivery: mensagens nao-ack dentro do timeout sao reenviadas

### 3.3 Protocolo — Novos Frames

- [ ] `CREATE_STREAM 0x60` — criar stream com config
- [ ] `STREAM_INFO 0x61` — metadados do stream
- [ ] `DELETE_STREAM 0x62` — remover stream
- [ ] `ACK 0x50` — acknowledge de mensagem
- [ ] `NACK 0x51` — negative acknowledge
- [ ] `PUB` ampliado: flag `stream` no header ou frame separado `PUB_TO_STREAM`

### 3.4 Integracao com Broker

- [ ] Routing: PUB com stream → store + deliver para consumers
- [ ] Queue group + durable: cada membro tem consumer separado
- [ ] Flow control: PULL consumers pedem N mensagens explicitamente
- [ ] Metrics: `messages_stored`, `messages_acked`, `store_bytes`, `consumers`

### 3.5 Testes de Persistencia

- [ ] Teste: publish → restart broker → subscribe → recebe mensagens
- [ ] Teste: durable sub disconnect → reconnect → resume
- [ ] Teste: retention policy age → mensagens expiradas removidas
- [ ] Teste: compaction → tamanho do log reduz
- [ ] Benchmark: throughput com persistencia vs in-memory

**Entregavel:** Broker com persistencia opcional, durable subs, replay, ack model.

---

## Fase 4 — Cluster (Routing Mesh)

**Objetivo:** Multi-node para escalabilidade horizontal e alta disponibilidade.

### 4.1 Cluster Protocol

- [ ] Route connections: server-to-server TCP com protocolo de roteamento
- [ ] Gossip: informacao de topologia, subject interest propagation
- [ ] Config: `cluster.name`, `cluster.routes[]`, `cluster.port`
- [ ] Route handshake: `ROUTE` frame com server info + known subjects

### 4.2 Subject Interest Protocol

- [ ] Cada node mantem interesse local: quais subjects tem subscribers
- [ ] Interest propagation: ao SUB/UNSUB, node notifica peers
- [ ] Optimistic: PUB forward apenas para nodes com interesse conhecido
- [ ] Fallback: se interesse desconhecido, broadcast (com otimizacao gradual)

### 4.3 Message Routing

- [ ] PUB recebido → match local + forward para nodes remotos com interesse
- [ ] Fanout em cluster: dedup por message_id ou correlation_id
- [ ] Queue groups distribuidos: round-robin entre members em qualquer node
- [ ] Hop count: evitar loops de roteamento (max hops = cluster diameter)

### 4.4 Cluster Membership

- [ ] Auto-discovery: gossip protocol para encontrar peers
- [ ] Health check: heartbeat entre nodes (PING/PONG em route connection)
- [ ] Node failure detection: timeout → remove do routing table
- [ ] Rebalancing: subscribers redistribuidos quando node join/leave

### 4.5 Testes de Cluster

- [ ] Teste: 3-node cluster, pub em node A, sub em node B recebe
- [ ] Teste: queue group round-robin entre nodes
- [ ] Teste: node failure → messages redirecionados
- [ ] Teste: node join → interest propagation + routing atualizado
- [ ] Benchmark: throughput com cluster vs single-node

**Entregavel:** Cluster funcional com routing inteligente e failover basico.

---

## Fase 5 — Monitoramento + Admin API

**Objetivo:** Visibilidade operacional e gerenciamento em runtime.

### 5.1 HTTP Monitoring Server

- [ ] Porta HTTP separada (default: 8222) com endpoints REST-like
- [ ] `GET /healthz` — liveness probe
- [ ] `GET /readyz` — readiness probe
- [ ] `GET /metrics` — Prometheus-compatible metrics (text format)
- [ ] `GET /stats` — JSON com connections, subscriptions, messages, memory
- [ ] `GET /connections` — lista conexoes ativas com detalhes
- [ ] `GET /subscriptions` — lista subscriptions ativas
- [ ] `GET /routes` — cluster routes (se habilitado)

### 5.2 Prometheus Metrics

- [ ] `zetmq_connections_active` — gauge
- [ ] `zetmq_subscriptions_active` — gauge
- [ ] `zetmq_messages_published_total` — counter
- [ ] `zetmq_messages_delivered_total` — counter
- [ ] `zetmq_messages_dropped_total` — counter
- [ ] `zetmq_protocol_errors_total` — counter
- [ ] `zetmq_payload_bytes_total` — counter (bytes publicados)
- [ ] Histogramas: latency de publish-to-deliver

### 5.3 Admin API (Write)

- [ ] `POST /admin/shutdown` — graceful shutdown
- [ ] `POST /admin/drain` — drain all connections
- [ ] `DELETE /admin/connections/:id` — kick connection
- [ ] `DELETE /admin/subscriptions/:id` — force unsub
- [ ] Auth via Bearer token ou basic auth para admin

### 5.4 Serverz Dashboard

- [ ] `GET /` — HTML com overview visual (simple embedded page)
- [ ] Auto-refresh com SSE ou polling

**Entregavel:** Monitoramento completo com Prometheus + endpoints de management.

---

## Fase 6 — Client Resilience

**Objetivo:** Client SDK tolerante a falhas de rede e broker.

### 6.1 Auto-Reconnect

- [ ] Config: `reconnect: true`, `max_reconnects: 10`, `reconnect_delay: Initial(100ms) + Backoff(2s max)`
- [ ] Deteccao de disconnect: read returns 0 ou write error
- [ ] Reconnect loop com exponential backoff + jitter
- [ ] Flush buffer: mensagens publicadas durante reconnect sao enfileiradas
- [ ] Max buffer size para evitar OOM (configuravel)

### 6.2 Subscription Replay

- [ ] Re-sub automatico de subscriptions ativas apos reconnect
- [ ] Deteccao de duplicate messages (via message_id se disponivel)
- [ ] Ordered delivery guarantee por subscription

### 6.3 Connection Health

- [ ] Ping/Pong keepalive: client envia PING a cada interval (default: 30s)
- [ ] Se PONG nao volta em 2x interval → considera desconectado → trigger reconnect
- [ ] Config: `ping_interval`, `pong_timeout`

### 6.4 Error Recovery

- [ ] Publish com servidor down → buffer local ou erro imediato (configuravel)
- [ ] Server ERROR frame → client callback handler
- [ ] Async error channel: `client.on_error()` para notificar aplicacao

**Entregavel:** Client SDK que tolera falhas de rede com zero intervencao.

---

## Fase 7 — WebSocket Gateway

**Objetivo:** Suportar clientes browser e linguagens sem SDK nativo.

### 7.1 WebSocket Server

- [ ] Aceitar WebSocket connections na mesma porta (upgrade HTTP) ou porta separada
- [ ] Binary e text frames: binary para protocolo ZetMQ, text para JSON
- [ ] JSON protocol adapter: `{ "type": "pub", "subject": "...", "payload": "base64..." }`
- [ ] CORS configuravel

### 7.2 JSON Protocol Mapping

- [ ] `CONNECT` → `{ "type": "connect", "name": "..." }`
- [ ] `PUB` → `{ "type": "pub", "subject": "...", "payload": "<base64>", "headers": {...} }`
- [ ] `SUB` → `{ "type": "sub", "pattern": "..." }` → retorna `{ "type": "suback", "id": 42 }`
- [ ] `MSG` ← `{ "type": "msg", "subject": "...", "payload": "<base64>", "sub_id": 42 }`
- [ ] `UNSUB` → `{ "type": "unsub", "id": 42 }`
- [ ] `PING/PONG` → `{ "type": "ping" }` / `{ "type": "pong" }`

### 7.3 JavaScript/TypeScript Client

- [ ] NPM package: `zetmq`
- [ ] API: `connect()`, `publish()`, `subscribe()`, `request()`, `close()`
- [ ] Auto-reconnect com backoff
- [ ] Suporte a Node.js e browsers

**Entregavel:** Gateway WebSocket + client JS/TS funcional.

---

## Fase 8 — Ecosystem Clients

**Objetivo:** SDKs para as linguagens mais usadas.

### 8.1 Go Client

- [ ] Module: `github.com/zetmq/zetmq-go`
- [ ] API: `Connect()`, `Publish()`, `Subscribe()`, `Request()`, `Close()`
- [ ] Reconnect, keepalive, queue groups

### 8.2 Python Client

- [ ] Package: `zetmq` (pip)
- [ ] API: `Client.connect()`, `client.publish()`, `client.subscribe()` (async)
- [ ] Sync wrapper para uso simples

### 8.3 Client Compatibility Tests

- [ ] Suite de testes compartilhada que valida qualquer client
- [ ] Testes: connect, pub/sub, wildcards, queue groups, request/reply, headers, reconnect, TLS

**Entregavel:** 3+ SDKs oficiais com testes de compatibilidade.

---

## Fase 9 — Advanced Features

**Objetivo:** Features diferenciadas para casos de uso avancados.

### 9.1 Flow Control Avancado

- [ ] Per-subscription flow control: consumer pede N mensagens (PULL model)
- [ ] Backpressure feedback: consumer notifica broker sobre capacidade
- [ ] Priority queues: mensagens com prioridade no delivery

### 9.2 Message Encryption (End-to-End)

- [ ] Opcao de encryption no publish: broker nao ve o payload
- [ ] NaCl box encryption com chave compartilhada entre publisher e subscriber
- [ ] Key distribution via broker metadata (public keys)

### 9.3 Schema Registry (Lightweight)

- [ ] Validacao de schema no publish (JSON Schema, Protobuf)
- [ ] Schema storage no broker
- [ ] Config: `streams[].schema` com URL ou embedded schema

### 9.4 Message Transforms

- [ ] Transform functions por stream: header injection, subject rewrite
- [ ] Source/sink connectors: bridge para Kafka, RabbitMQ, Redis Pub/Sub

### 9.5 Geographic Clustering

- [ ] Leaf nodes: broker local conecta a hub remoto
- [ ] Gateway connections entre clusters
- [ ] Subject mapping entre clusters

**Entregavel:** Features avancadas para diferenciação competitiva.

---

## Fase 10 — Production Polish

**Objetivo:** Pronto para producao com confianca.

### 10.1 Documentacao

- [ ] Website com docs (mdbook ou similar): getting started, API reference, deployment guide
- [ ] rustdoc completo para todas as APIs publicas
- [ ] Tutoriais: pub/sub, request/reply, queue groups, persistence, cluster, TLS
- [ ] ADRs documentados para todas as decisoes arquiteturais

### 10.2 CI/CD

- [ ] GitHub Actions: test, lint, clippy, fmt check em PRs
- [ ] Cross-platform tests: Linux, macOS, Windows
- [ ] Release automation: `cargo publish` + GitHub release + changelog
- [ ] Security audit: `cargo audit` no CI

### 10.3 Docker + Deployment

- [ ] Dockerfile multi-stage otimizado (scratch ou distroless)
- [ ] Docker Compose: single broker + cluster example
- [ ] Helm chart para Kubernetes
- [ ] Systemd unit file

### 10.4 Benchmarks Finais

- [ ] Benchmark suite completo:
  - Single-node throughput (1, 10, 100 publishers)
  - Fanout (1→1, 1→10, 1→100 subscribers)
  - Wildcard matching overhead
  - Queue group round-robin overhead
  - TLS overhead
  - Persistence throughput
  - Cluster throughput (2, 3, 5 nodes)
  - Latency percentiles (p50, p95, p99, p99.9)
  - Memory usage under load
- [ ] Comparativo com NATS.io (mesmo cenario)
- [ ] Publicar resultados

### 10.5 Release

- [ ] Versao `1.0.0` com changelog completo
- [ ] Binarios pre-compilados para Linux (amd64, arm64), macOS, Windows
- [ ] Anuncio: blog post, Reddit, Rust community

**Entregavel:** ZetMQ 1.0 production-ready com documentacao, deployment e benchmarks publicos.

---

## Ordem de Execucao Recomendada

```
MVP (done) → Fase 1 (Hardening) → Fase 2 (TLS+Auth) → Fase 5 (Monitoring)
                                                  ↘
                                      Fase 3 (Persistence) → Fase 4 (Cluster)
                                                                ↘
                                       Fase 6 (Client Resilience) → Fase 7 (WebSocket)
                                                                                ↘
                                                       Fase 8 (SDKs) → Fase 9 (Advanced) → Fase 10 (Polish)
```

**Critical path:** Fases 1 → 2 → 3 → 4 (producao core)
**Paralelizavel:** Fase 5 pode comecar depois da Fase 1. Fases 6 e 7 podem rodar em paralelo com a Fase 4.
**Opcional:** Fases 8, 9 e 10 podem ser priorizadas conforme adocao.

---

## Metricas de Sucesso por Fase

| Fase | Metrica | Target |
|------|---------|--------|
| 1 | Uptime sob carga (continuous test) | 24h sem crash |
| 2 | TLS throughput vs plain | < 20% overhead |
| 3 | Persist throughput vs in-memory | < 40% overhead |
| 4 | Cluster throughput scale | ~linear ate 5 nodes |
| 5 | Prometheus scrape latency | < 5ms |
| 6 | Reconnect time | < 500ms |
| 7 | WebSocket latency vs TCP | < 1ms adicional |
| 10 | Latencia p99 single-node | < 100us |
