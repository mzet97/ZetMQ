# architecture.md — Arquitetura Técnica do ZetMQ

## 1. Objetivo do Documento

Este documento define a arquitetura técnica do **ZetMQ**, um broker de mensagens distribuído escrito do zero em Rust, inspirado no modelo operacional do **NATS.io** e em conceitos de mensageria do **AMQP**.

O objetivo deste arquivo é transformar o `prompt.md` em uma arquitetura concreta, com:

- Visão geral do sistema.
- Separação de camadas.
- Crates e módulos.
- Modelo de domínio.
- Modelo de concorrência.
- Modelo de rede.
- Modelo de protocolo.
- Modelo de roteamento.
- Modelo de entrega.
- Modelo de backpressure.
- Modelo de observabilidade.
- Decisões arquiteturais.
- Riscos técnicos.
- Roadmap de evolução.

Este documento deve ser usado como contrato arquitetural antes da implementação.

---

## 2. Visão Geral

O **ZetMQ** será um broker de mensagens assíncrono, orientado a subjects, com conexões persistentes e entrega baseada em Pub/Sub.

A arquitetura inicial será **single-node**, in-memory e assíncrona, mas desenhada para evoluir para:

- Persistência.
- Queue Groups.
- Request/Reply.
- Durable subscriptions.
- Cluster.
- Replicação.
- Autenticação.
- Autorização.
- Observabilidade avançada.

A primeira versão deve priorizar:

- Baixa latência.
- Alto throughput.
- Simplicidade arquitetural.
- Testabilidade.
- Evolução incremental.
- Controle explícito do protocolo.

---

## 3. Princípios Arquiteturais

### 3.1 Separação de responsabilidades

Cada camada deve ter responsabilidade única.

A rede não deve conhecer detalhes internos de roteamento.

O roteador não deve conhecer TCP.

O protocolo não deve conhecer o broker.

O client SDK não deve acessar estruturas internas do servidor.

---

### 3.2 Core independente de transporte

O `zetmq-core` deve funcionar sem TCP.

Isso permite testar o broker internamente usando canais, mocks ou chamadas diretas.

Correto:

```text
BrokerCore recebe comandos de domínio.
BrokerCore não recebe TcpStream.
```

Incorreto:

```text
BrokerCore manipula socket TCP diretamente.
```

---

### 3.3 Protocolo independente do domínio

O protocolo deve transformar bytes em frames.

O domínio deve transformar comandos em ações.

Fluxo correto:

```text
Bytes TCP
  -> Frame protocolar
  -> Command de aplicação
  -> Operação no BrokerCore
```

---

### 3.4 Evolução incremental

O ZetMQ não deve tentar implementar cluster, persistência e segurança no primeiro MVP.

A ordem correta é:

1. Core Pub/Sub in-memory.
2. Protocolo estável.
3. Client SDK.
4. Queue Groups.
5. Request/Reply.
6. Persistência.
7. Cluster.
8. Segurança.
9. Operação avançada.

---

### 3.5 Performance orientada a medição

Nenhuma otimização complexa deve ser aceita sem benchmark.

Toda decisão de performance deve ser validada com:

- Latência p50.
- Latência p90.
- Latência p99.
- Throughput.
- Uso de CPU.
- Uso de memória.
- Contenção.

---

## 4. Visão Macro da Arquitetura

Arquitetura lógica:

```text
+------------------+
|   ZetMQ Client   |
+--------+---------+
         |
         | TCP / ZetMQ Protocol
         |
+--------v---------+
|  Network Layer   |
|  zetmq-server    |
+--------+---------+
         |
         | Frame -> Command
         |
+--------v---------+
|   Broker Core    |
|   zetmq-core     |
+--------+---------+
         |
         | Route
         |
+--------v---------+
| Routing Engine   |
+--------+---------+
         |
         | Match subscriptions
         |
+--------v---------+
| Subscription     |
| Registry         |
+--------+---------+
         |
         | Deliver
         |
+--------v---------+
| Outbound Queues  |
+--------+---------+
         |
         | Frame -> TCP
         |
+--------v---------+
| Subscribers      |
+------------------+
```

---

## 5. Workspace Rust

Estrutura inicial recomendada:

```text
zetmq/
  Cargo.toml
  README.md
  docs/
    prompt.md
    architecture.md
    protocol.md
    specification.md
    decisions/
      ADR-0001-protocol-binary.md
      ADR-0002-routing-trie.md
  crates/
    zetmq-core/
      Cargo.toml
      src/
        lib.rs
        broker/
        message/
        subject/
        routing/
        subscription/
        queue_group/
        delivery/
        error/
        metrics/
    zetmq-protocol/
      Cargo.toml
      src/
        lib.rs
        frame/
        codec/
        command/
        error/
        version/
    zetmq-server/
      Cargo.toml
      src/
        main.rs
        config/
        network/
        session/
        runtime/
        observability/
        shutdown/
        error/
    zetmq-client/
      Cargo.toml
      src/
        lib.rs
        client/
        connection/
        publisher/
        subscriber/
        request_reply/
        error/
    zetmq-bench/
      Cargo.toml
      benches/
        pubsub_latency.rs
        publish_throughput.rs
        fanout.rs
        routing_match.rs
    zetmq-tests/
      Cargo.toml
      tests/
        pubsub_integration.rs
        protocol_integration.rs
        reconnect_integration.rs
```

---

## 6. Crates

## 6.1 `zetmq-core`

### Responsabilidade

Contém o núcleo do broker.

Não deve depender de TCP, Tokio `TcpStream`, arquivo de configuração ou CLI.

### Deve conter

```text
BrokerCore
RoutingEngine
SubscriptionRegistry
Message
Subject
Subscription
Subscriber
QueueGroup
DeliveryPolicy
BackpressurePolicy
CoreMetrics
Domain errors
```

### Pode depender de

```text
bytes
thiserror
dashmap
parking_lot
crossbeam
tracing
```

### Não deve depender de

```text
tokio::net::TcpStream
clap
serde_json para protocolo de rede
hyper
axum
qualquer broker externo
```

---

## 6.2 `zetmq-protocol`

### Responsabilidade

Converter bytes em frames e frames em bytes.

### Deve conter

```text
Frame
FrameType
FrameHeader
FrameCodec
Command
ProtocolVersion
ProtocolError
Encoder
Decoder
```

### Não deve conhecer

```text
BrokerCore
SubscriptionRegistry
RoutingEngine
TcpListener
```

---

## 6.3 `zetmq-server`

### Responsabilidade

Executar o broker como processo de servidor.

### Deve conter

```text
Config loader
TCP listener
Connection accept loop
Session manager
Read loop
Write loop
Command dispatcher
Graceful shutdown
Observability setup
```

### Pode depender de

```text
zetmq-core
zetmq-protocol
tokio
tracing
serde
toml
clap
```

---

## 6.4 `zetmq-client`

### Responsabilidade

Fornecer SDK Rust para aplicações usarem o ZetMQ.

### Deve expor

```rust
Client::connect(...)
Client::publish(...)
Client::subscribe(...)
Client::unsubscribe(...)
Client::request(...)
Client::close(...)
```

### Não deve expor

- Detalhes binários do protocolo.
- Estruturas internas do servidor.
- Locks internos.
- Estado interno de sessão.

---

## 6.5 `zetmq-bench`

### Responsabilidade

Executar benchmarks controlados.

### Deve medir

- Latência.
- Throughput.
- Fanout.
- Matching de subjects.
- Backpressure.
- Uso aproximado de memória.
- Impacto de payload size.

---

## 7. Camadas da Arquitetura

## 7.1 Network Layer

### Responsabilidade

Gerenciar conexões TCP.

### Componentes

```text
TcpServer
ConnectionAcceptor
ConnectionSession
ReadLoop
WriteLoop
ConnectionId
OutboundQueue
```

### Fluxo

```text
TcpListener aceita conexão
  -> cria ConnectionId
  -> cria Session
  -> divide socket em read half e write half
  -> inicia read loop
  -> inicia write loop
```

### Regras

- Uma conexão não pode bloquear o accept loop.
- Uma conexão lenta não pode bloquear outras conexões.
- Toda conexão deve ter fila de saída limitada.
- Desconexão deve limpar subscriptions.

---

## 7.2 Protocol Layer

### Responsabilidade

Transformar bytes em frames e frames em bytes.

### Componentes

```text
FrameDecoder
FrameEncoder
FrameType
ProtocolError
ProtocolLimits
```

### Regras

- Validar tamanho máximo de frame.
- Validar versão.
- Validar tipo de frame.
- Rejeitar payloads inválidos.
- Nunca fazer panic por input externo.
- Ter testes com frames malformados.

---

## 7.3 Session Layer

### Responsabilidade

Representar o estado lógico de uma conexão.

### Estado mínimo

```text
ConnectionId
ClientInfo
Connected / Handshaked
Subscriptions owned by connection
Outbound sender
Last ping timestamp
Bytes in/out
```

### Estados possíveis

```text
New
Connected
Draining
Closing
Closed
```

### Regras

- Cliente inicia em `New`.
- Após `CONNECT` válido, vai para `Connected`.
- Durante shutdown, vai para `Draining`.
- Após encerrar recursos, vai para `Closed`.

---

## 7.4 Broker Core Layer

### Responsabilidade

Executar operações centrais de mensageria.

### Operações

```text
connect_client
disconnect_client
subscribe
unsubscribe
publish
route_message
deliver_message
register_queue_group
remove_connection_resources
```

### Regras

- Não conhecer TCP.
- Não fazer parsing de bytes.
- Não serializar protocolo.
- Receber comandos já validados parcialmente.
- Validar invariantes de domínio.

---

## 7.5 Routing Layer

### Responsabilidade

Resolver quais subscriptions recebem uma mensagem.

### Inputs

```text
Published subject
Subscription patterns
```

### Output

```text
Lista de delivery targets
```

### Regras

- Publicação não pode conter wildcard.
- Subscription pode conter wildcard.
- Matching deve ser determinístico.
- Resultado deve ser estável e testável.

---

## 7.6 Delivery Layer

### Responsabilidade

Entregar mensagens para filas de saída dos subscribers.

### Regras

- Fanout normal entrega para todos os subscribers compatíveis.
- Queue Group entrega para um membro do grupo.
- Falha em um subscriber não deve impedir entrega aos demais.
- Entrega deve respeitar backpressure.

---

## 7.7 Observability Layer

### Responsabilidade

Expor estado operacional.

### Métricas mínimas

```text
connections_active
connections_total
messages_published_total
messages_delivered_total
messages_dropped_total
protocol_errors_total
subscriptions_active
bytes_received_total
bytes_sent_total
```

### Logs mínimos

- Boot.
- Shutdown.
- Nova conexão.
- Desconexão.
- Erro de protocolo.
- Backpressure aplicado.
- Erro interno.

---

## 8. Modelo de Domínio

## 8.1 Message

Representa uma mensagem publicada.

Campos mínimos:

```rust
pub struct Message {
    pub subject: Subject,
    pub payload: Bytes,
    pub headers: HeaderMap,
    pub reply_to: Option<Subject>,
    pub timestamp_ns: u64,
}
```

### Decisão

Usar `Bytes` para payload.

### Justificativa

`Bytes` permite compartilhamento barato entre múltiplos subscribers sem copiar o conteúdo inteiro da mensagem.

---

## 8.2 Subject

Representa um destino lógico.

Exemplos:

```text
orders.created
orders.cancelled
metrics.cpu.host01
```

Regras:

- Não pode ser vazio.
- Não pode ter token vazio.
- Não pode exceder tamanho máximo.
- Publicação não aceita wildcard.
- Subscription aceita wildcard.

---

## 8.3 Subscription

Representa interesse em um subject pattern.

Campos mínimos:

```rust
pub struct Subscription {
    pub id: SubscriptionId,
    pub connection_id: ConnectionId,
    pub pattern: SubjectPattern,
    pub queue_group: Option<QueueGroupName>,
}
```

---

## 8.4 Subscriber

Representa um destino de entrega.

Campos mínimos:

```rust
pub struct Subscriber {
    pub connection_id: ConnectionId,
    pub subscription_id: SubscriptionId,
    pub outbound: DeliveryHandle,
}
```

---

## 8.5 Queue Group

Representa um grupo de balanceamento.

Exemplo:

```text
SUB orders.created workers
```

Significa:

- Todos assinam `orders.created`.
- Apenas um membro do grupo `workers` recebe cada mensagem.
- Algoritmo inicial: round-robin.

---

## 9. Modelo de Subject Matching

## 9.1 Subject exato

```text
orders.created
```

Casa apenas com:

```text
orders.created
```

---

## 9.2 Wildcard `*`

Representa exatamente um token.

```text
orders.*
```

Casa com:

```text
orders.created
orders.cancelled
```

Não casa com:

```text
orders.created.high_priority
orders
```

---

## 9.3 Wildcard `>`

Representa um ou mais tokens restantes.

```text
orders.>
```

Casa com:

```text
orders.created
orders.created.high_priority
orders.created.high_priority.eu
```

Não deve ser usado no meio do pattern.

Válido:

```text
orders.>
```

Inválido:

```text
orders.>.created
```

---

## 10. Estrutura de Dados para Roteamento

## 10.1 Opção recomendada para MVP

Usar combinação de:

```text
HashMap para exact match
Trie para wildcard match
```

### Por quê?

Exact match é o caminho crítico mais comum.

Wildcard match exige percorrer tokens.

Arquitetura:

```text
RoutingEngine
  ├── exact_subscriptions: DashMap<Subject, Vec<SubscriptionId>>
  └── wildcard_trie: SubjectTrie
```

---

## 10.2 SubjectTrie

Modelo lógico:

```text
root
 └── orders
      ├── created
      ├── *
      └── >
```

Cada nó pode conter:

```text
children: HashMap<Token, TrieNode>
single_wildcard: Option<TrieNode>
multi_wildcard_subscriptions: Vec<SubscriptionId>
subscriptions: Vec<SubscriptionId>
```

---

## 10.3 Estratégia de matching

Para publicar em:

```text
orders.created.high_priority
```

O router deve procurar:

1. Caminho exato.
2. Caminhos com `*`.
3. Caminhos com `>`.
4. Consolidar subscriptions.
5. Remover duplicatas.
6. Separar fanout normal de queue groups.

---

## 11. Modelo de Concorrência

## 11.1 Runtime

Usar Tokio.

Configuração:

```text
multi-thread runtime
worker_threads configurável
```

---

## 11.2 Por conexão

Cada conexão terá:

```text
Read task
Write task
Outbound channel
Session state
```

Modelo:

```text
Connection
  ├── read_loop
  ├── write_loop
  └── outbound_queue
```

---

## 11.3 Broker compartilhado

O broker será compartilhado com:

```rust
Arc<BrokerCore>
```

Internamente, o broker deve evitar um lock global único.

---

## 11.4 Canais

Usar canais para separar produtores e consumidores internos.

Exemplo:

```text
Session read loop
  -> CommandDispatcher
  -> BrokerCore
  -> DeliveryHandle
  -> Session write loop
```

---

## 12. Fluxo de Publicação

Fluxo completo:

```text
Cliente envia PUB
  -> TCP read loop recebe bytes
  -> ProtocolDecoder gera Frame::Pub
  -> Session valida estado
  -> CommandDispatcher converte para PublishCommand
  -> BrokerCore valida subject e payload
  -> RoutingEngine encontra subscriptions compatíveis
  -> DeliveryLayer cria MSG frame lógico
  -> Envia para outbound queues dos subscribers
  -> WriteLoop serializa MSG
  -> TCP envia para subscribers
```

---

## 13. Fluxo de Subscription

```text
Cliente envia SUB
  -> Decoder gera Frame::Sub
  -> Session valida estado
  -> BrokerCore cria Subscription
  -> SubscriptionRegistry registra subscription
  -> RoutingEngine indexa pattern
  -> Broker retorna SUBACK
  -> Session envia SUBACK ao cliente
```

---

## 14. Fluxo de Unsubscribe

```text
Cliente envia UNSUB
  -> Decoder gera Frame::Unsub
  -> Session valida ownership da subscription
  -> BrokerCore remove subscription
  -> SubscriptionRegistry remove
  -> RoutingEngine remove index
  -> Broker retorna UNSUBACK
```

---

## 15. Fluxo de Desconexão

```text
TCP fecha ou erro ocorre
  -> Session detecta disconnect
  -> BrokerCore remove conexão
  -> Remove subscriptions da conexão
  -> Remove queue group membership
  -> Fecha outbound queue
  -> Atualiza métricas
  -> Loga desconexão
```

---

## 16. Backpressure

## 16.1 Problema

Um subscriber lento pode acumular mensagens e consumir memória indefinidamente.

## 16.2 Solução MVP

Cada conexão terá fila de saída limitada.

Configuração:

```toml
connection_output_buffer = 1024
```

## 16.3 Políticas

### DisconnectSlowConsumer

Se a fila lotar, desconectar cliente lento.

Vantagem:

- Protege o broker.

Desvantagem:

- Cliente perde conexão.

### DropNewest

Descarta mensagem nova.

Vantagem:

- Mantém conexão.

Desvantagem:

- Perde mensagens.

### DropOldest

Remove mensagem antiga e insere nova.

Vantagem:

- Mantém dados recentes.

Desvantagem:

- Ordem pode ser prejudicada.

## 16.4 Política padrão

Para MVP:

```text
DisconnectSlowConsumer
```

Justificativa:

- Mais simples.
- Mais seguro.
- Mais parecido com brokers de baixa latência que preferem proteger o sistema.

---

## 17. Modelo de Protocolo

## 17.1 Estratégia

Usar protocolo binário próprio, baseado em frames.

Frame lógico:

```text
+----------------+----------------+
| Field          | Size           |
+----------------+----------------+
| Magic          | 2 bytes        |
| Version        | 1 byte         |
| Frame Type     | 1 byte         |
| Flags          | 2 bytes        |
| Correlation ID | 8 bytes        |
| Header Length  | 4 bytes        |
| Payload Length | 4 bytes        |
| Headers        | variable       |
| Payload        | variable       |
+----------------+----------------+
```

## 17.2 Tipos iniciais

```text
CONNECT
CONNACK
PING
PONG
PUB
MSG
SUB
SUBACK
UNSUB
UNSUBACK
ERROR
```

## 17.3 Versão

O protocolo deve ser versionado desde o início.

Primeira versão:

```text
ZetMQ Protocol v1
```

---

## 18. Modelo de Comandos Internos

Frames de protocolo devem ser convertidos para comandos internos.

Exemplo:

```rust
pub enum BrokerCommand {
    Connect(ConnectCommand),
    Publish(PublishCommand),
    Subscribe(SubscribeCommand),
    Unsubscribe(UnsubscribeCommand),
    Ping(PingCommand),
}
```

Justificativa:

- Evita acoplar protocolo ao domínio.
- Facilita testes.
- Permite outros transportes no futuro.

---

## 19. Modelo de Entrega

## 19.1 Fanout normal

Uma publicação em `orders.created` deve ser entregue a todos os subscribers compatíveis.

Exemplo:

```text
SUB orders.created client-a
SUB orders.*       client-b
SUB orders.>       client-c

PUB orders.created
```

Resultado:

```text
client-a recebe
client-b recebe
client-c recebe
```

---

## 19.2 Queue Group

Exemplo:

```text
SUB orders.created queue=workers client-a
SUB orders.created queue=workers client-b
SUB orders.created queue=workers client-c
```

Resultado para uma publicação:

```text
apenas um dos três recebe
```

Algoritmo inicial:

```text
round-robin por subject + queue group
```

---

## 20. Estado Interno do Broker

Estado mínimo:

```rust
pub struct BrokerCore {
    subscriptions: SubscriptionRegistry,
    router: RoutingEngine,
    connections: ConnectionRegistry,
    metrics: CoreMetrics,
}
```

Observação:

O tipo exato pode mudar, mas as responsabilidades devem permanecer separadas.

---

## 21. Identificadores

Usar identificadores fortes em vez de tipos primitivos soltos.

Exemplo:

```rust
pub struct ConnectionId(u64);
pub struct SubscriptionId(u64);
pub struct MessageId(u64);
```

Justificativa:

- Evita troca acidental de IDs.
- Melhora legibilidade.
- Facilita evolução futura.

---

## 22. Erros

## 22.1 Categorias

```text
ProtocolError
NetworkError
SessionError
BrokerError
RoutingError
SubscriptionError
ConfigError
ClientError
```

## 22.2 Regras

- Nunca usar `unwrap()` no caminho de produção.
- Nunca fazer panic por input externo.
- Erros enviados ao cliente devem ser sanitizados.
- Logs internos podem conter detalhes técnicos.
- Erros devem ser testáveis.

---

## 23. Configuração

Arquivo sugerido:

```toml
[server]
host = "127.0.0.1"
port = 4222
max_connections = 10000
connection_output_buffer = 1024
max_payload_bytes = 1048576

[protocol]
version = 1
max_frame_size = 2097152

[routing]
max_subject_tokens = 32
max_subject_length = 512

[backpressure]
policy = "disconnect_slow_consumer"

[observability]
log_level = "info"
metrics_enabled = true

[performance]
worker_threads = 0
```

---

## 24. Observabilidade

## 24.1 Logs

Usar `tracing`.

Eventos mínimos:

```text
server_started
server_stopped
connection_opened
connection_closed
client_connected
subscription_created
subscription_removed
message_published
message_delivered
protocol_error
slow_consumer_detected
```

## 24.2 Métricas

Métricas atômicas no MVP.

Exemplo:

```rust
pub struct CoreMetrics {
    pub active_connections: AtomicU64,
    pub total_connections: AtomicU64,
    pub active_subscriptions: AtomicU64,
    pub messages_published: AtomicU64,
    pub messages_delivered: AtomicU64,
    pub messages_dropped: AtomicU64,
    pub protocol_errors: AtomicU64,
}
```

---

## 25. Segurança Inicial

No MVP, segurança será mínima.

Deve existir:

- Limite de frame.
- Limite de payload.
- Limite de conexões.
- Limite de subject length.
- Limite de subscriptions por conexão.
- Validação de input externo.

Não entra no MVP:

- TLS.
- Autenticação.
- Autorização.
- ACL por subject.

Essas features devem ser planejadas para fases futuras.

---

## 26. Persistência

Persistência não entra no MVP.

Mas a arquitetura deve reservar fronteiras para ela.

Futuro crate possível:

```text
zetmq-storage
```

Responsabilidades futuras:

- Append-only log.
- Segmentos.
- Índices.
- Retention.
- Replay.
- Durable subscriptions.
- ACK/NACK.
- Redelivery.

A camada core não deve assumir que toda mensagem é efêmera.

---

## 27. Cluster

Cluster não entra no MVP.

Mas a arquitetura deve evitar decisões que impeçam cluster.

Futuro crate possível:

```text
zetmq-cluster
```

Responsabilidades futuras:

- Membership.
- Gossip.
- Routing entre nós.
- Leaf nodes.
- Gateway nodes.
- Replicação.
- Failover.

---

## 28. Decisões Arquiteturais Iniciais

## ADR-0001 — Usar Rust

### Decisão

Implementar o ZetMQ em Rust.

### Justificativa

Rust oferece segurança de memória, controle de baixo nível, bom suporte assíncrono e performance previsível.

### Trade-offs

- Curva de aprendizado maior.
- Mais rigor no modelo de ownership.
- Menos velocidade inicial que linguagens dinâmicas.

---

## ADR-0002 — Usar Tokio

### Decisão

Usar Tokio como runtime assíncrono.

### Justificativa

Tokio é o ecossistema assíncrono mais maduro em Rust para rede TCP de alta concorrência.

### Trade-offs

- Dependência forte do modelo async do Tokio.
- Exige cuidado com locks em contexto async.

---

## ADR-0003 — Usar protocolo binário próprio

### Decisão

Criar protocolo binário próprio baseado em frames.

### Justificativa

Permite controle de overhead, versionamento e suporte nativo a payload binário.

### Alternativas

- JSON por linha.
- RESP.
- Protobuf.
- Cap'n Proto.
- FlatBuffers.

### Trade-off

Protocolo próprio exige documentação e testes mais rigorosos.

---

## ADR-0004 — Separar core de network

### Decisão

O broker core não deve depender de TCP.

### Justificativa

Aumenta testabilidade e permite transportes futuros.

### Impacto

Facilita QUIC, Unix sockets e testes em memória no futuro.

---

## ADR-0005 — Usar `Bytes` para payload

### Decisão

Representar payload com `bytes::Bytes`.

### Justificativa

Permite clone barato e reduz cópias no fanout.

### Trade-off

Exige atenção com ciclo de vida e retenção de memória.

---

## ADR-0006 — Política padrão de slow consumer: desconectar

### Decisão

No MVP, cliente lento será desconectado quando sua fila de saída lotar.

### Justificativa

Protege memória e latência do broker.

### Trade-off

Cliente lento perde mensagens e precisa reconectar.

---

## 29. Riscos Técnicos

## 29.1 Contenção no registry de subscriptions

Risco:

- Muitos publishers e subscribers podem gerar contenção.

Mitigação:

- Separar exact match de wildcard.
- Usar sharding ou estruturas lock-free futuramente.
- Medir com benchmark.

---

## 29.2 Crescimento de memória por fanout

Risco:

- Muitos subscribers lentos podem aumentar memória.

Mitigação:

- Outbound queue limitada.
- Política de slow consumer.
- Métricas de fila.

---

## 29.3 Parser vulnerável a input malicioso

Risco:

- Cliente envia frame gigante ou malformado.

Mitigação:

- Limite de frame.
- Limite de payload.
- Decoder defensivo.
- Testes fuzz futuramente.

---

## 29.4 Complexidade de cluster

Risco:

- Cluster muda suposições do core.

Mitigação:

- Não acoplar core a nó único demais.
- Incluir origem da mensagem.
- Definir IDs fortes.
- Planejar metadados distribuídos.

---

## 29.5 Benchmarks enganosos

Risco:

- Medições artificiais podem induzir decisões erradas.

Mitigação:

- Separar microbenchmark de benchmark end-to-end.
- Registrar ambiente.
- Medir p50, p90, p99.
- Comparar com NATS e Redis Pub/Sub futuramente.

---

## 30. Roadmap Arquitetural

## Fase 1 — MVP In-Memory

Entregáveis:

- Workspace Rust.
- `zetmq-core`.
- `zetmq-protocol`.
- `zetmq-server`.
- TCP server.
- Pub/Sub.
- Subject routing.
- Wildcards.
- Backpressure básico.
- Testes.
- Benchmarks iniciais.

---

## Fase 2 — SDK e Semântica NATS-like

Entregáveis:

- `zetmq-client`.
- Request/Reply.
- Queue Groups.
- Reconnect.
- Flush.
- Timeout.
- Heartbeat.

---

## Fase 3 — Persistência

Entregáveis:

- `zetmq-storage`.
- Append-only log.
- Retention.
- Replay.
- Durable subscriptions.
- ACK/NACK.
- Redelivery.

---

## Fase 4 — Cluster

Entregáveis:

- `zetmq-cluster`.
- Node discovery.
- Gossip.
- Inter-node routing.
- Replication.
- Failover.

---

## Fase 5 — Operação Produção

Entregáveis:

- TLS.
- Auth.
- ACL por subject.
- Prometheus.
- Admin API.
- Dashboard.
- Tuning guide.
- Hardening guide.

---

## 31. Critérios de Aceite da Arquitetura

A arquitetura é aceita quando:

- O core é independente de TCP.
- O protocolo é independente do broker.
- O servidor apenas integra rede, protocolo e core.
- O client SDK não conhece detalhes internos do servidor.
- O roteamento é testável isoladamente.
- O backpressure está definido.
- O modelo de concorrência está explícito.
- Os riscos técnicos estão documentados.
- O roadmap é incremental.
- A estrutura permite persistência e cluster no futuro.

---

## 32. Próximo Documento

Após este `architecture.md`, o próximo arquivo deve ser:

```text
tasks.md
```

O `tasks.md` deve quebrar a implementação em épicos, fases, tarefas, subtarefas e critérios de aceite, seguindo a arquitetura definida aqui.
