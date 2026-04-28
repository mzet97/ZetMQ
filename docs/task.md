# task.md — Plano de Implementação SDD do ZetMQ

## 1. Objetivo do Documento

Este documento define o plano de execução do projeto **ZetMQ**, seguindo a metodologia **SDD — Specification-Driven Development / Software Design-Driven Development**.

O objetivo é quebrar a arquitetura definida em `architecture.md` em:

- fases;
- épicos;
- tarefas;
- subtarefas;
- dependências;
- critérios de aceite;
- testes obrigatórios;
- entregáveis;
- definição de pronto.

Este arquivo deve ser usado como guia operacional para implementação incremental do ZetMQ.

---

## 2. Premissas de Implementação

O projeto ZetMQ será implementado em Rust, do zero, com foco em:

- Pub/Sub;
- baixa latência;
- alta concorrência;
- arquitetura modular;
- código limpo;
- testabilidade;
- futura evolução para Queue Groups, Request/Reply, persistência e cluster.

O MVP não deve tentar resolver tudo.

O MVP deve entregar um broker funcional, testável e benchmarkável com:

- TCP server;
- protocolo próprio;
- conexão de clientes;
- publicação;
- assinatura;
- roteamento por subject;
- wildcard matching;
- entrega de mensagens;
- backpressure básico;
- graceful shutdown;
- métricas básicas.

---

## 3. Ordem Geral de Execução

A ordem correta de implementação é:

```text
1. Workspace e estrutura base
2. Tipos de domínio
3. Validação de subject
4. Routing engine
5. Subscription registry
6. Broker core
7. Protocolo de frames
8. Codec
9. Server TCP
10. Session lifecycle
11. Pub/Sub end-to-end
12. Backpressure
13. Observabilidade
14. Testes de integração
15. Benchmarks
16. Client SDK
17. Queue Groups
18. Request/Reply
19. Persistência
20. Cluster
```

Regra:

> Não implementar rede antes do core estar testado isoladamente.

---

## 4. Fase 0 — Preparação do Projeto

## EPIC 0 — Bootstrap do Workspace

### TASK 0.1 — Criar estrutura inicial do workspace

#### Objetivo

Criar o workspace Rust do ZetMQ com crates separados.

#### Subtarefas

- Criar pasta raiz `zetmq`.
- Criar `Cargo.toml` de workspace.
- Criar crates:
  - `zetmq-core`
  - `zetmq-protocol`
  - `zetmq-server`
  - `zetmq-client`
  - `zetmq-bench`
  - `zetmq-tests`
- Criar pasta `docs`.
- Mover `prompt.md`, `architecture.md` e `task.md` para `docs`.

#### Critérios de aceite

- `cargo check --workspace` executa.
- Todos os crates compilam, mesmo vazios.
- Workspace está organizado.
- Nenhum crate possui dependência desnecessária.

#### Testes

- Não aplicável nesta tarefa.

#### Definição de pronto

- Estrutura criada.
- `cargo check --workspace` sem erro.
- `README.md` inicial criado.

---

### TASK 0.2 — Configurar padrões de qualidade

#### Objetivo

Adicionar configuração mínima de qualidade de código.

#### Subtarefas

- Configurar `rustfmt`.
- Configurar `clippy`.
- Criar `.gitignore`.
- Criar comandos padrão no README.
- Definir política de `deny` ou `warn` para lints críticos.

#### Critérios de aceite

- `cargo fmt --check` executa.
- `cargo clippy --workspace` executa.
- `cargo test --workspace` executa.
- O projeto documenta os comandos básicos.

#### Testes

- Não aplicável.

#### Definição de pronto

- Comandos básicos documentados.
- Projeto preparado para CI futura.

---

### TASK 0.3 — Definir dependências iniciais

#### Objetivo

Adicionar somente as dependências necessárias para cada crate.

#### Dependências recomendadas

Para `zetmq-core`:

```toml
bytes
thiserror
dashmap
parking_lot
tracing
```

Para `zetmq-protocol`:

```toml
bytes
thiserror
```

Para `zetmq-server`:

```toml
tokio
tracing
tracing-subscriber
serde
toml
clap
thiserror
```

Para `zetmq-client`:

```toml
tokio
bytes
thiserror
```

Para `zetmq-bench`:

```toml
criterion
tokio
```

#### Critérios de aceite

- Cada dependência tem justificativa.
- Nenhuma dependência pesada é adicionada sem necessidade.
- Nenhum broker externo é usado.

---

## 5. Fase 1 — Domínio e Core In-Memory

## EPIC 1 — Tipos de Domínio

### TASK 1.1 — Criar IDs fortes

#### Objetivo

Evitar uso de tipos primitivos soltos para identificadores internos.

#### Tipos mínimos

```rust
ConnectionId
SubscriptionId
MessageId
QueueGroupId
```

#### Critérios de aceite

- IDs são tipos próprios.
- IDs implementam `Copy`, `Clone`, `Debug`, `Eq`, `Hash`.
- Não há mistura acidental entre IDs diferentes.

#### Testes

- Testar criação e comparação.
- Testar uso em `HashMap`.

---

### TASK 1.2 — Implementar `Subject`

#### Objetivo

Representar subject de publicação.

#### Regras

- Não pode ser vazio.
- Não pode conter token vazio.
- Não pode conter wildcard.
- Deve respeitar tamanho máximo.
- Deve ser tokenizado por ponto.

#### Exemplos válidos

```text
orders.created
user.updated
metrics.cpu.host01
```

#### Exemplos inválidos

```text
""
orders..created
orders.*
orders.>
.orders
orders.
```

#### Critérios de aceite

- `Subject::parse()` valida corretamente.
- Subject válido armazena representação interna eficiente.
- Subject inválido retorna erro tipado.

#### Testes obrigatórios

- Subject simples válido.
- Subject com múltiplos tokens.
- Subject vazio.
- Subject com token vazio.
- Subject com wildcard.
- Subject maior que limite.

---

### TASK 1.3 — Implementar `SubjectPattern`

#### Objetivo

Representar pattern de subscription.

#### Regras

- Pode conter `*`.
- Pode conter `>`.
- `*` representa exatamente um token.
- `>` representa um ou mais tokens restantes.
- `>` só pode aparecer no último token.
- Não pode conter token vazio.

#### Exemplos válidos

```text
orders.created
orders.*
orders.>
*.created
metrics.*.host01
```

#### Exemplos inválidos

```text
""
orders..created
orders.>.created
orders.created.>
```

Observação:

`orders.created.>` pode ser aceito ou rejeitado conforme regra escolhida. Para o MVP, definir que `>` representa um ou mais tokens; portanto `orders.created.>` só casa com `orders.created.anything`, não com `orders.created`.

#### Critérios de aceite

- Pattern válido é aceito.
- Pattern inválido retorna erro tipado.
- Wildcards são interpretados corretamente.

#### Testes obrigatórios

- Pattern exato.
- Pattern com `*`.
- Pattern com `>`.
- Pattern com `>` no meio deve falhar.
- Pattern vazio deve falhar.

---

### TASK 1.4 — Implementar `Message`

#### Objetivo

Representar a unidade de mensagem do broker.

#### Campos mínimos

```rust
subject: Subject
payload: Bytes
headers: HeaderMap
reply_to: Option<Subject>
timestamp_ns: u64
```

#### Critérios de aceite

- Payload usa `Bytes`.
- Message não copia payload desnecessariamente.
- Message pode ser clonada de forma barata quando necessário.

#### Testes obrigatórios

- Criar mensagem.
- Clonar mensagem.
- Validar que payload permanece íntegro.

---

## EPIC 2 — Routing Engine

### TASK 2.1 — Implementar matching exato

#### Objetivo

Roteamento rápido para subscriptions exatas.

#### Estrutura sugerida

```text
HashMap/ DashMap<Subject, Vec<SubscriptionId>>
```

#### Critérios de aceite

- Publicação em subject exato encontra subscriptions exatas.
- Publicação sem subscriptions retorna lista vazia.
- Matching exato não executa lógica de wildcard.

#### Testes obrigatórios

- Um subject com uma subscription.
- Um subject com várias subscriptions.
- Subject sem subscription.
- Isolamento entre subjects.

---

### TASK 2.2 — Implementar matching com `*`

#### Objetivo

Permitir wildcard de um token.

#### Exemplo

```text
Pattern: orders.*
Subject: orders.created
Resultado: match
```

#### Critérios de aceite

- `*` casa com exatamente um token.
- `*` não casa com zero tokens.
- `*` não casa com múltiplos tokens.

#### Testes obrigatórios

- `orders.*` casa com `orders.created`.
- `orders.*` não casa com `orders.created.high`.
- `orders.*` não casa com `orders`.

---

### TASK 2.3 — Implementar matching com `>`

#### Objetivo

Permitir wildcard de múltiplos tokens finais.

#### Exemplo

```text
Pattern: orders.>
Subject: orders.created.high
Resultado: match
```

#### Critérios de aceite

- `>` casa com um ou mais tokens restantes.
- `>` só é aceito no fim do pattern.
- `>` não deve gerar ambiguidade.

#### Testes obrigatórios

- `orders.>` casa com `orders.created`.
- `orders.>` casa com `orders.created.high`.
- `orders.>` não casa com `orders`.
- `orders.>.created` é inválido.

---

### TASK 2.4 — Implementar `RoutingEngine`

#### Objetivo

Concentrar registro, remoção e busca de subscriptions.

#### API sugerida

```rust
insert(pattern, subscription_id)
remove(pattern, subscription_id)
match_subject(subject) -> Vec<SubscriptionId>
```

#### Critérios de aceite

- Inserção funciona.
- Remoção funciona.
- Matching retorna subscriptions corretas.
- Não retorna duplicatas.
- Testável sem rede.

#### Testes obrigatórios

- Inserir e buscar.
- Remover e buscar.
- Exact + wildcard juntos.
- Duplicidade evitada.

---

## EPIC 3 — Subscription Registry

### TASK 3.1 — Implementar `Subscription`

#### Objetivo

Representar uma assinatura ativa.

#### Campos mínimos

```rust
id: SubscriptionId
connection_id: ConnectionId
pattern: SubjectPattern
queue_group: Option<QueueGroupName>
```

#### Critérios de aceite

- Subscription tem ownership claro.
- Subscription sabe a qual conexão pertence.
- Subscription pode ser removida por ID.

---

### TASK 3.2 — Implementar `SubscriptionRegistry`

#### Objetivo

Gerenciar subscriptions ativas.

#### Operações mínimas

```rust
add_subscription(...)
remove_subscription(...)
remove_all_for_connection(...)
get_subscription(...)
get_by_connection(...)
```

#### Critérios de aceite

- Registrar subscription.
- Remover subscription.
- Remover todas as subscriptions de uma conexão.
- Buscar subscriptions por conexão.
- Buscar subscription por ID.

#### Testes obrigatórios

- Add.
- Remove.
- Remove all by connection.
- Get by ID.
- Get by connection.
- Remoção inexistente retorna erro ou no-op documentado.

---

## EPIC 4 — Broker Core

### TASK 4.1 — Implementar `BrokerCore`

#### Objetivo

Criar o núcleo do broker sem dependência de TCP.

#### Operações mínimas

```rust
register_connection(connection_id, delivery_handle)
remove_connection(connection_id)
subscribe(connection_id, pattern, queue_group)
unsubscribe(connection_id, subscription_id)
publish(message)
```

#### Critérios de aceite

- Core registra conexões.
- Core registra subscriptions.
- Core publica mensagens.
- Core remove recursos ao desconectar.
- Core é testável com delivery handle fake.

---

### TASK 4.2 — Implementar `DeliveryHandle`

#### Objetivo

Abstrair entrega sem acoplar ao TCP.

#### Possível modelo

```rust
trait DeliveryHandle {
    fn deliver(&self, message: DeliveryMessage) -> DeliveryResult;
}
```

Ou usar canal interno concreto no MVP.

#### Decisão recomendada para MVP

Usar canal concreto inicialmente para reduzir abstração prematura.

#### Critérios de aceite

- Broker consegue enviar mensagens para conexão.
- Delivery falha de forma controlada.
- Falha de um subscriber não bloqueia outros.

---

### TASK 4.3 — Publicação end-to-end no core

#### Objetivo

Publicar mensagem no core e entregar a subscribers registrados.

#### Critérios de aceite

- Publicação sem subscribers não falha.
- Publicação com um subscriber entrega.
- Publicação com múltiplos subscribers entrega para todos.
- Wildcard funciona.
- Desconexão remove subscriptions.

#### Testes obrigatórios

- Publish sem subscriber.
- Publish com subscriber exato.
- Publish com wildcard.
- Publish após unsubscribe.
- Publish após disconnect.

---

## 6. Fase 2 — Protocolo

## EPIC 5 — Frame Protocol

### TASK 5.1 — Definir `FrameType`

#### Objetivo

Criar os tipos de frame do protocolo ZetMQ v1.

#### Tipos mínimos

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

#### Critérios de aceite

- Cada tipo tem representação numérica.
- Tipo desconhecido retorna erro.
- Conversão é testada.

---

### TASK 5.2 — Definir `FrameHeader`

#### Objetivo

Criar cabeçalho binário do frame.

#### Campos mínimos

```text
magic
version
frame_type
flags
correlation_id
header_len
payload_len
```

#### Critérios de aceite

- Header tem tamanho fixo.
- Header valida magic.
- Header valida version.
- Header valida payload length.
- Header valida frame size total.

---

### TASK 5.3 — Implementar `Frame`

#### Objetivo

Representar frame completo.

#### Campos mínimos

```rust
header: FrameHeader
headers: Bytes
payload: Bytes
```

#### Critérios de aceite

- Frame suporta payload binário.
- Frame suporta headers opcionais.
- Frame pode ser serializado e desserializado.

---

### TASK 5.4 — Implementar Encoder

#### Objetivo

Converter `Frame` em bytes.

#### Critérios de aceite

- Encoder gera bytes no formato definido.
- Encoder respeita endianess documentada.
- Encoder não aloca excessivamente.
- Encoder tem testes unitários.

---

### TASK 5.5 — Implementar Decoder

#### Objetivo

Converter bytes em `Frame`.

#### Critérios de aceite

- Decoder suporta frame parcial.
- Decoder rejeita frame inválido.
- Decoder rejeita magic inválido.
- Decoder rejeita versão incompatível.
- Decoder rejeita payload acima do limite.
- Decoder não faz panic com input externo.

#### Testes obrigatórios

- Frame completo.
- Frame parcial.
- Dois frames no mesmo buffer.
- Magic inválido.
- Tipo inválido.
- Payload acima do limite.
- Header incompleto.

---

### TASK 5.6 — Mapear Frames para Commands

#### Objetivo

Converter frames em comandos internos.

#### Comandos mínimos

```rust
ConnectCommand
PublishCommand
SubscribeCommand
UnsubscribeCommand
PingCommand
```

#### Critérios de aceite

- Frame `PUB` vira `PublishCommand`.
- Frame `SUB` vira `SubscribeCommand`.
- Payload inválido retorna erro.
- Conversão não depende de TCP.

---

## 7. Fase 3 — Servidor TCP

## EPIC 6 — Network Server

### TASK 6.1 — Implementar configuração do servidor

#### Objetivo

Carregar configuração do ZetMQ.

#### Fontes

- Defaults.
- Arquivo TOML.
- Variáveis de ambiente.
- CLI opcional.

#### Critérios de aceite

- Config padrão funciona.
- Config inválida falha no boot.
- Porta e host configuráveis.
- Limites são aplicados.

---

### TASK 6.2 — Implementar TCP Listener

#### Objetivo

Aceitar conexões TCP.

#### Critérios de aceite

- Server inicia em host/porta configurados.
- Aceita múltiplas conexões.
- Falha se porta estiver ocupada.
- Loga início e fim.

---

### TASK 6.3 — Implementar Session

#### Objetivo

Controlar o ciclo de vida da conexão.

#### Estados

```text
New
Connected
Draining
Closing
Closed
```

#### Critérios de aceite

- Nova conexão inicia em `New`.
- `CONNECT` válido muda para `Connected`.
- Comando inválido antes de connect retorna erro.
- Desconexão limpa recursos.

---

### TASK 6.4 — Implementar Read Loop

#### Objetivo

Ler bytes do socket e decodificar frames.

#### Critérios de aceite

- Lê frames completos.
- Suporta frames parciais.
- Encaminha comandos para dispatcher.
- Trata erro de protocolo.
- Trata desconexão.

---

### TASK 6.5 — Implementar Write Loop

#### Objetivo

Enviar frames para cliente.

#### Critérios de aceite

- Recebe mensagens da outbound queue.
- Codifica frames.
- Escreve no socket.
- Encerra corretamente quando canal fecha.
- Aplica backpressure pelo tamanho da fila.

---

### TASK 6.6 — Implementar Command Dispatcher

#### Objetivo

Conectar session, protocolo e broker core.

#### Critérios de aceite

- `CONNECT` chama registro de conexão.
- `SUB` chama subscribe.
- `UNSUB` chama unsubscribe.
- `PUB` chama publish.
- `PING` responde `PONG`.
- Erros viram frame `ERROR`.

---

## 8. Fase 4 — Pub/Sub End-to-End

## EPIC 7 — Integração Servidor + Core + Protocolo

### TASK 7.1 — Fluxo CONNECT end-to-end

#### Critérios de aceite

- Cliente conecta via TCP.
- Envia `CONNECT`.
- Recebe `CONNACK`.
- Broker registra conexão.

#### Testes

- Integração com socket real.
- CONNECT inválido.
- Comando antes de CONNECT.

---

### TASK 7.2 — Fluxo PING/PONG end-to-end

#### Critérios de aceite

- Cliente envia `PING`.
- Broker responde `PONG`.
- Funciona antes ou depois de CONNECT, conforme regra documentada.

---

### TASK 7.3 — Fluxo SUB end-to-end

#### Critérios de aceite

- Cliente envia `SUB`.
- Broker registra subscription.
- Cliente recebe `SUBACK`.

---

### TASK 7.4 — Fluxo PUB/MSG end-to-end

#### Critérios de aceite

- Cliente A assina subject.
- Cliente B publica no subject.
- Cliente A recebe `MSG`.
- Payload permanece íntegro.

---

### TASK 7.5 — Fluxo UNSUB end-to-end

#### Critérios de aceite

- Cliente assina subject.
- Cliente cancela subscription.
- Cliente não recebe mensagens futuras.

---

### TASK 7.6 — Wildcards end-to-end

#### Critérios de aceite

- `orders.*` funciona via TCP.
- `orders.>` funciona via TCP.
- Patterns inválidos retornam `ERROR`.

---

## 9. Fase 5 — Backpressure e Slow Consumers

## EPIC 8 — Backpressure

### TASK 8.1 — Fila de saída limitada por conexão

#### Objetivo

Impedir crescimento infinito de memória.

#### Critérios de aceite

- Cada conexão tem limite configurável.
- Ao atingir limite, política é aplicada.
- Métrica de slow consumer é incrementada.

---

### TASK 8.2 — Política `DisconnectSlowConsumer`

#### Objetivo

Desconectar cliente lento.

#### Critérios de aceite

- Cliente lento é detectado.
- Conexão é encerrada.
- Subscriptions são removidas.
- Broker continua funcional.

---

### TASK 8.3 — Teste de subscriber lento

#### Objetivo

Validar comportamento sob backpressure.

#### Critérios de aceite

- Criar subscriber que não lê mensagens.
- Publicar mensagens até lotar fila.
- Verificar desconexão ou erro esperado.
- Verificar limpeza de resources.

---

## 10. Fase 6 — Observabilidade

## EPIC 9 — Logs e Métricas

### TASK 9.1 — Configurar `tracing`

#### Critérios de aceite

- Logs estruturados.
- Log level configurável.
- Logs de conexão, publicação, erro e shutdown.

---

### TASK 9.2 — Implementar métricas internas

#### Métricas mínimas

```text
active_connections
total_connections
active_subscriptions
messages_published
messages_delivered
messages_dropped
protocol_errors
slow_consumers
bytes_received
bytes_sent
```

#### Critérios de aceite

- Métricas usam contadores atômicos.
- Métricas não introduzem lock crítico.
- Métricas são testáveis.

---

### TASK 9.3 — Endpoint ou dump de métricas

#### Opções

- Dump no log.
- Comando admin futuro.
- Endpoint HTTP futuro.

#### MVP

Começar com dump interno/log e API programática.

---

## 11. Fase 7 — Testes

## EPIC 10 — Testes Unitários

### TASK 10.1 — Testes de domínio

Cobrir:

- `Subject`
- `SubjectPattern`
- `Message`
- IDs fortes
- erros

---

### TASK 10.2 — Testes de routing

Cobrir:

- exact match;
- wildcard `*`;
- wildcard `>`;
- remoção de subscription;
- duplicidade;
- grande número de subscriptions.

---

### TASK 10.3 — Testes de protocolo

Cobrir:

- encode;
- decode;
- frame parcial;
- frame inválido;
- payload acima do limite;
- versionamento;
- frame type desconhecido.

---

### TASK 10.4 — Testes de broker core

Cobrir:

- connect;
- disconnect;
- subscribe;
- unsubscribe;
- publish;
- delivery;
- falha de delivery.

---

## EPIC 11 — Testes de Integração

### TASK 11.1 — Teste TCP CONNECT

#### Critérios de aceite

- Sobe servidor em porta dinâmica.
- Cliente conecta.
- Cliente recebe CONNACK.

---

### TASK 11.2 — Teste TCP Pub/Sub

#### Critérios de aceite

- Dois clientes reais.
- Um assina.
- Outro publica.
- Subscriber recebe.

---

### TASK 11.3 — Teste TCP Wildcard

#### Critérios de aceite

- Subscriber em `orders.*`.
- Publisher em `orders.created`.
- Entrega ocorre.

---

### TASK 11.4 — Teste de desconexão

#### Critérios de aceite

- Cliente assina e desconecta.
- Broker remove subscription.
- Publicações futuras não tentam entregar a conexão morta.

---

## 12. Fase 8 — Benchmarks

## EPIC 12 — Benchmark Suite

### TASK 12.1 — Benchmark de routing

#### Mede

- exact match;
- wildcard `*`;
- wildcard `>`;
- número crescente de subscriptions.

#### Saídas

- tempo por match;
- throughput de matches/s.

---

### TASK 12.2 — Benchmark de pub/sub local

#### Mede

- latência p50;
- latência p90;
- latência p99;
- throughput;
- payload sizes diferentes.

Payloads:

```text
16 B
128 B
1 KB
16 KB
1 MB
```

---

### TASK 12.3 — Benchmark de fanout

#### Mede entrega para:

```text
1 subscriber
10 subscribers
100 subscribers
1000 subscribers
```

---

### TASK 12.4 — Benchmark de publishers concorrentes

#### Cenários

```text
1 publisher
5 publishers
10 publishers
50 publishers
100 publishers
```

---

### TASK 12.5 — Benchmark de slow consumer

#### Mede

- tempo até detectar;
- mensagens antes de desconectar;
- uso de memória;
- impacto nos outros subscribers.

---

## 13. Fase 9 — Client SDK Rust

## EPIC 13 — ZetMQ Client

### TASK 13.1 — Implementar `Client::connect`

#### Critérios de aceite

- Conecta ao broker.
- Envia CONNECT.
- Aguarda CONNACK.
- Retorna erro se falhar.

---

### TASK 13.2 — Implementar `publish`

#### Critérios de aceite

- Publica subject + payload.
- Valida subject.
- Retorna erro de protocolo/rede.

---

### TASK 13.3 — Implementar `subscribe`

#### Critérios de aceite

- Envia SUB.
- Recebe SUBACK.
- Retorna stream/receiver de mensagens.
- Permite múltiplas subscriptions.

---

### TASK 13.4 — Implementar `unsubscribe`

#### Critérios de aceite

- Envia UNSUB.
- Recebe UNSUBACK.
- Fecha receiver local.

---

### TASK 13.5 — Implementar `close`

#### Critérios de aceite

- Encerra conexão.
- Fecha tasks internas.
- Libera recursos.

---

## 14. Fase 10 — Queue Groups

## EPIC 14 — Load Balancing por Grupo

### TASK 14.1 — Modelar `QueueGroupName`

#### Regras

- Nome não vazio.
- Tamanho máximo.
- Caracteres válidos documentados.

---

### TASK 14.2 — Registrar subscriptions com queue group

#### Critérios de aceite

- `SUB subject queue_group`.
- Registry armazena grupo.
- Routing separa fanout normal de grupo.

---

### TASK 14.3 — Implementar round-robin

#### Critérios de aceite

- Mensagens são distribuídas entre membros.
- Apenas um membro do grupo recebe cada mensagem.
- Subscribers fora do grupo recebem normalmente.

---

### TASK 14.4 — Testes de Queue Groups

Cobrir:

- 2 membros.
- 3 membros.
- membro desconectado.
- grupo + subscriber normal.
- wildcard + grupo.

---

## 15. Fase 11 — Request/Reply

## EPIC 15 — Semântica RPC-like

### TASK 15.1 — Suporte a `reply_to`

#### Critérios de aceite

- Mensagem pode ter reply subject.
- Broker preserva reply_to na entrega.

---

### TASK 15.2 — Inbox subjects

#### Critérios de aceite

- Client SDK gera inbox único.
- Inbox é assinado temporariamente.
- Resposta é recebida no inbox.

---

### TASK 15.3 — `Client::request`

#### Critérios de aceite

- Publica request.
- Aguarda uma resposta.
- Aplica timeout.
- Remove subscription temporária.

---

### TASK 15.4 — Testes Request/Reply

Cobrir:

- resposta normal;
- timeout;
- múltiplos requests concorrentes;
- replier desconectado.

---

## 16. Fase 12 — Persistência Futura

## EPIC 16 — Storage Design

Esta fase não entra no MVP, mas deve ser preparada.

### TASK 16.1 — Definir `zetmq-storage`

#### Objetivo

Criar fronteira para persistência futura.

#### Possíveis componentes

```text
LogWriter
LogReader
Segment
RetentionPolicy
MessageIndex
ConsumerOffset
```

---

### TASK 16.2 — Append-only log

#### Objetivo

Persistir mensagens por subject ou stream.

#### Critérios futuros

- Escrita sequencial.
- Segmentação.
- Flush configurável.
- Recovery no boot.

---

### TASK 16.3 — Durable subscriptions

#### Objetivo

Permitir que subscribers retomem mensagens.

#### Critérios futuros

- Offset por consumer.
- ACK.
- Redelivery.
- Dead-letter.

---

## 17. Fase 13 — Cluster Futuro

## EPIC 17 — Distributed ZetMQ

### TASK 17.1 — Definir modelo de nó

#### Campos

```text
node_id
node_address
cluster_address
status
last_seen
```

---

### TASK 17.2 — Definir membership

#### Opções

- gossip;
- static config;
- seed nodes.

---

### TASK 17.3 — Definir inter-node routing

#### Objetivo

Permitir que publicação em um nó chegue a subscriber em outro nó.

---

### TASK 17.4 — Definir replicação

#### Objetivo

Replicar metadados e mensagens persistentes.

---

## 18. Dependências entre Tarefas

Mapa simplificado:

```text
TASK 0.1 -> todas
TASK 1.2 -> TASK 2.x
TASK 1.3 -> TASK 2.x
TASK 2.x -> TASK 4.x
TASK 3.x -> TASK 4.x
TASK 4.x -> TASK 7.x
TASK 5.x -> TASK 6.x
TASK 6.x -> TASK 7.x
TASK 7.x -> TASK 8.x
TASK 7.x -> TASK 11.x
TASK 7.x -> TASK 12.x
TASK 13.x depende de TASK 7.x
TASK 14.x depende de TASK 13.x
TASK 15.x depende de TASK 13.x
```

---

## 19. Critérios Globais de Aceite do MVP

O MVP é aceito quando:

- Workspace compila.
- Core é testável sem rede.
- Protocol codec possui testes.
- Server TCP inicia.
- Cliente consegue conectar.
- Cliente consegue assinar.
- Cliente consegue publicar.
- Subscriber recebe mensagem.
- Wildcards funcionam.
- Unsubscribe funciona.
- Desconexão limpa recursos.
- Backpressure básico funciona.
- Métricas básicas existem.
- Testes unitários passam.
- Testes de integração passam.
- Benchmark inicial existe.
- `cargo fmt` passa.
- `cargo clippy` não apresenta warnings críticos.
- README documenta uso básico.

---

## 20. Definition of Done por Tarefa

Uma tarefa só é considerada pronta quando:

- Código implementado.
- Testes adicionados.
- Testes passando.
- Erros tratados.
- Sem `unwrap()` em código de produção.
- Sem `todo!()` em fluxo real.
- Sem acoplamento indevido.
- Documentação mínima atualizada.
- Critérios de aceite validados.

---

## 21. Definition of Done por Fase

Uma fase só é considerada pronta quando:

- Todas as tarefas da fase estão prontas.
- Testes da fase passam.
- Não há regressões.
- Arquitetura continua coerente com `architecture.md`.
- Decisões divergentes foram registradas.
- Benchmarks relevantes foram executados quando aplicável.

---

## 22. Políticas de Implementação

## 22.1 Política sobre `unwrap`

Proibido em código de produção.

Permitido somente em:

- testes;
- exemplos;
- inicialização controlada onde falha significa bug de programação, com justificativa.

---

## 22.2 Política sobre `unsafe`

Proibido no MVP.

Qualquer uso futuro exige ADR específico.

---

## 22.3 Política sobre dependências

Toda nova dependência deve responder:

```text
Por que é necessária?
Qual problema resolve?
Qual alternativa foi considerada?
Qual impacto no binário?
Qual impacto em performance?
Qual impacto em segurança?
```

---

## 22.4 Política sobre performance

Toda otimização complexa exige:

- benchmark antes;
- alteração;
- benchmark depois;
- comparação documentada.

---

## 23. Ordem Recomendada para Primeira Implementação Real

A primeira sequência de codificação deve ser:

```text
1. Criar workspace
2. Criar zetmq-core
3. Implementar IDs fortes
4. Implementar Subject
5. Implementar SubjectPattern
6. Implementar matching puro com função simples
7. Implementar RoutingEngine
8. Implementar SubscriptionRegistry
9. Implementar BrokerCore com delivery fake
10. Testar Pub/Sub sem rede
11. Criar zetmq-protocol
12. Implementar FrameType
13. Implementar FrameHeader
14. Implementar Encoder/Decoder
15. Criar zetmq-server
16. Implementar TCP listener
17. Integrar session + protocol + core
18. Fazer Pub/Sub via TCP
```

Essa ordem reduz risco porque valida o domínio antes da complexidade de rede.

---

## 24. Próximo Documento Recomendado

Após este `task.md`, o próximo arquivo recomendado é:

```text
protocol.md
```

O `protocol.md` deve especificar formalmente:

- formato binário;
- endianess;
- magic bytes;
- versionamento;
- frame types;
- flags;
- headers;
- payload;
- comandos;
- erros;
- exemplos de frames;
- limites;
- compatibilidade futura.
