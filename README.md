# ms-customer

Microsserviço de clientes (NestJS, Prisma, PostgreSQL, Kafka):

- **Clientes**: é o dono do `Customer`, com CPF validado e normalizado, contato
  e a lista de tipos de transporte autorizados. É exposto por HTTP e publica
  `customer.CustomerCreated` e `customer.CustomerUpdated` via Transactional
  Outbox.
- **Réplica de transporte**: consome `transport.TransportTypeCreated` e
  `transport.TransportTypeUpdated` (publicados pelo ms-transport) e mantém
  `transport_types_replica`. A réplica valida os transportes autorizados sem
  chamada síncrona ao ms-transport.

## Arquitetura

Camadas hexagonais, com dependências apontando para dentro:

- `src/domain`:
  - agregado `Customer` (create, restore e update; `isTransportAuthorized`);
  - value objects de CPF, email e telefone;
  - eventos `CustomerCreated` e `CustomerUpdated`, com snapshot completo;
  - erros de domínio (404, 409, 422).

  Não depende de Nest, Prisma nem Kafka.
- `src/application`:
  - use cases `CreateCustomer`, `UpdateCustomer`, `GetCustomer`,
    `ListCustomers` e `SyncTransportType`;
  - ports em `application/ports`: repositório de clientes, réplica de
    transporte, port de entrada do sync e DLT.
- `src/infrastructure`:
  - Prisma (repositórios e outbox);
  - Kafka (decoder Zod, consumer, DLT, producer);
  - HTTP, com Idempotency-Key copiado do ms-catalog;
  - métricas e health.

  A composição fica em `CustomerModule` (`useFactory`). Nenhum código é
  compartilhado com os outros serviços: o contrato é o formato do evento.

### Regras do cliente

Todas vêm do monólito `sales-order-api`.

- **CPF**:
  - aceito com ou sem máscara e normalizado para `XXX.XXX.XXX-XX`;
  - dígitos verificadores calculados por mod 11, com resto 10 valendo 0;
  - sequências repetidas são rejeitadas;
  - é único e imutável.
- **Telefone**: DDD mais 8 ou 9 dígitos, normalizado para `(XX) XXXX-XXXX` ou
  `(XX) XXXXX-XXXX`.
- **Email**: normalizado em minúsculas. No update, `null` limpa email e
  telefone.
- **`authorizedTransportTypeIds`**:
  - UUIDs sem repetição, no máximo 50;
  - armazenados ordenados;
  - no update, a lista substitui a anterior.
- **Transportes novos na lista**: precisam existir na réplica e estar ativos.
  Caso contrário, 422 `UnknownTransportTypeError`. Um transporte já autorizado
  que ficou inativo pode continuar na lista; quem impede o uso dele é a ordem
  de venda.
- **Update sem mudança**: não gera evento nem altera `updatedAt`.

## Eventos publicados

Envelope v2, com key = id do cliente e headers `eventType`, `schemaVersion` e
`correlationId`:

```json
{
  "eventId": "uuid", "eventType": "CustomerCreated", "schemaVersion": 2,
  "occurredAt": "ISO-8601", "aggregateId": "uuid", "correlationId": "...",
  "payload": { "id": "uuid", "name": "...", "document": "529.982.247-25",
               "authorizedTransportTypeIds": ["uuid"] }
}
```

O payload traz sempre o estado completo, para as réplicas guardarem só o último
estado. Email e telefone ficam de fora do evento, porque nenhum consumidor
precisa deles (minimização de dado pessoal). O ms-sales-order depende exatamente
desses campos.

## Consumo de `transport.*`

- **Group**: `ms-customer.transport-type-sync` (`TRANSPORT_SYNC_GROUP_ID`), com
  `fromBeginning: true`. Sem offset commitado, ele lê os tópicos inteiros e
  constrói a réplica.
- **Commit**: `autoCommit: false`. O `offset + 1` só é commitado depois da
  transação Prisma, do ack da DLT ou da detecção de duplicata.
- **Idempotência**: o insert em `processed_events` e o upsert na réplica rodam
  na mesma transação. Evento repetido é ignorado (`duplicate`).
- **Ordem**: Created e Updated chegam por tópicos diferentes, sem ordem
  garantida entre eles. O upsert só sobrescreve se o `occurredAt` for mais
  recente que o gravado. Um Created que chega depois do Updated é descartado
  (`stale`), e o estado final fica correto.
- **Contrato aceito**: só o envelope v2, com payload
  `{id, name, description, active}` e `payload.id === aggregateId`.

| Tipo de erro | Exemplos | Tratamento |
|---|---|---|
| Não recuperável | JSON inválido, schema Zod, `schemaVersion` diferente de 2, dado rejeitado pelo banco | DLT com os bytes originais; commit depois do ack |
| Recuperável | banco ou broker indisponível, timeout, erro não classificado | sem commit; retry em processo com backoff exponencial e jitter; esgotado, a partição é pausada por `CONSUMER_PAUSE_MS` |

### DLT

Cada serviço consumidor tem a sua DLT, com o nome
`<tópico>.ms-customer.DLT`. O ms-sales-order consome os mesmos tópicos
`transport.*`, e as mensagens mortas dos dois não podem se misturar.

| DLT | Origem |
|---|---|
| `transport.TransportTypeCreated.ms-customer.DLT` | `transport.TransportTypeCreated` |
| `transport.TransportTypeUpdated.ms-customer.DLT` | `transport.TransportTypeUpdated` |

- **Criação**: o `kafka-init` do ms-platform cria as DLTs. O consumer também
  as cria na subida, de forma idempotente e com retenção infinita, caso não
  existam.
- **Headers**:
  - `dlt-reason`, `dlt-detail`;
  - `dlt-source-topic`, `dlt-source-partition`, `dlt-source-offset`,
    `dlt-source-timestamp`;
  - `dlt-failed-at`;
  - `dlt-consumer-group`: o group que falhou;
  - além dos headers originais.

Para inspecionar:

```bash
docker compose -f ../ms-platform/docker-compose.yml exec kafka kafka-console-consumer \
  --bootstrap-server kafka:29092 --topic transport.TransportTypeCreated.ms-customer.DLT \
  --from-beginning --property print.headers=true
```

### Replay

A idempotência torna o replay seguro.

- **Opção 1: resetar o group principal**, com o serviço parado:

  ```bash
  docker compose -f ../ms-platform/docker-compose.yml exec kafka kafka-consumer-groups \
    --bootstrap-server kafka:29092 --group ms-customer.transport-type-sync \
    --all-topics --reset-offsets --to-earliest --execute
  ```

- **Opção 2: group temporário fixo**, por exemplo
  `TRANSPORT_SYNC_GROUP_ID=ms-customer.transport-type-sync.replay-20260928 yarn start`.
  Depois, apague o group com `kafka-consumer-groups --delete --group ...`.

## Como subir

Com a infraestrutura do [ms-platform](../ms-platform/README.md) no ar
(`docker compose up -d postgres kafka kafka-init kafka-ui`):

```bash
cp .env.example .env
yarn install
yarn prisma migrate deploy
yarn start
```

O projeto fixa Yarn 1 (`packageManager: yarn@1.22.22`, `yarn.lock` v1).

## Variáveis de ambiente

Validadas com Zod no boot; env inválida impede a subida com uma mensagem clara.
Veja `.env.example`.

| Variável | Padrão | Descrição |
|---|---|---|
| `DATABASE_URL` | obrigatória | database `customer` |
| `KAFKA_BROKER` | obrigatória | `host:porta`, separados por vírgula |
| `PORT` | 3003 | porta HTTP |
| `TRANSPORT_SYNC_GROUP_ID` | `ms-customer.transport-type-sync` | group da réplica; outro valor, só para replay |
| `CONSUMER_RETRY_RETRIES` / `_INITIAL_MS` / `_MAX_MS` | 5 / 300 / 30000 | retry em processo |
| `CONSUMER_PAUSE_MS` | 30000 | pausa da partição quando o retry se esgota |
| `OUTBOX_POLL_INTERVAL_MS` / `OUTBOX_BATCH_SIZE` | 2000 / 20 | publisher do outbox |
| `IDEMPOTENCY_TTL_HOURS` / `_LOCK_TIMEOUT_MS` / `_CLEANUP_INTERVAL_MS` | 24 / 30000 / 3600000 | Idempotency-Key |
| `THROTTLE_DEFAULT_TTL_MS` / `_LIMIT` | 60000 / 100 | rate limit |
| `HEALTH_CHECK_TIMEOUT_MS` / `SHUTDOWN_TIMEOUT_MS` | 1500 / 10000 | health e graceful shutdown |

## API HTTP

| Método | Rota | Descrição |
|---|---|---|
| `POST` | `/customers` | cria; `Idempotency-Key` opcional (mesmo corpo repete a resposta, corpo diferente dá 422). Respostas: 409 com CPF repetido, 422 com CPF ou transporte inválido, 400 com DTO inválido |
| `PUT` | `/customers/:id` | altera `name`, `email`, `phone` e `authorizedTransportTypeIds` (substitui a lista); o `document` é imutável |
| `GET` | `/customers` | lista paginada (`page`, `limit` até 100) com `total`, ordenada por `createdAt desc, id desc` |
| `GET` | `/customers/:id` | 404 se não existir |
| `GET` | `/health/live` e `/health/ready` | readiness verifica banco, broker e consumer |
| `GET` | `/metrics` | Prometheus; fica fora do throttler e não é roteado pelo gateway |

## Testes

```bash
yarn lint && yarn typecheck
yarn test              # unitários: CPF, telefone, entidade, use cases, decoder, consumer base, DLT
yarn test:integration  # Postgres e Kafka reais (testcontainers)
```

A integração cobre:

- réplica alimentada por `transport.*`, incluindo Updated antes do Created;
- poison message na DLT do serviço;
- cadastro com envelope, key e headers;
- transporte desconhecido ou inativo (422);
- CPF inválido (422 e 400) e CPF duplicado (409);
- Idempotency-Key;
- `CustomerUpdated`;
- `/metrics` e readiness.

## Limitações conhecidas

- **Consistência eventual.** Um tipo de transporte recém-criado no ms-transport
  pode dar 422 por alguns instantes, até chegar à réplica.
- **Unicidade dos ids autorizados** é garantida só pelo domínio e pelo DTO. O
  CHECK do banco não aceita subconsulta.
- **O replay republica na DLT** as mensagens não recuperáveis que já estavam lá.
- **O outbox não usa `SKIP LOCKED`.** Várias réplicas publicariam em
  duplicidade, o que é seguro para consumidores idempotentes.
- **Rate limit em memória, por réplica.**
- **Os serviços confiam no gateway.** Não há autenticação própria.
