import { execFileSync } from "node:child_process";
import { randomUUID } from "node:crypto";
import type { INestApplication } from "@nestjs/common";
import { KafkaContainer, type StartedKafkaContainer } from "@testcontainers/kafka";
import { PostgreSqlContainer, type StartedPostgreSqlContainer } from "@testcontainers/postgresql";
import { z } from "zod";
import type { PrismaService } from "@infrastructure/database/prisma/prisma.service";
import { sampleValue } from "../../src/test/metrics.helpers";
import { KafkaTestClient, waitFor } from "./kafka-test-client";

const CREATED = "transport.TransportTypeCreated";
const UPDATED = "transport.TransportTypeUpdated";
const CUSTOMER_CREATED = "customer.CustomerCreated";
const CUSTOMER_UPDATED = "customer.CustomerUpdated";
const TRUCK = randomUUID();
const BIKE = randomUUID();
const INACTIVE = randomUUID();
const REORDERED = randomUUID();
const CPF = "529.982.247-25";

function transportEvent(
  eventType: string,
  id: string,
  occurredAt: string,
  payload: { name: string; active: boolean },
): { key: string; value: string } {
  return {
    key: id,
    value: JSON.stringify({
      eventId: randomUUID(),
      eventType,
      schemaVersion: 2,
      occurredAt,
      aggregateId: id,
      correlationId: `corr-${id}`,
      payload: { id, description: null, ...payload },
    }),
  };
}

const customerSchema = z.object({
  id: z.uuid(),
  name: z.string(),
  document: z.string(),
  email: z.string().nullable(),
  phone: z.string().nullable(),
  authorizedTransportTypeIds: z.array(z.uuid()),
});

const envelopeSchema = z.object({
  eventId: z.uuid(),
  eventType: z.string(),
  schemaVersion: z.literal(2),
  occurredAt: z.iso.datetime(),
  aggregateId: z.uuid(),
  correlationId: z.string(),
  payload: z.object({
    id: z.uuid(),
    name: z.string(),
    document: z.string(),
    authorizedTransportTypeIds: z.array(z.uuid()),
  }).strict(),
});

interface JsonResponse {
  readonly status: number;
  readonly body: unknown;
  readonly headers: Headers;
}

// Postgres e Kafka reais: replica de transporte via Kafka, cadastro de
// cliente com outbox, Idempotency-Key e DLT.
describe("ms-customer: clientes e replica de transporte (integracao)", () => {
  let postgres: StartedPostgreSqlContainer;
  let kafkaContainer: StartedKafkaContainer;
  let kafka: KafkaTestClient;
  let app: INestApplication;
  let prisma: PrismaService;
  let baseUrl: string;

  const send = async (
    method: "POST" | "PUT" | "GET",
    path: string,
    body?: unknown,
    headers: Record<string, string> = {},
  ): Promise<JsonResponse> => {
    const response = await fetch(`${baseUrl}${path}`, {
      method,
      headers: { "content-type": "application/json", ...headers },
      body: body === undefined ? undefined : JSON.stringify(body),
    });
    return { status: response.status, body: await response.json(), headers: response.headers };
  };

  const replicaRow = (id: string): Promise<{ name: string; active: boolean } | null> =>
    prisma.transportTypeReplica.findUnique({ where: { transportTypeId: id }, select: { name: true, active: true } });

  beforeAll(async () => {
    [postgres, kafkaContainer] = await Promise.all([
      new PostgreSqlContainer("postgres:16-alpine").start(),
      new KafkaContainer("confluentinc/cp-kafka:7.6.1")
        .withKraft()
        .withEnvironment({ KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false" })
        .start(),
    ]);
    const broker = `${kafkaContainer.getHost()}:${kafkaContainer.getMappedPort(9093)}`;
    kafka = new KafkaTestClient(broker);
    for (const topic of [CREATED, UPDATED, CUSTOMER_CREATED, CUSTOMER_UPDATED]) await kafka.createTopic(topic);

    const databaseUrl = postgres.getConnectionUri();
    execFileSync("npx", ["prisma", "migrate", "deploy"], {
      env: { ...process.env, DATABASE_URL: databaseUrl },
      stdio: "pipe",
    });

    // Massa publicada antes do app subir (group novo, fromBeginning). REORDERED:
    // Updated chega antes do Created (topicos diferentes, sem ordem garantida).
    await kafka.produce(CREATED, [
      transportEvent("TransportTypeCreated", TRUCK, "2026-09-28T10:00:00.000Z", { name: "Caminhao", active: true }),
      transportEvent("TransportTypeCreated", BIKE, "2026-09-28T10:00:00.000Z", { name: "Moto", active: true }),
      transportEvent("TransportTypeCreated", INACTIVE, "2026-09-28T10:00:00.000Z", { name: "Carroca", active: false }),
      { key: "poison", value: "t" },
    ]);
    await kafka.produce(UPDATED, [
      transportEvent("TransportTypeUpdated", REORDERED, "2026-09-28T11:00:00.000Z", { name: "Van nova", active: false }),
    ]);
    await kafka.produce(CREATED, [
      transportEvent("TransportTypeCreated", REORDERED, "2026-09-28T10:00:00.000Z", { name: "Van", active: true }),
    ]);

    Object.assign(process.env, {
      NODE_ENV: "production",
      LOG_LEVEL: "error",
      DATABASE_URL: databaseUrl,
      KAFKA_BROKER: broker,
      OUTBOX_POLL_INTERVAL_MS: "200",
      CONSUMER_RETRY_RETRIES: "2",
      CONSUMER_RETRY_INITIAL_MS: "100",
    });

    const { NestFactory } = await import("@nestjs/core");
    const { AppModule } = await import("../../src/app.module");
    const { configureApp } = await import("../../src/app.setup");
    const { PrismaService: PrismaServiceToken } = await import("@infrastructure/database/prisma/prisma.service");
    app = await NestFactory.create(AppModule, { logger: false, abortOnError: false });
    configureApp(app);
    await app.listen(0);
    baseUrl = (await app.getUrl()).replace("[::1]", "localhost");
    prisma = app.get(PrismaServiceToken);
  });

  afterAll(async () => {
    await app?.close();
    await Promise.all([postgres?.stop(), kafkaContainer?.stop()]);
  });

  it("replica recebe os tipos de transporte; Updated antes do Created nao e sobrescrito", async () => {
    await waitFor("replica completa", async () => (await prisma.transportTypeReplica.count()) === 4);

    expect(await replicaRow(TRUCK)).toEqual({ name: "Caminhao", active: true });
    expect(await replicaRow(REORDERED)).toEqual({ name: "Van nova", active: false });
  });

  it("poison message em transport.* vai para a DLT do servico com motivo e group", async () => {
    const [dead] = await kafka.readFromBeginning(`${CREATED}.ms-customer.DLT`, 1, 30_000);

    expect(dead?.value).toBe("t");
    expect(dead?.headers).toMatchObject({
      "dlt-reason": "invalid_json",
      "dlt-source-topic": CREATED,
      "dlt-consumer-group": "ms-customer.transport-type-sync",
    });
  });

  let customerId = "";

  it("POST cria o cliente e publica CustomerCreated com envelope v2, key, headers e payload do contrato", async () => {
    const created = await send(
      "POST",
      "/customers",
      { name: "Ana", document: "52998224725", email: "Ana@X.com", phone: "11987654321", authorizedTransportTypeIds: [TRUCK] },
      { "x-correlation-id": "cust-corr-1" },
    );

    expect(created.status).toBe(201);
    const customer = customerSchema.parse(created.body);
    customerId = customer.id;
    expect(customer).toMatchObject({ document: CPF, email: "ana@x.com", phone: "(11) 98765-4321" });

    const [message] = await kafka.readFromBeginning(CUSTOMER_CREATED, 1, 30_000);
    expect(message?.key).toBe(customer.id);
    expect(message?.headers).toEqual({ eventType: "CustomerCreated", schemaVersion: "2", correlationId: "cust-corr-1" });
    const envelope = envelopeSchema.parse(JSON.parse(message?.value ?? ""));
    expect(envelope.payload).toEqual({ id: customer.id, name: "Ana", document: CPF, authorizedTransportTypeIds: [TRUCK] });
  });

  it("transporte desconhecido ou inativo: 422", async () => {
    const unknown = await send("POST", "/customers", { name: "Bia", document: "111.444.777-35", authorizedTransportTypeIds: [randomUUID()] });
    const inactive = await send("POST", "/customers", { name: "Bia", document: "111.444.777-35", authorizedTransportTypeIds: [INACTIVE] });

    expect(unknown.status).toBe(422);
    expect(unknown.body).toMatchObject({ error: "UnknownTransportTypeError" });
    expect(inactive.status).toBe(422);
  });

  it("CPF com digito errado: 422; campo invalido no DTO: 400; CPF duplicado: 409", async () => {
    const badCpf = await send("POST", "/customers", { name: "Bia", document: "529.982.247-24", authorizedTransportTypeIds: [] });
    const badDto = await send("POST", "/customers", { name: "Bia", document: CPF, authorizedTransportTypeIds: ["x"] });
    const duplicate = await send("POST", "/customers", { name: "Outra", document: CPF, authorizedTransportTypeIds: [] });

    expect(badCpf.status).toBe(422);
    expect(badDto.status).toBe(400);
    expect(duplicate.status).toBe(409);
    expect(duplicate.body).toMatchObject({ error: "CustomerDocumentAlreadyExistsError" });
  });

  it("Idempotency-Key: mesma chave e corpo devolvem a resposta original; corpo diferente 422", async () => {
    const body = { name: "Caio", document: "111.444.777-35", authorizedTransportTypeIds: [BIKE] };
    const headers = { "idempotency-key": "cust-key-1" };

    const first = await send("POST", "/customers", body, headers);
    const replay = await send("POST", "/customers", body, headers);
    const mismatch = await send("POST", "/customers", { ...body, name: "Outro" }, headers);

    expect(first.status).toBe(201);
    expect(replay.status).toBe(201);
    expect(replay.headers.get("idempotent-replayed")).toBe("true");
    expect(replay.body).toEqual(first.body);
    expect(mismatch.status).toBe(422);
    expect(await prisma.customer.count({ where: { document: "111.444.777-35" } })).toBe(1);
  });

  it("PUT com mudanca publica CustomerUpdated; GET por id, 404 e lista paginada", async () => {
    const updated = await send("PUT", `/customers/${customerId}`, { authorizedTransportTypeIds: [TRUCK, BIKE], email: null });
    const found = await send("GET", `/customers/${customerId}`);
    const missing = await send("GET", `/customers/${randomUUID()}`);
    const page = await send("GET", "/customers?page=1&limit=10");

    expect(updated.status).toBe(200);
    expect(customerSchema.parse(updated.body).email).toBeNull();
    const [message] = await kafka.readFromBeginning(CUSTOMER_UPDATED, 1, 30_000);
    const envelope = envelopeSchema.parse(JSON.parse(message?.value ?? ""));
    expect(envelope.payload.authorizedTransportTypeIds).toEqual([TRUCK, BIKE].sort());
    expect(found.status).toBe(200);
    expect(missing.status).toBe(404);
    expect(page.body).toMatchObject({ total: 2, page: 1, pageSize: 10 });
  });

  it("GET /metrics expoe consumo por resultado e outbox", async () => {
    const scrape = async (): Promise<string> => (await fetch(`${baseUrl}/metrics`)).text();
    // O publisher marca publishedAt depois de enviar o lote: espera o ciclo fechar.
    await waitFor("outbox drenado", async () => sampleValue(await scrape(), "outbox_pending_events", {}) === 0);
    const text = await scrape();

    expect(sampleValue(text, "kafka_messages_consumed_total", { topic: CREATED, outcome: "dead_letter" })).toBe(1);
    // Created x Updated do REORDERED: a ordem entre topicos varia, entao so o
    // total aplicado no topico de Updated e deterministico.
    expect(sampleValue(text, "kafka_messages_consumed_total", { topic: UPDATED, outcome: "applied" })).toBe(1);
    expect(sampleValue(text, "outbox_events_published_total", { event_type: "CustomerCreated" })).toBe(2);
    expect(sampleValue(text, "outbox_pending_events", {})).toBe(0);
  });

  it("health ready com banco, broker e consumer", async () => {
    expect((await fetch(`${baseUrl}/health/ready`)).status).toBe(200);
  });
});
