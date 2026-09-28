import { ConfigService } from "@nestjs/config";
import { z } from "zod";
import type { ILogger } from "@common/logger/logger.interface";
import type { Env } from "@config/env";
import type { OutboxEvent } from "@infrastructure/database/generated/client";
import type { OutboundMessage } from "@infrastructure/messaging/event-envelope";
import type { KafkaProducerService } from "@infrastructure/messaging/kafka-producer.service";
import { MetricsService } from "@infrastructure/metrics/metrics.service";
import { OutboxPublisherService } from "./outbox-publisher.service";
import type { OutboxRepository } from "./outbox.repository";

const POLL_MS = 100;
const A = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa";
const B = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb";

function outboxEvent(id: string, sequence: number, aggregateId: string): OutboxEvent {
  return {
    id,
    sequence: BigInt(sequence),
    aggregateId,
    eventType: "CustomerUpdated",
    schemaVersion: 2,
    correlationId: `corr-${id}`,
    payload: {},
    createdAt: new Date("2026-09-28T12:00:00.000Z"),
    publishedAt: null,
  };
}

const envelopeIdSchema = z.object({ eventId: z.string() });

const silentLogger: ILogger = { log: () => undefined, warn: () => undefined, error: () => undefined, debug: () => undefined };

// Roda exatamente um ciclo do publisher e devolve o que foi enviado/marcado.
async function runCycle(
  pending: OutboxEvent[],
  failingEventIds: ReadonlySet<string>,
): Promise<{ sent: string[]; marked: string[] }> {
  const sent: string[] = [];
  const marked: string[] = [];
  const outbox: Pick<OutboxRepository, "findPending" | "countPending" | "markPublished"> = {
    findPending: async () => pending,
    countPending: async () => 0,
    markPublished: async (ids) => {
      marked.push(...ids);
    },
  };
  const producer: Pick<KafkaProducerService, "send"> = {
    send: async (message: OutboundMessage) => {
      const { eventId } = envelopeIdSchema.parse(JSON.parse(String(message.value)));
      if (failingEventIds.has(eventId)) throw new Error("broker indisponivel");
      sent.push(eventId);
    },
  };
  const config = new ConfigService<Env, true>({ OUTBOX_POLL_INTERVAL_MS: POLL_MS, OUTBOX_BATCH_SIZE: 20 });
  const publisher = new OutboxPublisherService(
    outbox as OutboxRepository,
    producer as KafkaProducerService,
    silentLogger,
    new MetricsService(),
    config,
  );

  publisher.onModuleInit();
  await jest.advanceTimersByTimeAsync(POLL_MS);
  await publisher.onModuleDestroy();
  return { sent, marked };
}

describe("OutboxPublisherService", () => {
  beforeEach(() => {
    jest.useFakeTimers();
  });

  afterEach(() => {
    jest.useRealTimers();
  });

  it("publica na ordem recebida e marca todos", async () => {
    const pending = [outboxEvent("e1", 1, A), outboxEvent("e2", 2, B), outboxEvent("e3", 3, A)];

    const { sent, marked } = await runCycle(pending, new Set());

    expect(sent).toEqual(["e1", "e2", "e3"]);
    expect(marked).toEqual(["e1", "e2", "e3"]);
  });

  it("falha num evento bloqueia o resto do agregado no ciclo; outros agregados seguem", async () => {
    const pending = [
      outboxEvent("a1", 1, A),
      outboxEvent("a2", 2, A),
      outboxEvent("b1", 3, B),
      outboxEvent("a3", 4, A),
      outboxEvent("b2", 5, B),
    ];

    const { sent, marked } = await runCycle(pending, new Set(["a2"]));

    // a3 nao e tentado: publicar depois de a2 inverteria a ordem do agregado.
    expect(sent).toEqual(["a1", "b1", "b2"]);
    expect(marked).toEqual(["a1", "b1", "b2"]);
  });

  it("proximo ciclo retoma o agregado bloqueado a partir do evento que falhou", async () => {
    const first = await runCycle([outboxEvent("a1", 1, A), outboxEvent("a2", 2, A)], new Set(["a1"]));
    const retry = await runCycle([outboxEvent("a1", 1, A), outboxEvent("a2", 2, A)], new Set());

    expect(first.sent).toEqual([]);
    expect(first.marked).toEqual([]);
    expect(retry.sent).toEqual(["a1", "a2"]);
  });
});
