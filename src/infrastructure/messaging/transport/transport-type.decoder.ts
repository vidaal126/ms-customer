import { z } from "zod";
import type { DeadLetterReason } from "@application/ports/dead-letter.port";
import type { TransportTypeEvent } from "@application/ports/sync-transport-type.port";

export const TRANSPORT_TYPE_EVENT_TYPES = ["TransportTypeCreated", "TransportTypeUpdated"] as const;
const SUPPORTED_ENVELOPE_VERSION = 2;

const envelopeSchema = z.object({
  eventId: z.uuid(),
  eventType: z.enum(TRANSPORT_TYPE_EVENT_TYPES),
  schemaVersion: z.number().int(),
  occurredAt: z.iso.datetime(),
  aggregateId: z.string().min(1),
  correlationId: z.string().min(1),
  payload: z.unknown(),
});

const payloadSchema = z.object({
  id: z.uuid(),
  name: z.string().min(1),
  description: z.string().nullable(),
  active: z.boolean(),
});

export type DecodeResult =
  | { readonly ok: true; readonly event: TransportTypeEvent; readonly correlationId: string }
  | { readonly ok: false; readonly reason: DeadLetterReason; readonly detail: string };

// Contrato de transport.TransportType* (ms-transport): so envelope v2. Nunca
// lanca: qualquer mensagem fora do contrato vira motivo de DLT.
export function decodeTransportTypeEvent(raw: Buffer | null): DecodeResult {
  if (raw === null || raw.length === 0) return fail("invalid_json", "mensagem vazia");

  let parsed: unknown;
  try {
    parsed = JSON.parse(raw.toString("utf8"));
  } catch (err) {
    return fail("invalid_json", err instanceof Error ? err.message : "JSON invalido");
  }

  const envelope = envelopeSchema.safeParse(parsed);
  if (!envelope.success) return fail("schema_validation_failed", z.prettifyError(envelope.error));
  if (envelope.data.schemaVersion !== SUPPORTED_ENVELOPE_VERSION) {
    return fail("unsupported_schema_version", `schemaVersion ${envelope.data.schemaVersion} no envelope`);
  }

  const payload = payloadSchema.safeParse(envelope.data.payload);
  if (!payload.success) return fail("schema_validation_failed", z.prettifyError(payload.error));
  if (payload.data.id !== envelope.data.aggregateId) {
    return fail(
      "schema_validation_failed",
      `aggregateId ${envelope.data.aggregateId} difere de payload.id ${payload.data.id}`,
    );
  }

  return {
    ok: true,
    correlationId: envelope.data.correlationId,
    event: {
      eventId: envelope.data.eventId,
      eventType: envelope.data.eventType,
      occurredAt: new Date(envelope.data.occurredAt),
      transportTypeId: payload.data.id,
      name: payload.data.name,
      active: payload.data.active,
    },
  };
}

function fail(reason: DeadLetterReason, detail: string): DecodeResult {
  return { ok: false, reason, detail };
}
