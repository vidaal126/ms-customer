import type { Prisma } from "@infrastructure/database/generated/client";
import type { CustomerEvent } from "@domain/events/customer.events";
import type { PersistenceContext } from "@application/ports/customer.ports";

// Contrato publicado de customer.* (envelope v2). O ms-sales-order depende
// exatamente destes campos: mudanca incompativel exige nova versao.
export const CUSTOMER_EVENTS_SCHEMA_VERSION = 2;

export function toOutboxEventData(
  event: CustomerEvent,
  context: PersistenceContext,
): Prisma.OutboxEventCreateManyInput {
  const payload: Prisma.InputJsonObject = {
    id: event.aggregateId,
    name: event.snapshot.name,
    document: event.snapshot.document,
    authorizedTransportTypeIds: [...event.snapshot.authorizedTransportTypeIds],
  };
  return {
    aggregateId: event.aggregateId,
    eventType: event.eventType,
    schemaVersion: CUSTOMER_EVENTS_SCHEMA_VERSION,
    correlationId: context.correlationId,
    payload,
    createdAt: event.occurredAt,
  };
}
