import type { SyncOutcome } from "./customer.ports";

export const SYNC_TRANSPORT_TYPE = Symbol("SYNC_TRANSPORT_TYPE");

// Evento transport.TransportType* ja decodificado (envelope v2).
export interface TransportTypeEvent {
  readonly eventId: string;
  readonly eventType: string;
  readonly occurredAt: Date;
  readonly transportTypeId: string;
  readonly name: string;
  readonly active: boolean;
}

// Port de entrada: o adapter Kafka entrega o evento decodificado. Lanca
// InvariantViolationError para evento que nunca sera aceito; qualquer outro
// erro e recuperavel.
export interface SyncTransportTypePort {
  execute(event: TransportTypeEvent): Promise<SyncOutcome>;
}
