import type { DomainEvent } from "./domain-event";

// Estado publicado do cliente. Email e telefone ficam de fora: nenhum
// consumidor precisa deles (minimizacao de dado pessoal).
export interface CustomerSnapshot {
  readonly name: string;
  readonly document: string;
  readonly authorizedTransportTypeIds: readonly string[];
}

export class CustomerCreatedEvent implements DomainEvent {
  static readonly EVENT_TYPE = "CustomerCreated";

  readonly eventType = CustomerCreatedEvent.EVENT_TYPE;

  constructor(
    readonly aggregateId: string,
    readonly occurredAt: Date,
    readonly snapshot: CustomerSnapshot,
  ) {}
}

export class CustomerUpdatedEvent implements DomainEvent {
  static readonly EVENT_TYPE = "CustomerUpdated";

  readonly eventType = CustomerUpdatedEvent.EVENT_TYPE;

  constructor(
    readonly aggregateId: string,
    readonly occurredAt: Date,
    readonly snapshot: CustomerSnapshot,
  ) {}
}

export type CustomerEvent = CustomerCreatedEvent | CustomerUpdatedEvent;
