import type { Customer } from "@domain/entities/customer.entity";

export const CUSTOMER_REPOSITORY = Symbol("CUSTOMER_REPOSITORY");
export const TRANSPORT_TYPE_REPLICA = Symbol("TRANSPORT_TYPE_REPLICA");

export interface PageRequest {
  readonly page: number;
  readonly pageSize: number;
}

export interface Page<T> {
  readonly items: T[];
  readonly total: number;
}

// Metadados que acompanham os eventos do agregado ate o outbox.
export interface PersistenceContext {
  readonly correlationId: string;
}

export interface ICustomerRepository {
  findById(id: string): Promise<Customer | undefined>;
  findAll(request: PageRequest): Promise<Page<Customer>>;
  // Persistem o agregado e gravam seus eventos no outbox na mesma transacao.
  // Documento duplicado: CustomerDocumentAlreadyExistsError.
  create(customer: Customer, context: PersistenceContext): Promise<void>;
  // Compare-and-set pela versao lida: CustomerConcurrentModificationError.
  update(customer: Customer, context: PersistenceContext): Promise<void>;
}

// Copia local dos tipos de transporte (dono: ms-transport).
export interface ReplicatedTransportType {
  readonly transportTypeId: string;
  readonly name: string;
  readonly active: boolean;
}

export interface SourceEvent {
  readonly eventId: string;
  readonly eventType: string;
  readonly occurredAt: Date;
}

// applied: gravado ou atualizado. duplicate: eventId ja processado.
// stale: mais antigo (ou igual) que o evento que gerou a versao gravada.
export type SyncOutcome = "applied" | "duplicate" | "stale";

export interface ITransportTypeReplicaRepository {
  findByIds(ids: readonly string[]): Promise<ReplicatedTransportType[]>;
  // Registra o evento como processado e aplica o estado na MESMA transacao.
  // Dado rejeitado pelo banco: InvalidTransportTypeEventError.
  syncFromEvent(transportType: ReplicatedTransportType, event: SourceEvent): Promise<SyncOutcome>;
}
