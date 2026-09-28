import { Customer } from "@domain/entities/customer.entity";
import {
  CustomerConcurrentModificationError,
  CustomerDocumentAlreadyExistsError,
} from "@domain/errors/customer.errors";
import type { CustomerEvent } from "@domain/events/customer.events";
import type {
  ICustomerRepository,
  ITransportTypeReplicaRepository,
  Page,
  PageRequest,
  PersistenceContext,
  ReplicatedTransportType,
  SourceEvent,
  SyncOutcome,
} from "@application/ports/customer.ports";

export const VALID_CPF = "529.982.247-25";
export const OTHER_VALID_CPF = "111.444.777-35";
export const TRUCK_ID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa";
export const BIKE_ID = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb";

export class InMemoryCustomerRepository implements ICustomerRepository {
  readonly rows = new Map<string, Customer>();
  readonly outbox: Array<{ event: CustomerEvent; context: PersistenceContext }> = [];
  updates = 0;

  // Copia a cada leitura, como o banco: duas leituras nao compartilham estado.
  async findById(id: string): Promise<Customer | undefined> {
    const row = this.rows.get(id);
    return row ? snapshot(row, row.version) : undefined;
  }

  async findAll(request: PageRequest): Promise<Page<Customer>> {
    const all = [...this.rows.values()];
    const start = (request.page - 1) * request.pageSize;
    return { items: all.slice(start, start + request.pageSize), total: all.length };
  }

  async create(customer: Customer, context: PersistenceContext): Promise<void> {
    if ([...this.rows.values()].some((c) => c.document === customer.document)) {
      throw new CustomerDocumentAlreadyExistsError(customer.document);
    }
    this.rows.set(customer.id, customer);
    this.pushEvents(customer, context);
  }

  async update(customer: Customer, context: PersistenceContext): Promise<void> {
    const stored = this.rows.get(customer.id);
    if (stored?.version !== customer.version) throw new CustomerConcurrentModificationError(customer.id);
    this.updates += 1;
    this.rows.set(customer.id, snapshot(customer, customer.version + 1));
    this.pushEvents(customer, context);
  }

  private pushEvents(customer: Customer, context: PersistenceContext): void {
    for (const event of customer.pullDomainEvents()) this.outbox.push({ event, context });
  }
}

function snapshot(customer: Customer, version: number): Customer {
  return Customer.restore({
    id: customer.id,
    name: customer.name,
    document: customer.document,
    email: customer.email,
    phone: customer.phone,
    authorizedTransportTypeIds: customer.authorizedTransportTypeIds,
    version,
    createdAt: customer.createdAt,
    updatedAt: customer.updatedAt,
  });
}

export class InMemoryTransportTypeReplica implements ITransportTypeReplicaRepository {
  readonly rows = new Map<string, ReplicatedTransportType & { readonly occurredAt: Date }>();
  readonly processed = new Set<string>();

  seed(transportTypeId: string, active = true): void {
    this.rows.set(transportTypeId, { transportTypeId, name: transportTypeId, active, occurredAt: new Date(0) });
  }

  async findByIds(ids: readonly string[]): Promise<ReplicatedTransportType[]> {
    return ids.flatMap((id) => {
      const row = this.rows.get(id);
      return row ? [row] : [];
    });
  }

  async syncFromEvent(transportType: ReplicatedTransportType, event: SourceEvent): Promise<SyncOutcome> {
    if (this.processed.has(event.eventId)) return "duplicate";
    this.processed.add(event.eventId);
    const current = this.rows.get(transportType.transportTypeId);
    if (current && current.occurredAt.getTime() >= event.occurredAt.getTime()) return "stale";
    this.rows.set(transportType.transportTypeId, { ...transportType, occurredAt: event.occurredAt });
    return "applied";
  }
}
