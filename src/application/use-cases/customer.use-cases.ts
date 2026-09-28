import { Customer } from "@domain/entities/customer.entity";
import { CustomerNotFoundError, UnknownTransportTypeError } from "@domain/errors/customer.errors";
import type {
  ICustomerRepository,
  ITransportTypeReplicaRepository,
  PersistenceContext,
} from "@application/ports/customer.ports";

export const DEFAULT_PAGE_SIZE = 20;
export const MAX_PAGE_SIZE = 100;

export function resolvePageSize(limit?: number): number {
  if (!limit || limit <= 0) return DEFAULT_PAGE_SIZE;
  return Math.min(limit, MAX_PAGE_SIZE);
}

type Clock = () => Date;
const systemClock: Clock = () => new Date();

// Todo id autorizado precisa existir na replica e estar ativo.
async function assertTransportTypesAvailable(
  replica: ITransportTypeReplicaRepository,
  ids: readonly string[],
): Promise<void> {
  if (ids.length === 0) return;
  const known = await replica.findByIds(ids);
  const available = new Set(known.filter((t) => t.active).map((t) => t.transportTypeId));
  const missing = ids.filter((id) => !available.has(id));
  if (missing.length > 0) throw new UnknownTransportTypeError(missing);
}

export interface CreateCustomerInput {
  readonly name: string;
  readonly document: string;
  readonly email?: string | undefined;
  readonly phone?: string | undefined;
  readonly authorizedTransportTypeIds: readonly string[];
}

export class CreateCustomerUseCase {
  constructor(
    private readonly customers: ICustomerRepository,
    private readonly replica: ITransportTypeReplicaRepository,
    private readonly clock: Clock = systemClock,
  ) {}

  // Documento duplicado nao e pre-checado: o unique do banco decide.
  async execute(input: CreateCustomerInput, context: PersistenceContext): Promise<Customer> {
    const customer = Customer.create({ ...input, now: this.clock() });
    await assertTransportTypesAvailable(this.replica, customer.authorizedTransportTypeIds);
    await this.customers.create(customer, context);
    return customer;
  }
}

export interface UpdateCustomerInput {
  readonly id: string;
  readonly name?: string | undefined;
  readonly email?: string | null | undefined;
  readonly phone?: string | null | undefined;
  readonly authorizedTransportTypeIds?: readonly string[] | undefined;
}

export class UpdateCustomerUseCase {
  constructor(
    private readonly customers: ICustomerRepository,
    private readonly replica: ITransportTypeReplicaRepository,
    private readonly clock: Clock = systemClock,
  ) {}

  async execute(input: UpdateCustomerInput, context: PersistenceContext): Promise<Customer> {
    const customer = await this.customers.findById(input.id);
    if (!customer) throw new CustomerNotFoundError(input.id);

    // Valida antes de mutar o agregado. So os transportes novos na lista
    // precisam estar replicados e ativos: um ja autorizado que ficou inativo
    // pode continuar na lista (a ordem de venda e quem barra o uso).
    if (input.authorizedTransportTypeIds !== undefined) {
      const added = input.authorizedTransportTypeIds
        .map((id) => id.toLowerCase())
        .filter((id) => !customer.isTransportAuthorized(id));
      await assertTransportTypesAvailable(this.replica, added);
    }

    const changed = customer.update({
      name: input.name,
      email: input.email,
      phone: input.phone,
      authorizedTransportTypeIds: input.authorizedTransportTypeIds,
      now: this.clock(),
    });
    if (!changed) return customer;
    await this.customers.update(customer, context);
    return customer;
  }
}

export class GetCustomerUseCase {
  constructor(private readonly customers: ICustomerRepository) {}

  async execute(id: string): Promise<Customer> {
    const customer = await this.customers.findById(id);
    if (!customer) throw new CustomerNotFoundError(id);
    return customer;
  }
}

export interface ListCustomersInput {
  readonly page?: number | undefined;
  readonly limit?: number | undefined;
}

export interface ListCustomersOutput {
  readonly items: Customer[];
  readonly total: number;
  readonly page: number;
  readonly pageSize: number;
}

export class ListCustomersUseCase {
  constructor(private readonly customers: ICustomerRepository) {}

  async execute(input: ListCustomersInput): Promise<ListCustomersOutput> {
    const page = input.page && input.page > 0 ? input.page : 1;
    const pageSize = resolvePageSize(input.limit);
    const { items, total } = await this.customers.findAll({ page, pageSize });
    return { items, total, page, pageSize };
  }
}
