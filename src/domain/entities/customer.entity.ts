import { randomUUID } from "node:crypto";
import { InvalidCustomerError } from "@domain/errors/customer.errors";
import {
  CustomerCreatedEvent,
  type CustomerEvent,
  type CustomerSnapshot,
  CustomerUpdatedEvent,
} from "@domain/events/customer.events";
import { normalizeEmail, normalizePhone } from "@domain/value-objects/contact.value-objects";
import { normalizeCpf } from "@domain/value-objects/document.value-object";
import { AggregateRoot } from "./aggregate-root";

export const CUSTOMER_NAME_MAX_LENGTH = 200;
export const MAX_AUTHORIZED_TRANSPORT_TYPES = 50;
const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;

export interface CreateCustomerProps {
  readonly name: string;
  readonly document: string;
  readonly email?: string | null | undefined;
  readonly phone?: string | null | undefined;
  readonly authorizedTransportTypeIds: readonly string[];
  readonly now: Date;
}

export interface RestoreCustomerProps {
  readonly id: string;
  readonly name: string;
  readonly document: string;
  readonly email: string | null;
  readonly phone: string | null;
  readonly authorizedTransportTypeIds: readonly string[];
  readonly createdAt: Date;
  readonly updatedAt: Date;
}

// undefined = nao informado (mantem); null em email/phone = limpar. A lista de
// transportes, quando informada, substitui a atual. document e imutavel.
export interface UpdateCustomerProps {
  readonly name?: string | undefined;
  readonly email?: string | null | undefined;
  readonly phone?: string | null | undefined;
  readonly authorizedTransportTypeIds?: readonly string[] | undefined;
  readonly now: Date;
}

// Agregado Cliente (monolito: Customer + CustomerTransportType). A regra de
// autorizacao de transporte pertence a ele.
export class Customer extends AggregateRoot<CustomerEvent> {
  private constructor(
    readonly id: string,
    private _name: string,
    readonly document: string,
    private _email: string | null,
    private _phone: string | null,
    private _authorizedTransportTypeIds: readonly string[],
    readonly createdAt: Date,
    private _updatedAt: Date,
  ) {
    super();
  }

  static create(props: CreateCustomerProps): Customer {
    const customer = new Customer(
      randomUUID(),
      normalizeName(props.name),
      normalizeCpf(props.document),
      optional(props.email, normalizeEmail),
      optional(props.phone, normalizePhone),
      normalizeTransportTypeIds(props.authorizedTransportTypeIds),
      props.now,
      props.now,
    );
    customer.record(new CustomerCreatedEvent(customer.id, props.now, customer.snapshot()));
    return customer;
  }

  static restore(props: RestoreCustomerProps): Customer {
    return new Customer(
      props.id,
      props.name,
      props.document,
      props.email,
      props.phone,
      props.authorizedTransportTypeIds,
      props.createdAt,
      props.updatedAt,
    );
  }

  get name(): string {
    return this._name;
  }

  get email(): string | null {
    return this._email;
  }

  get phone(): string | null {
    return this._phone;
  }

  get authorizedTransportTypeIds(): readonly string[] {
    return this._authorizedTransportTypeIds;
  }

  get updatedAt(): Date {
    return this._updatedAt;
  }

  isTransportAuthorized(transportTypeId: string): boolean {
    return this._authorizedTransportTypeIds.includes(transportTypeId);
  }

  // Registra CustomerUpdated so quando algo muda; true = houve mudanca.
  update(props: UpdateCustomerProps): boolean {
    const name = props.name === undefined ? this._name : normalizeName(props.name);
    const email = props.email === undefined ? this._email : optional(props.email, normalizeEmail);
    const phone = props.phone === undefined ? this._phone : optional(props.phone, normalizePhone);
    const transportTypeIds =
      props.authorizedTransportTypeIds === undefined
        ? this._authorizedTransportTypeIds
        : normalizeTransportTypeIds(props.authorizedTransportTypeIds);

    const unchanged =
      name === this._name &&
      email === this._email &&
      phone === this._phone &&
      sameIds(transportTypeIds, this._authorizedTransportTypeIds);
    if (unchanged) return false;

    this._name = name;
    this._email = email;
    this._phone = phone;
    this._authorizedTransportTypeIds = transportTypeIds;
    this._updatedAt = props.now;
    this.record(new CustomerUpdatedEvent(this.id, props.now, this.snapshot()));
    return true;
  }

  snapshot(): CustomerSnapshot {
    return {
      name: this._name,
      document: this.document,
      authorizedTransportTypeIds: [...this._authorizedTransportTypeIds],
    };
  }
}

function normalizeName(raw: string): string {
  const name = raw.trim();
  if (name.length === 0 || name.length > CUSTOMER_NAME_MAX_LENGTH) {
    throw new InvalidCustomerError(`Nome deve ter entre 1 e ${CUSTOMER_NAME_MAX_LENGTH} caracteres`);
  }
  return name;
}

// Ordenados: comparacao e payload deterministicos. Duplicata e erro (nao e
// silenciosamente removida): indica um cliente da API com bug.
function normalizeTransportTypeIds(raw: readonly string[]): readonly string[] {
  const ids = raw.map((id) => id.toLowerCase());
  if (ids.length > MAX_AUTHORIZED_TRANSPORT_TYPES) {
    throw new InvalidCustomerError(`No maximo ${MAX_AUTHORIZED_TRANSPORT_TYPES} tipos de transporte autorizados`);
  }
  if (new Set(ids).size !== ids.length) {
    throw new InvalidCustomerError("Tipo de transporte autorizado repetido");
  }
  const invalid = ids.find((id) => !UUID_PATTERN.test(id));
  if (invalid !== undefined) throw new InvalidCustomerError(`Tipo de transporte ${invalid} invalido`);
  return [...ids].sort();
}

function sameIds(a: readonly string[], b: readonly string[]): boolean {
  return a.length === b.length && a.every((id, index) => id === b[index]);
}

function optional(value: string | null | undefined, normalize: (raw: string) => string): string | null {
  if (value === undefined || value === null) return null;
  return normalize(value);
}
