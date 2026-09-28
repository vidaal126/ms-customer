import { EntityConflictError, EntityNotFoundError, InvariantViolationError } from "./domain.error";

export class CustomerNotFoundError extends EntityNotFoundError {
  constructor(readonly customerId: string) {
    super(`Cliente ${customerId} nao encontrado`);
  }
}

export class CustomerDocumentAlreadyExistsError extends EntityConflictError {
  constructor(readonly document: string) {
    super(`Ja existe um cliente com o documento ${document}`);
  }
}

export class InvalidCustomerError extends InvariantViolationError {}

// Id de transporte que a replica local nao conhece (ainda nao replicado ou
// inexistente) ou que esta inativo. Consistencia eventual: um tipo recem-criado
// no ms-transport pode levar alguns instantes para chegar aqui.
export class UnknownTransportTypeError extends InvariantViolationError {
  constructor(readonly transportTypeIds: readonly string[]) {
    super(`Tipos de transporte desconhecidos ou inativos: ${transportTypeIds.join(", ")}`);
  }
}

// Evento de transporte que o banco da replica rejeitou (CHECK/NOT NULL):
// nunca sera aceito, vai para a DLT.
export class InvalidTransportTypeEventError extends InvariantViolationError {}
