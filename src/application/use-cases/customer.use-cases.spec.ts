import {
  CustomerDocumentAlreadyExistsError,
  CustomerNotFoundError,
  UnknownTransportTypeError,
} from "@domain/errors/customer.errors";
import {
  BIKE_ID,
  InMemoryCustomerRepository,
  InMemoryTransportTypeReplica,
  TRUCK_ID,
  VALID_CPF,
} from "../../test/customer.fakes";
import {
  CreateCustomerUseCase,
  GetCustomerUseCase,
  ListCustomersUseCase,
  UpdateCustomerUseCase,
} from "./customer.use-cases";
import { SyncTransportTypeUseCase } from "./sync-transport-type.use-case";

const NOW = new Date("2026-09-28T12:00:00.000Z");
const context = { correlationId: "corr-1" };

describe("casos de uso de cliente", () => {
  let customers: InMemoryCustomerRepository;
  let replica: InMemoryTransportTypeReplica;
  let create: CreateCustomerUseCase;

  beforeEach(() => {
    customers = new InMemoryCustomerRepository();
    replica = new InMemoryTransportTypeReplica();
    replica.seed(TRUCK_ID);
    replica.seed(BIKE_ID, false);
    create = new CreateCustomerUseCase(customers, replica, () => NOW);
  });

  const input = { name: "Ana", document: VALID_CPF, authorizedTransportTypeIds: [TRUCK_ID] };

  it("cria com transporte replicado e ativo e grava CustomerCreated no outbox", async () => {
    const customer = await create.execute(input, context);

    expect(customers.rows.get(customer.id)).toBe(customer);
    expect(customers.outbox).toHaveLength(1);
    expect(customers.outbox[0]).toMatchObject({ event: { eventType: "CustomerCreated" }, context });
  });

  it("transporte desconhecido ou inativo: UnknownTransportTypeError, nada gravado", async () => {
    const unknown = "cccccccc-cccc-4ccc-8ccc-cccccccccccc";

    await expect(create.execute({ ...input, authorizedTransportTypeIds: [unknown] }, context)).rejects.toBeInstanceOf(
      UnknownTransportTypeError,
    );
    await expect(create.execute({ ...input, authorizedTransportTypeIds: [BIKE_ID] }, context)).rejects.toThrow(BIKE_ID);
    expect(customers.rows.size).toBe(0);
  });

  it("CPF duplicado propaga conflito", async () => {
    await create.execute(input, context);
    await expect(create.execute(input, context)).rejects.toBeInstanceOf(CustomerDocumentAlreadyExistsError);
  });

  it("update valida so transportes novos, antes de mutar; sem mudanca nao persiste", async () => {
    const customer = await create.execute(input, context);
    const update = new UpdateCustomerUseCase(customers, replica, () => NOW);

    await update.execute({ id: customer.id, name: "Ana" }, context);
    expect(customers.updates).toBe(0);

    await expect(
      update.execute({ id: customer.id, authorizedTransportTypeIds: [BIKE_ID] }, context),
    ).rejects.toBeInstanceOf(UnknownTransportTypeError);

    // Ja autorizado e agora inativo: pode permanecer na lista.
    replica.seed(TRUCK_ID, false);
    await update.execute({ id: customer.id, name: "Ana Maria", authorizedTransportTypeIds: [TRUCK_ID] }, context);
    expect(customers.updates).toBe(1);
    expect(customers.outbox.map((e) => e.event.eventType)).toEqual(["CustomerCreated", "CustomerUpdated"]);
  });

  it("update e get de inexistente: CustomerNotFoundError", async () => {
    const missing = "dddddddd-dddd-4ddd-8ddd-dddddddddddd";
    await expect(
      new UpdateCustomerUseCase(customers, replica).execute({ id: missing, name: "x" }, context),
    ).rejects.toBeInstanceOf(CustomerNotFoundError);
    await expect(new GetCustomerUseCase(customers).execute(missing)).rejects.toBeInstanceOf(CustomerNotFoundError);
  });

  it("listar aplica limite maximo", async () => {
    expect(await new ListCustomersUseCase(customers).execute({ limit: 500 })).toMatchObject({ page: 1, pageSize: 100 });
  });
});

describe("SyncTransportTypeUseCase", () => {
  it("aplica, deduplica e descarta evento mais antigo", async () => {
    const replica = new InMemoryTransportTypeReplica();
    const sync = new SyncTransportTypeUseCase(replica);
    const base = { eventType: "TransportTypeUpdated", transportTypeId: TRUCK_ID, name: "Caminhao" };

    expect(await sync.execute({ ...base, eventId: "e2", occurredAt: new Date(2_000), active: false })).toBe("applied");
    expect(await sync.execute({ ...base, eventId: "e2", occurredAt: new Date(2_000), active: false })).toBe("duplicate");
    expect(await sync.execute({ ...base, eventId: "e1", occurredAt: new Date(1_000), active: true })).toBe("stale");
    expect(replica.rows.get(TRUCK_ID)?.active).toBe(false);
  });
});
