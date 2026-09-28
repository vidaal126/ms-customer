import { InvalidCustomerError } from "@domain/errors/customer.errors";
import { CustomerCreatedEvent, CustomerUpdatedEvent } from "@domain/events/customer.events";
import { BIKE_ID, TRUCK_ID, VALID_CPF } from "../../test/customer.fakes";
import { Customer } from "./customer.entity";

const T0 = new Date("2026-09-28T12:00:00.000Z");
const T1 = new Date("2026-09-28T13:00:00.000Z");

function newCustomer(): Customer {
  const customer = Customer.create({
    name: " Ana Souza ",
    document: "52998224725",
    email: "Ana@Example.com",
    phone: "11987654321",
    authorizedTransportTypeIds: [TRUCK_ID, BIKE_ID],
    now: T0,
  });
  customer.pullDomainEvents();
  return customer;
}

describe("Customer", () => {
  it("create normaliza campos, ordena transportes e registra CustomerCreated", () => {
    const customer = Customer.create({
      name: " Ana Souza ",
      document: "52998224725",
      email: "Ana@Example.com",
      phone: "11987654321",
      authorizedTransportTypeIds: [TRUCK_ID.toUpperCase(), BIKE_ID],
      now: T0,
    });

    expect(customer).toMatchObject({
      name: "Ana Souza",
      document: VALID_CPF,
      email: "ana@example.com",
      phone: "(11) 98765-4321",
      authorizedTransportTypeIds: [TRUCK_ID, BIKE_ID],
      version: 0,
    });
    const [event] = customer.pullDomainEvents();
    expect(event).toBeInstanceOf(CustomerCreatedEvent);
    expect(event?.snapshot).toEqual({
      name: "Ana Souza",
      document: VALID_CPF,
      authorizedTransportTypeIds: [TRUCK_ID, BIKE_ID],
    });
  });

  it("isTransportAuthorized", () => {
    const customer = newCustomer();
    expect(customer.isTransportAuthorized(TRUCK_ID)).toBe(true);
    expect(customer.isTransportAuthorized("cccccccc-cccc-4ccc-8ccc-cccccccccccc")).toBe(false);
  });

  it.each([
    ["nome vazio", { name: "  " }],
    ["transporte repetido", { authorizedTransportTypeIds: [TRUCK_ID, TRUCK_ID] }],
    ["transporte nao uuid", { authorizedTransportTypeIds: ["x"] }],
    ["CPF invalido", { document: "529.982.247-24" }],
  ])("rejeita %s", (_label, override) => {
    expect(() =>
      Customer.create({ name: "Ana", document: VALID_CPF, authorizedTransportTypeIds: [], now: T0, ...override }),
    ).toThrow(InvalidCustomerError);
  });

  it("update com mudanca registra CustomerUpdated; null limpa email e telefone", () => {
    const customer = newCustomer();

    const changed = customer.update({ email: null, phone: null, authorizedTransportTypeIds: [BIKE_ID], now: T1 });

    expect(changed).toBe(true);
    expect(customer).toMatchObject({ email: null, phone: null, authorizedTransportTypeIds: [BIKE_ID], updatedAt: T1 });
    const [event] = customer.pullDomainEvents();
    expect(event).toBeInstanceOf(CustomerUpdatedEvent);
    expect(event?.snapshot.authorizedTransportTypeIds).toEqual([BIKE_ID]);
  });

  it("update sem mudanca (mesma lista em outra ordem) nao registra evento", () => {
    const customer = newCustomer();

    expect(customer.update({ name: "Ana Souza", authorizedTransportTypeIds: [BIKE_ID, TRUCK_ID], now: T1 })).toBe(false);
    expect(customer.updatedAt).toEqual(T0);
    expect(customer.pullDomainEvents()).toHaveLength(0);
  });
});
