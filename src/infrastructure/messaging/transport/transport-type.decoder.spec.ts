import { decodeTransportTypeEvent } from "./transport-type.decoder";

const ID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa";

function envelope(overrides: Record<string, unknown> = {}, payload: Record<string, unknown> = {}): Buffer {
  return Buffer.from(
    JSON.stringify({
      eventId: "11111111-1111-4111-8111-111111111111",
      eventType: "TransportTypeCreated",
      schemaVersion: 2,
      occurredAt: "2026-09-28T12:00:00.000Z",
      aggregateId: ID,
      correlationId: "corr-1",
      payload: { id: ID, name: "Caminhao", description: null, active: true, ...payload },
      ...overrides,
    }),
  );
}

describe("decodeTransportTypeEvent", () => {
  it("decodifica envelope v2 valido", () => {
    expect(decodeTransportTypeEvent(envelope())).toEqual({
      ok: true,
      correlationId: "corr-1",
      event: {
        eventId: "11111111-1111-4111-8111-111111111111",
        eventType: "TransportTypeCreated",
        occurredAt: new Date("2026-09-28T12:00:00.000Z"),
        transportTypeId: ID,
        name: "Caminhao",
        active: true,
      },
    });
  });

  it.each([
    ["vazio", null, "invalid_json"],
    ["texto", Buffer.from("t"), "invalid_json"],
    ["versao 3", envelope({ schemaVersion: 3 }), "unsupported_schema_version"],
    ["eventType desconhecido", envelope({ eventType: "TransportTypeDeleted" }), "schema_validation_failed"],
    ["payload sem active", envelope({}, { active: "sim" }), "schema_validation_failed"],
    ["aggregateId divergente", envelope({ aggregateId: "outro" }), "schema_validation_failed"],
  ])("%s: DLT com motivo %s", (_label, raw, reason) => {
    expect(decodeTransportTypeEvent(raw)).toMatchObject({ ok: false, reason });
  });
});
