import { InvalidEnvironmentError, validateEnv } from "./env";

const valid = {
  DATABASE_URL: "postgresql://u:p@localhost:5435/db",
  KAFKA_BROKER: "localhost:9092",
};

describe("validateEnv", () => {
  it("aplica defaults e converte tipos", () => {
    const env = validateEnv({ ...valid, PORT: "4001" });

    expect(env.PORT).toBe(4001);
    expect(env.KAFKA_BROKER).toEqual(["localhost:9092"]);
    expect(env.TRANSPORT_SYNC_GROUP_ID).toBe("ms-customer.transport-type-sync");
    expect(env.CONSUMER_PAUSE_MS).toBe(30_000);
    expect(env.KAFKA_SEND_TIMEOUT_MS).toBe(5_000);
  });

  it("aceita group de replay fixo e rejeita group id invalido", () => {
    expect(
      validateEnv({ ...valid, TRANSPORT_SYNC_GROUP_ID: "ms-customer.transport-type-sync.replay-1" })
        .TRANSPORT_SYNC_GROUP_ID,
    ).toBe("ms-customer.transport-type-sync.replay-1");
    expect(() => validateEnv({ ...valid, TRANSPORT_SYNC_GROUP_ID: "com espaco" })).toThrow(
      /TRANSPORT_SYNC_GROUP_ID/,
    );
  });

  it("falha listando todas as variaveis invalidas", () => {
    const attempt = (): unknown =>
      validateEnv({ PORT: "abc", KAFKA_BROKER: "sem-porta" });

    expect(attempt).toThrow(InvalidEnvironmentError);
    expect(attempt).toThrow(/DATABASE_URL/);
    expect(attempt).toThrow(/PORT/);
    expect(attempt).toThrow(/KAFKA_BROKER/);
  });
});
