import { HealthIndicatorService } from "@nestjs/terminus";
import type { ConsumerStatus } from "@infrastructure/messaging/kafka-consumer.base";
import type { TransportTypeConsumer } from "@infrastructure/messaging/transport/transport-type.consumer";
import { TransportTypeConsumerHealthIndicator } from "./transport-type-consumer.health";

function indicatorFor(status: ConsumerStatus): TransportTypeConsumerHealthIndicator {
  const consumer: Pick<TransportTypeConsumer, "getHealth"> = {
    getHealth: () => ({
      status,
      since: new Date("2026-09-22T12:00:00.000Z"),
      lastError: "Can't reach database server at localhost:5435",
    }),
  };
  return new TransportTypeConsumerHealthIndicator(
    consumer as TransportTypeConsumer,
    new HealthIndicatorService(),
  );
}

describe("TransportTypeConsumerHealthIndicator", () => {
  it("running: up", () => {
    expect(indicatorFor("running").isHealthy("transportTypeConsumer").transportTypeConsumer.status).toBe("up");
  });

  it.each<ConsumerStatus>(["starting", "degraded", "stopped"])("%s: down", (status) => {
    expect(indicatorFor(status).isHealthy("transportTypeConsumer").transportTypeConsumer.status).toBe("down");
  });

  it("nao expoe a mensagem de erro do consumer", () => {
    const result = indicatorFor("degraded").isHealthy("transportTypeConsumer");

    expect(JSON.stringify(result)).not.toContain("localhost");
  });
});
