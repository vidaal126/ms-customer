import { ConfigService } from "@nestjs/config";
import type { Kafka, Producer } from "kafkajs";
import type { ILogger } from "@common/logger/logger.interface";
import { TimeoutError } from "@common/with-timeout";
import { KafkaProducerService } from "./kafka-producer.service";

const TIMEOUT_MS = 20;
const MESSAGE = { topic: "customer.CustomerUpdated", key: "k", value: "{}", headers: {} };

const silentLogger: ILogger = { log: () => undefined, warn: () => undefined, error: () => undefined, debug: () => undefined };

function producerWith(send: Producer["send"]): KafkaProducerService {
  const producer: Pick<Producer, "connect" | "send" | "on" | "events"> = {
    connect: async () => undefined,
    send,
    on: () => () => undefined,
    events: { DISCONNECT: "producer.disconnect" } as Producer["events"],
  };
  const kafka = { producer: () => producer } as unknown as Kafka;
  return new KafkaProducerService(
    kafka,
    silentLogger,
    new ConfigService({ KAFKA_SEND_TIMEOUT_MS: TIMEOUT_MS }),
  );
}

describe("KafkaProducerService", () => {
  it("envia dentro do teto", async () => {
    const send = jest.fn<ReturnType<Producer["send"]>, Parameters<Producer["send"]>>(async () => []);

    await producerWith(send).send(MESSAGE);

    expect(send).toHaveBeenCalledWith(expect.objectContaining({ topic: MESSAGE.topic }));
  });

  it("ack que nunca chega: rejeita com TimeoutError em vez de prender o chamador", async () => {
    const service = producerWith(() => new Promise(() => undefined));

    await expect(service.send(MESSAGE)).rejects.toBeInstanceOf(TimeoutError);
  });
});
