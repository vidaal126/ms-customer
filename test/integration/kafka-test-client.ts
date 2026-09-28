import { randomUUID } from "node:crypto";
import { Kafka, logLevel, Partitioners } from "kafkajs";

export interface ProducedMessage {
  readonly key: string;
  readonly value: string;
  readonly headers?: Record<string, string>;
}

export interface ConsumedMessage {
  readonly key: string | null;
  readonly value: string;
  readonly headers: Record<string, string>;
}

// Cliente Kafka dos testes: publica mensagens e le topicos do inicio.
export class KafkaTestClient {
  private readonly kafka: Kafka;

  constructor(broker: string) {
    this.kafka = new Kafka({ clientId: "integration-test", brokers: [broker], logLevel: logLevel.NOTHING });
  }

  // Cria o topico antes do uso, como o kafka-init do docker compose.
  async createTopic(topic: string): Promise<void> {
    const admin = this.kafka.admin();
    await admin.connect();
    try {
      await admin.createTopics({ waitForLeaders: true, topics: [{ topic, numPartitions: 1 }] });
    } finally {
      await admin.disconnect();
    }
  }

  async produce(topic: string, messages: readonly ProducedMessage[]): Promise<void> {
    const producer = this.kafka.producer({ createPartitioner: Partitioners.DefaultPartitioner });
    await producer.connect();
    try {
      // Uma mensagem por send para fixar a ordem dos offsets.
      for (const message of messages) {
        await producer.send({ topic, messages: [message] });
      }
    } finally {
      await producer.disconnect();
    }
  }

  // Le do inicio ate juntar `count` mensagens ou estourar o tempo.
  async readFromBeginning(topic: string, count: number, timeoutMs: number): Promise<ConsumedMessage[]> {
    const consumer = this.kafka.consumer({ groupId: `integration-reader-${randomUUID()}` });
    const received: ConsumedMessage[] = [];
    await consumer.connect();
    try {
      await consumer.subscribe({ topics: [topic], fromBeginning: true });
      await new Promise<void>((resolve) => {
        const timer = setTimeout(resolve, timeoutMs);
        void consumer.run({
          eachMessage: async ({ message }): Promise<void> => {
            const headers: Record<string, string> = {};
            for (const [name, raw] of Object.entries(message.headers ?? {})) {
              const first = Array.isArray(raw) ? raw[0] : raw;
              if (first !== undefined) headers[name] = first.toString();
            }
            received.push({
              key: message.key?.toString() ?? null,
              value: message.value?.toString() ?? "",
              headers,
            });
            if (received.length >= count) {
              clearTimeout(timer);
              resolve();
            }
          },
        });
      });
    } finally {
      await consumer.disconnect();
    }
    return received;
  }
}

export async function waitFor(
  description: string,
  condition: () => Promise<boolean>,
  timeoutMs = 60_000,
  intervalMs = 500,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await condition()) return;
    await new Promise((resolve) => setTimeout(resolve, intervalMs));
  }
  throw new Error(`Tempo esgotado esperando: ${description}`);
}
