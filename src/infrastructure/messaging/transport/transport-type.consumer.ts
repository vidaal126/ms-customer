import { Inject, Injectable } from "@nestjs/common";
import { ConfigService } from "@nestjs/config";
import type { Kafka } from "kafkajs";
import { runWithCorrelationId } from "@common/correlation/correlation-context";
import { type ILogger, LOGGER_TOKEN } from "@common/logger/logger.interface";
import { type Env, readEnv } from "@config/env";
import { DEAD_LETTER_PORT, type DeadLetterPort, type DeadLetterSource } from "@application/ports/dead-letter.port";
import {
  SYNC_TRANSPORT_TYPE,
  type SyncTransportTypePort,
  type TransportTypeEvent,
} from "@application/ports/sync-transport-type.port";
import { InvariantViolationError } from "@domain/errors/domain.error";
import {
  type ConsumerSubscription,
  type InboundMessage,
  KafkaConsumerBase,
} from "@infrastructure/messaging/kafka-consumer.base";
import { KAFKA_CLIENT } from "@infrastructure/messaging/kafka.tokens";
import { MetricsService } from "@infrastructure/metrics/metrics.service";
import { decodeTransportTypeEvent } from "./transport-type.decoder";

export const TRANSPORT_TYPE_TOPICS = ["transport.TransportTypeCreated", "transport.TransportTypeUpdated"] as const;

// Adapter de entrada da replica de tipos de transporte:
// - nao recuperavel (JSON, schema, versao, invariante/dado rejeitado): DLT com
//   os bytes originais e retorna => offset commitado depois do ack da DLT;
// - recuperavel (banco, timeout, conexao, nao classificado): lanca => sem
//   commit; a base faz retry com backoff e depois pausa a particao.
@Injectable()
export class TransportTypeConsumer extends KafkaConsumerBase {
  protected readonly subscription: ConsumerSubscription;

  constructor(
    @Inject(KAFKA_CLIENT) kafka: Kafka,
    @Inject(LOGGER_TOKEN) logger: ILogger,
    @Inject(SYNC_TRANSPORT_TYPE) private readonly syncTransportType: SyncTransportTypePort,
    @Inject(DEAD_LETTER_PORT) private readonly deadLetter: DeadLetterPort,
    private readonly metrics: MetricsService,
    config: ConfigService<Env, true>,
  ) {
    super(kafka, logger);
    this.subscription = {
      groupId: readEnv(config, "TRANSPORT_SYNC_GROUP_ID"),
      topics: TRANSPORT_TYPE_TOPICS,
      fromBeginning: true,
      retry: {
        retries: readEnv(config, "CONSUMER_RETRY_RETRIES"),
        initialDelayMs: readEnv(config, "CONSUMER_RETRY_INITIAL_MS"),
        maxDelayMs: readEnv(config, "CONSUMER_RETRY_MAX_MS"),
        pauseMs: readEnv(config, "CONSUMER_PAUSE_MS"),
      },
    };
  }

  protected override async beforeStart(): Promise<void> {
    for (const topic of TRANSPORT_TYPE_TOPICS) await this.deadLetter.ensureTopic(topic);
  }

  protected async handle(message: InboundMessage): Promise<void> {
    const decoded = decodeTransportTypeEvent(message.value);
    if (!decoded.ok) {
      await this.deadLetter.publish(this.deadLetterSource(message), decoded.reason, decoded.detail);
      this.metrics.recordConsumed(message.topic, "dead_letter");
      return;
    }
    await runWithCorrelationId(decoded.correlationId, () => this.apply(message, decoded.event));
  }

  private async apply(message: InboundMessage, event: TransportTypeEvent): Promise<void> {
    try {
      const outcome = await this.syncTransportType.execute(event);
      this.metrics.recordConsumed(message.topic, outcome);
      const context = {
        topic: message.topic,
        partition: message.partition,
        offset: message.offset,
        eventId: event.eventId,
        transportTypeId: event.transportTypeId,
        outcome,
      };
      if (outcome === "applied") this.logger.log("Tipo de transporte replicado", context);
      else this.logger.debug("Evento de tipo de transporte ignorado", context);
    } catch (err) {
      if (err instanceof InvariantViolationError) {
        await this.deadLetter.publish(this.deadLetterSource(message), "domain_invariant_violation", err.message);
        this.metrics.recordConsumed(message.topic, "dead_letter");
        return;
      }
      throw err;
    }
  }

  private deadLetterSource(message: InboundMessage): DeadLetterSource {
    return { ...message, consumerGroup: this.subscription.groupId };
  }

  protected override onRetryExhausted(message: InboundMessage): void {
    this.metrics.recordConsumed(message.topic, "retry_exhausted");
  }
}
