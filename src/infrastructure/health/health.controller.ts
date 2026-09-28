import { Controller, Get } from "@nestjs/common";
import { HealthCheck, type HealthCheckResult, HealthCheckService } from "@nestjs/terminus";
import { SkipThrottle } from "@nestjs/throttler";
import { KafkaHealthIndicator } from "@infrastructure/messaging/kafka.health";
import { PrismaHealthIndicator } from "./prisma.health";
import { TransportTypeConsumerHealthIndicator } from "./transport-type-consumer.health";

@Controller("health")
@SkipThrottle()
export class HealthController {
  constructor(
    private readonly health: HealthCheckService,
    private readonly database: PrismaHealthIndicator,
    private readonly kafka: KafkaHealthIndicator,
    private readonly consumer: TransportTypeConsumerHealthIndicator,
  ) {}

  // Liveness: o processo responde, sem checar dependencias.
  @Get("live")
  @HealthCheck()
  live(): Promise<HealthCheckResult> {
    return this.health.check([]);
  }

  // Readiness: banco, broker e consumer da replica de transporte.
  @Get("ready")
  @HealthCheck()
  ready(): Promise<HealthCheckResult> {
    return this.health.check([
      () => this.database.isHealthy("database"),
      () => this.kafka.isHealthy("kafka"),
      () => this.consumer.isHealthy("transportTypeConsumer"),
    ]);
  }
}
