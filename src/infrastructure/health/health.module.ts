import { Module } from "@nestjs/common";
import { TerminusModule } from "@nestjs/terminus";
import { CustomerModule } from "@infrastructure/customer.module";
import { MessagingModule } from "@infrastructure/messaging/messaging.module";
import { HealthController } from "./health.controller";
import { PrismaHealthIndicator } from "./prisma.health";
import { TransportTypeConsumerHealthIndicator } from "./transport-type-consumer.health";

@Module({
  imports: [TerminusModule, MessagingModule, CustomerModule],
  controllers: [HealthController],
  providers: [PrismaHealthIndicator, TransportTypeConsumerHealthIndicator],
})
export class HealthModule {}
