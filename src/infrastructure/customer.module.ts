import { Module } from "@nestjs/common";
import {
  CUSTOMER_REPOSITORY,
  type ICustomerRepository,
  type ITransportTypeReplicaRepository,
  TRANSPORT_TYPE_REPLICA,
} from "@application/ports/customer.ports";
import { DEAD_LETTER_PORT } from "@application/ports/dead-letter.port";
import { SYNC_TRANSPORT_TYPE } from "@application/ports/sync-transport-type.port";
import {
  CreateCustomerUseCase,
  GetCustomerUseCase,
  ListCustomersUseCase,
  UpdateCustomerUseCase,
} from "@application/use-cases/customer.use-cases";
import { SyncTransportTypeUseCase } from "@application/use-cases/sync-transport-type.use-case";
import { CustomerRepositoryPrisma } from "@infrastructure/database/repositories/customer.repository";
import { TransportTypeReplicaRepositoryPrisma } from "@infrastructure/database/repositories/transport-type-replica.repository";
import { CustomerController } from "@infrastructure/http/customer.controller";
import { IdempotencyModule } from "@infrastructure/idempotency/idempotency.module";
import { DeadLetterPublisher } from "@infrastructure/messaging/dead-letter.publisher";
import { MessagingModule } from "@infrastructure/messaging/messaging.module";
import { TransportTypeConsumer } from "@infrastructure/messaging/transport/transport-type.consumer";
import { OutboxPublisherService } from "@infrastructure/outbox/outbox-publisher.service";
import { OutboxRepository } from "@infrastructure/outbox/outbox.repository";

// Clientes (HTTP + outbox customer.*) e replica de tipos de transporte
// (consumer de transport.*). Use cases nao conhecem Nest: composicao aqui.
@Module({
  imports: [MessagingModule, IdempotencyModule],
  controllers: [CustomerController],
  providers: [
    { provide: CUSTOMER_REPOSITORY, useClass: CustomerRepositoryPrisma },
    { provide: TRANSPORT_TYPE_REPLICA, useClass: TransportTypeReplicaRepositoryPrisma },
    ...[CreateCustomerUseCase, UpdateCustomerUseCase].map((useCase) => ({
      provide: useCase,
      useFactory: (customers: ICustomerRepository, replica: ITransportTypeReplicaRepository) =>
        new useCase(customers, replica),
      inject: [CUSTOMER_REPOSITORY, TRANSPORT_TYPE_REPLICA],
    })),
    ...[GetCustomerUseCase, ListCustomersUseCase].map((useCase) => ({
      provide: useCase,
      useFactory: (customers: ICustomerRepository) => new useCase(customers),
      inject: [CUSTOMER_REPOSITORY],
    })),
    {
      provide: SYNC_TRANSPORT_TYPE,
      useFactory: (replica: ITransportTypeReplicaRepository) => new SyncTransportTypeUseCase(replica),
      inject: [TRANSPORT_TYPE_REPLICA],
    },
    { provide: DEAD_LETTER_PORT, useExisting: DeadLetterPublisher },
    TransportTypeConsumer,
    OutboxRepository,
    OutboxPublisherService,
  ],
  exports: [TransportTypeConsumer],
})
export class CustomerModule {}
