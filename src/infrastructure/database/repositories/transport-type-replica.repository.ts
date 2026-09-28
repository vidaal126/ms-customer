import { Injectable } from "@nestjs/common";
import { InvalidTransportTypeEventError } from "@domain/errors/customer.errors";
import type {
  ITransportTypeReplicaRepository,
  ReplicatedTransportType,
  SourceEvent,
  SyncOutcome,
} from "@application/ports/customer.ports";
import { isIntegrityViolation } from "@infrastructure/database/prisma/integrity-violation";
import { PrismaService } from "@infrastructure/database/prisma/prisma.service";

@Injectable()
export class TransportTypeReplicaRepositoryPrisma implements ITransportTypeReplicaRepository {
  constructor(private readonly prisma: PrismaService) {}

  async findByIds(ids: readonly string[]): Promise<ReplicatedTransportType[]> {
    const rows = await this.prisma.transportTypeReplica.findMany({
      where: { transportTypeId: { in: [...ids] } },
      select: { transportTypeId: true, name: true, active: true },
    });
    return rows;
  }

  // Dado rejeitado pelo banco (CHECK, NOT NULL) vira erro de dominio: o
  // evento nunca sera aceito e vai para a DLT. Demais erros propagam.
  async syncFromEvent(transportType: ReplicatedTransportType, event: SourceEvent): Promise<SyncOutcome> {
    try {
      return await this.prisma.$transaction(async (tx) => {
        // ON CONFLICT DO NOTHING na PK: count 0 = eventId ja processado.
        const registered = await tx.processedEvent.createMany({
          data: [{ eventId: event.eventId, eventType: event.eventType }],
          skipDuplicates: true,
        });
        if (registered.count === 0) return "duplicate";

        // So sobrescreve estado mais antigo: Created e Updated chegam por
        // topicos diferentes, sem ordem garantida entre eles.
        const affected = await tx.$executeRaw`
          INSERT INTO "transport_types_replica" (
            "transportTypeId", "name", "active", "sourceEventId", "sourceOccurredAt", "createdAt", "updatedAt"
          ) VALUES (
            ${transportType.transportTypeId}, ${transportType.name}, ${transportType.active},
            ${event.eventId}, ${event.occurredAt}, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
          )
          ON CONFLICT ("transportTypeId") DO UPDATE SET
            "name" = EXCLUDED."name",
            "active" = EXCLUDED."active",
            "sourceEventId" = EXCLUDED."sourceEventId",
            "sourceOccurredAt" = EXCLUDED."sourceOccurredAt",
            "updatedAt" = CURRENT_TIMESTAMP
          WHERE "transport_types_replica"."sourceOccurredAt" < EXCLUDED."sourceOccurredAt"
        `;
        return affected === 1 ? "applied" : "stale";
      });
    } catch (err) {
      if (isIntegrityViolation(err)) {
        throw new InvalidTransportTypeEventError(
          `replica rejeitou o tipo ${transportType.transportTypeId}: ${err instanceof Error ? err.message : String(err)}`,
        );
      }
      throw err;
    }
  }
}
