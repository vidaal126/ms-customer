import type { ITransportTypeReplicaRepository, SyncOutcome } from "@application/ports/customer.ports";
import type { SyncTransportTypePort, TransportTypeEvent } from "@application/ports/sync-transport-type.port";

export class SyncTransportTypeUseCase implements SyncTransportTypePort {
  constructor(private readonly replica: ITransportTypeReplicaRepository) {}

  async execute(event: TransportTypeEvent): Promise<SyncOutcome> {
    return this.replica.syncFromEvent(
      { transportTypeId: event.transportTypeId, name: event.name, active: event.active },
      { eventId: event.eventId, eventType: event.eventType, occurredAt: event.occurredAt },
    );
  }
}
