import { Injectable } from "@nestjs/common";
import { Prisma, type Customer as CustomerModel } from "@infrastructure/database/generated/client";
import { Customer } from "@domain/entities/customer.entity";
import {
  CustomerConcurrentModificationError,
  CustomerDocumentAlreadyExistsError,
} from "@domain/errors/customer.errors";
import type {
  ICustomerRepository,
  Page,
  PageRequest,
  PersistenceContext,
} from "@application/ports/customer.ports";
import { toOutboxEventData } from "@infrastructure/database/mappers/outbox-event.mapper";
import { PrismaService } from "@infrastructure/database/prisma/prisma.service";

@Injectable()
export class CustomerRepositoryPrisma implements ICustomerRepository {
  constructor(private readonly prisma: PrismaService) {}

  async findById(id: string): Promise<Customer | undefined> {
    const row = await this.prisma.customer.findUnique({ where: { id } });
    return row ? toDomain(row) : undefined;
  }

  async findAll(request: PageRequest): Promise<Page<Customer>> {
    const [rows, total] = await this.prisma.$transaction([
      this.prisma.customer.findMany({
        take: request.pageSize,
        skip: (request.page - 1) * request.pageSize,
        orderBy: [{ createdAt: "desc" }, { id: "desc" }],
      }),
      this.prisma.customer.count(),
    ]);
    return { items: rows.map(toDomain), total };
  }

  async create(customer: Customer, context: PersistenceContext): Promise<void> {
    const events = customer.pullDomainEvents();
    try {
      await this.prisma.$transaction(async (tx) => {
        await tx.customer.create({
          data: {
            id: customer.id,
            name: customer.name,
            document: customer.document,
            email: customer.email,
            phone: customer.phone,
            authorizedTransportTypeIds: [...customer.authorizedTransportTypeIds],
            version: customer.version,
            createdAt: customer.createdAt,
            updatedAt: customer.updatedAt,
          },
        });
        if (events.length > 0) {
          await tx.outboxEvent.createMany({ data: events.map((e) => toOutboxEventData(e, context)) });
        }
      });
    } catch (err) {
      throw translateWriteError(err, customer);
    }
  }

  async update(customer: Customer, context: PersistenceContext): Promise<void> {
    const events = customer.pullDomainEvents();
    try {
      await this.prisma.$transaction(async (tx) => {
        // Compare-and-set pela versao lida: dois PUTs concorrentes no mesmo
        // cliente, so um grava; o outro recebe 409.
        const updated = await tx.customer.updateMany({
          where: { id: customer.id, version: customer.version },
          data: {
            name: customer.name,
            email: customer.email,
            phone: customer.phone,
            authorizedTransportTypeIds: [...customer.authorizedTransportTypeIds],
            updatedAt: customer.updatedAt,
            version: { increment: 1 },
          },
        });
        if (updated.count !== 1) throw new CustomerConcurrentModificationError(customer.id);
        if (events.length > 0) {
          await tx.outboxEvent.createMany({ data: events.map((e) => toOutboxEventData(e, context)) });
        }
      });
    } catch (err) {
      throw translateWriteError(err, customer);
    }
  }
}

// P2002: documento unico violado.
function translateWriteError(err: unknown, customer: Customer): unknown {
  if (err instanceof Prisma.PrismaClientKnownRequestError && err.code === "P2002") {
    return new CustomerDocumentAlreadyExistsError(customer.document);
  }
  return err;
}

function toDomain(row: CustomerModel): Customer {
  return Customer.restore({
    id: row.id,
    name: row.name,
    document: row.document,
    email: row.email,
    phone: row.phone,
    authorizedTransportTypeIds: row.authorizedTransportTypeIds,
    version: row.version,
    createdAt: row.createdAt,
    updatedAt: row.updatedAt,
  });
}
