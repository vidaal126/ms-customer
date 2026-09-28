import type { Customer } from "@domain/entities/customer.entity";
import type { ListCustomersOutput } from "@application/use-cases/customer.use-cases";
import type { CustomerResponseDto, PaginatedCustomersResponseDto } from "@infrastructure/http/dto/customer.dto";

export function toCustomerResponse(customer: Customer): CustomerResponseDto {
  return {
    id: customer.id,
    name: customer.name,
    document: customer.document,
    email: customer.email,
    phone: customer.phone,
    authorizedTransportTypeIds: [...customer.authorizedTransportTypeIds],
    createdAt: customer.createdAt.toISOString(),
    updatedAt: customer.updatedAt.toISOString(),
  };
}

export function toPaginatedCustomersResponse(output: ListCustomersOutput): PaginatedCustomersResponseDto {
  return {
    items: output.items.map(toCustomerResponse),
    total: output.total,
    page: output.page,
    pageSize: output.pageSize,
  };
}
