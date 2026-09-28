import { Body, Controller, Get, Param, ParseUUIDPipe, Post, Put, Query, UseInterceptors } from "@nestjs/common";
import {
  CreateCustomerUseCase,
  GetCustomerUseCase,
  ListCustomersUseCase,
  UpdateCustomerUseCase,
} from "@application/use-cases/customer.use-cases";
import { CorrelationId } from "./decorators/correlation-id.decorator";
import {
  CreateCustomerDto,
  type CustomerResponseDto,
  ListCustomersQueryDto,
  type PaginatedCustomersResponseDto,
  UpdateCustomerDto,
} from "./dto/customer.dto";
import { IdempotencyInterceptor } from "./interceptors/idempotency.interceptor";
import { toCustomerResponse, toPaginatedCustomersResponse } from "./mappers/customer-response.mapper";

@Controller("customers")
export class CustomerController {
  constructor(
    private readonly createCustomer: CreateCustomerUseCase,
    private readonly updateCustomer: UpdateCustomerUseCase,
    private readonly getCustomer: GetCustomerUseCase,
    private readonly listCustomers: ListCustomersUseCase,
  ) {}

  @Post()
  @UseInterceptors(IdempotencyInterceptor)
  async create(
    @Body() dto: CreateCustomerDto,
    @CorrelationId() correlationId: string,
  ): Promise<CustomerResponseDto> {
    const customer = await this.createCustomer.execute(
      {
        name: dto.name,
        document: dto.document,
        email: dto.email,
        phone: dto.phone,
        authorizedTransportTypeIds: dto.authorizedTransportTypeIds,
      },
      { correlationId },
    );
    return toCustomerResponse(customer);
  }

  @Put(":id")
  async update(
    @Param("id", ParseUUIDPipe) id: string,
    @Body() dto: UpdateCustomerDto,
    @CorrelationId() correlationId: string,
  ): Promise<CustomerResponseDto> {
    const customer = await this.updateCustomer.execute(
      {
        id,
        name: dto.name,
        email: dto.email,
        phone: dto.phone,
        authorizedTransportTypeIds: dto.authorizedTransportTypeIds,
      },
      { correlationId },
    );
    return toCustomerResponse(customer);
  }

  @Get()
  async findAll(@Query() query: ListCustomersQueryDto): Promise<PaginatedCustomersResponseDto> {
    return toPaginatedCustomersResponse(await this.listCustomers.execute(query));
  }

  @Get(":id")
  async findById(@Param("id", ParseUUIDPipe) id: string): Promise<CustomerResponseDto> {
    return toCustomerResponse(await this.getCustomer.execute(id));
  }
}
