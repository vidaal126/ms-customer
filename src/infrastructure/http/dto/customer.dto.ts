import { Type } from "class-transformer";
import {
  ArrayMaxSize,
  ArrayUnique,
  IsArray,
  IsInt,
  IsNotEmpty,
  IsOptional,
  IsString,
  IsUUID,
  Max,
  MaxLength,
  Min,
  ValidateIf,
} from "class-validator";
import { CUSTOMER_NAME_MAX_LENGTH, MAX_AUTHORIZED_TRANSPORT_TYPES } from "@domain/entities/customer.entity";
import { EMAIL_MAX_LENGTH } from "@domain/value-objects/contact.value-objects";

// Formato de CPF e telefone e validado (e normalizado) no dominio: aqui so
// tipo e tamanho. CPF com digito errado vira 422 (regra), nao 400.
export class CreateCustomerDto {
  @IsString()
  @IsNotEmpty()
  @MaxLength(CUSTOMER_NAME_MAX_LENGTH)
  readonly name!: string;

  @IsString()
  @IsNotEmpty()
  @MaxLength(20)
  readonly document!: string;

  @IsOptional()
  @IsString()
  @MaxLength(EMAIL_MAX_LENGTH)
  readonly email?: string;

  @IsOptional()
  @IsString()
  @MaxLength(20)
  readonly phone?: string;

  @IsArray()
  @ArrayMaxSize(MAX_AUTHORIZED_TRANSPORT_TYPES)
  @ArrayUnique()
  @IsUUID("all", { each: true })
  readonly authorizedTransportTypeIds!: string[];
}

// Ausente = mantem. email/phone: null limpa. name e a lista nao aceitam null
// (ValidateIf em vez de IsOptional, que deixaria null passar).
const isPresent = (_dto: object, value: unknown): boolean => value !== undefined;

export class UpdateCustomerDto {
  @ValidateIf(isPresent)
  @IsString()
  @IsNotEmpty()
  @MaxLength(CUSTOMER_NAME_MAX_LENGTH)
  readonly name?: string;

  @IsOptional()
  @IsString()
  @MaxLength(EMAIL_MAX_LENGTH)
  readonly email?: string | null;

  @IsOptional()
  @IsString()
  @MaxLength(20)
  readonly phone?: string | null;

  @ValidateIf(isPresent)
  @IsArray()
  @ArrayMaxSize(MAX_AUTHORIZED_TRANSPORT_TYPES)
  @ArrayUnique()
  @IsUUID("all", { each: true })
  readonly authorizedTransportTypeIds?: string[];
}

export class ListCustomersQueryDto {
  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(1)
  readonly page?: number;

  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(1)
  @Max(100)
  readonly limit?: number;
}

export class CustomerResponseDto {
  readonly id!: string;
  readonly name!: string;
  readonly document!: string;
  readonly email!: string | null;
  readonly phone!: string | null;
  readonly authorizedTransportTypeIds!: string[];
  readonly createdAt!: string;
  readonly updatedAt!: string;
}

export class PaginatedCustomersResponseDto {
  readonly items!: CustomerResponseDto[];
  readonly total!: number;
  readonly page!: number;
  readonly pageSize!: number;
}
