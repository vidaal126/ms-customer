import { InvalidCustomerError } from "@domain/errors/customer.errors";

export const EMAIL_MAX_LENGTH = 254;
const EMAIL_PATTERN = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;

export function normalizeEmail(raw: string): string {
  const email = raw.trim().toLowerCase();
  if (email.length > EMAIL_MAX_LENGTH || !EMAIL_PATTERN.test(email)) {
    throw new InvalidCustomerError(`Email ${raw} invalido`);
  }
  return email;
}

// Telefone brasileiro com DDD: 11 digitos (celular) ou 10 (fixo).
export const PHONE_PATTERN = /^\(\d{2}\) \d{4,5}-\d{4}$/;

export function normalizePhone(raw: string): string {
  const digits = raw.replace(/\D/g, "");
  if (raw.replace(/[\d()\-\s+]/g, "").length > 0) {
    throw new InvalidCustomerError(`Telefone ${raw} invalido`);
  }
  if (digits.length === 11) return `(${digits.slice(0, 2)}) ${digits.slice(2, 7)}-${digits.slice(7)}`;
  if (digits.length === 10) return `(${digits.slice(0, 2)}) ${digits.slice(2, 6)}-${digits.slice(6)}`;
  throw new InvalidCustomerError(`Telefone ${raw} invalido: esperado DDD + 8 ou 9 digitos`);
}
