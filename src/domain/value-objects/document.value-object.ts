import { InvalidCustomerError } from "@domain/errors/customer.errors";

// CPF normalizado: XXX.XXX.XXX-XX.
export const CPF_PATTERN = /^\d{3}\.\d{3}\.\d{3}-\d{2}$/;

// Aceita com ou sem mascara; valida os digitos verificadores (mod 11, resto
// 10 vira 0) e rejeita sequencias repetidas (111.111.111-11 passa no calculo).
export function normalizeCpf(raw: string): string {
  const digits = raw.replace(/\D/g, "");
  if (digits.length !== 11 || raw.replace(/[\d.\-\s]/g, "").length > 0) {
    throw new InvalidCustomerError(`Documento ${raw} invalido: formato esperado XXX.XXX.XXX-XX`);
  }
  if (/^(\d)\1{10}$/.test(digits) || !hasValidCheckDigits(digits)) {
    throw new InvalidCustomerError(`Documento ${raw} invalido: digitos verificadores incorretos`);
  }
  return `${digits.slice(0, 3)}.${digits.slice(3, 6)}.${digits.slice(6, 9)}-${digits.slice(9)}`;
}

function hasValidCheckDigits(digits: string): boolean {
  // digits so tem [0-9] (validado antes): charCode e seguro.
  const values = Array.from({ length: digits.length }, (_, i) => digits.charCodeAt(i) - 48);
  return checkDigit(values, 9) === values[9] && checkDigit(values, 10) === values[10];
}

// Digito na posicao `length`: soma ponderada dos `length` anteriores.
function checkDigit(values: readonly number[], length: number): number {
  let sum = 0;
  for (let i = 0; i < length; i++) sum += (values[i] ?? 0) * (length + 1 - i);
  const rest = (sum * 10) % 11;
  return rest === 10 ? 0 : rest;
}
