import { InvalidCustomerError } from "@domain/errors/customer.errors";
import { normalizeEmail, normalizePhone } from "./contact.value-objects";
import { normalizeCpf } from "./document.value-object";

describe("normalizeCpf", () => {
  it.each([
    ["52998224725", "529.982.247-25"],
    ["529.982.247-25", "529.982.247-25"],
    [" 111.444.777-35 ", "111.444.777-35"],
  ])("normaliza %p para %p", (raw, expected) => {
    expect(normalizeCpf(raw)).toBe(expected);
  });

  it.each([
    ["digito verificador errado", "529.982.247-24"],
    ["sequencia repetida", "111.111.111-11"],
    ["curto", "5299822472"],
    ["com letras", "529.982.247-2a"],
    ["vazio", ""],
  ])("rejeita CPF %s", (_label, raw) => {
    expect(() => normalizeCpf(raw)).toThrow(InvalidCustomerError);
  });
});

describe("normalizePhone", () => {
  it.each([
    ["11987654321", "(11) 98765-4321"],
    ["(11) 3456-7890", "(11) 3456-7890"],
    ["+ (21) 99999 0000", "(21) 99999-0000"],
  ])("normaliza %p para %p", (raw, expected) => {
    expect(normalizePhone(raw)).toBe(expected);
  });

  it.each(["123", "119876543210", "(11) 9876-54a1"])("rejeita %p", (raw) => {
    expect(() => normalizePhone(raw)).toThrow(InvalidCustomerError);
  });
});

describe("normalizeEmail", () => {
  it("normaliza para minusculas sem espacos", () => {
    expect(normalizeEmail("  Ana@Example.COM ")).toBe("ana@example.com");
  });

  it.each(["sem-arroba", "a@b", "a b@c.com"])("rejeita %p", (raw) => {
    expect(() => normalizeEmail(raw)).toThrow(InvalidCustomerError);
  });
});
