import { canonicalJson, toJsonValue } from "./json";

describe("canonicalJson", () => {
  it("ignora a ordem das chaves, inclusive aninhadas", () => {
    expect(canonicalJson({ b: 1, a: { d: 2, c: [3, { f: 4, e: 5 }] } })).toBe(
      canonicalJson({ a: { c: [3, { e: 5, f: 4 }], d: 2 }, b: 1 }),
    );
  });

  it("diferencia valores distintos", () => {
    expect(canonicalJson({ a: 1 })).not.toBe(canonicalJson({ a: "1" }));
  });

  it("omite chaves undefined como JSON.stringify", () => {
    expect(canonicalJson({ a: 1, b: undefined })).toBe(canonicalJson({ a: 1 }));
  });
});

describe("toJsonValue", () => {
  it("converte Date e remove undefined como na serializacao HTTP", () => {
    expect(
      toJsonValue({ at: new Date("2026-09-22T00:00:00.000Z"), x: undefined }),
    ).toEqual({ at: "2026-09-22T00:00:00.000Z" });
  });

  it("devolve null para undefined", () => {
    expect(toJsonValue(undefined)).toBeNull();
  });
});
