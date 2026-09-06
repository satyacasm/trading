import { describe, expect, it } from "vitest";
import { estimatedNotional, unrealisedPnl, validateOrderDraft, type OrderDraft } from "./trade";

function draft(overrides: Partial<OrderDraft> = {}): OrderDraft {
  return {
    portfolioId: 1,
    side: "BUY",
    orderType: "MARKET",
    quantity: "1",
    limitPrice: "",
    rationale: "testing the ticket",
    ...overrides,
  };
}

describe("validateOrderDraft", () => {
  it("accepts a complete market order", () => {
    expect(validateOrderDraft(draft())).toBeNull();
  });

  it("rejects a missing portfolio", () => {
    expect(validateOrderDraft(draft({ portfolioId: null }))).toBe("Choose a portfolio first.");
  });

  it("rejects a non-numeric quantity", () => {
    expect(validateOrderDraft(draft({ quantity: "abc" }))).toBe("Quantity must be a number.");
  });

  it("rejects a zero quantity", () => {
    expect(validateOrderDraft(draft({ quantity: "0" }))).toBe("Quantity must be greater than zero.");
  });

  it("rejects a negative quantity", () => {
    expect(validateOrderDraft(draft({ quantity: "-1" }))).toBe(
      "Quantity must be greater than zero."
    );
  });

  // The API enforces this too (rationale is §8's journal requirement, and a
  // blank one is a 422). Catching it here turns a raw validation error into
  // a sentence next to the field that caused it.
  it("rejects a blank rationale", () => {
    expect(validateOrderDraft(draft({ rationale: "   " }))).toBe(
      "Say why you're placing this trade."
    );
  });

  it("rejects a limit order with no limit price", () => {
    expect(validateOrderDraft(draft({ orderType: "LIMIT", limitPrice: "" }))).toBe(
      "A limit order needs a limit price."
    );
  });

  it("rejects a limit order whose limit price is not a positive number", () => {
    expect(validateOrderDraft(draft({ orderType: "LIMIT", limitPrice: "0" }))).toBe(
      "Limit price must be greater than zero."
    );
  });

  it("accepts a complete limit order", () => {
    expect(validateOrderDraft(draft({ orderType: "LIMIT", limitPrice: "50000" }))).toBeNull();
  });

  // A market order carries no limit price; a stale value left in the field
  // after switching type back must not block submission.
  it("ignores a leftover limit price on a market order", () => {
    expect(validateOrderDraft(draft({ orderType: "MARKET", limitPrice: "50000" }))).toBeNull();
  });
});

describe("estimatedNotional", () => {
  it("multiplies quantity by the reference price", () => {
    expect(estimatedNotional("0.5", 77000)).toBe(38500);
  });

  it("is null when there is no reference price yet", () => {
    expect(estimatedNotional("0.5", null)).toBeNull();
  });

  it("is null when the quantity is not a usable number", () => {
    expect(estimatedNotional("", 77000)).toBeNull();
    expect(estimatedNotional("abc", 77000)).toBeNull();
  });
});

describe("unrealisedPnl", () => {
  it("is the gain over average cost for a long position", () => {
    expect(unrealisedPnl({ quantity: 2, avgCost: 100 }, 110)).toBe(20);
  });

  it("is negative when the mark is below average cost", () => {
    expect(unrealisedPnl({ quantity: 2, avgCost: 100 }, 90)).toBe(-20);
  });

  // The mark is the one input this component does not own -- it arrives
  // from the tick stream, and a position whose instrument has not ticked
  // yet has no mark at all. Returning null (rather than 0) keeps "no
  // information" distinct from "no gain", the same distinction
  // breaker.compute_equity makes when it raises MissingMark rather than
  // valuing an unmarked position at zero.
  it("is null when the instrument has not ticked yet", () => {
    expect(unrealisedPnl({ quantity: 2, avgCost: 100 }, null)).toBeNull();
  });

  it("is zero for a flat position", () => {
    expect(unrealisedPnl({ quantity: 0, avgCost: 100 }, 110)).toBe(0);
  });
});

describe("a perpetual draft", () => {
  const perp = (over: Partial<OrderDraft> = {}): OrderDraft => ({
    portfolioId: 1,
    side: "SELL",
    orderType: "MARKET",
    quantity: "0.5",
    limitPrice: "",
    rationale: "short the funding",
    leverage: "10",
    isPerpetual: true,
    ...over,
  });

  it("accepts a sell with no position, because that is how you go short", () => {
    expect(validateOrderDraft(perp())).toBeNull();
  });

  it("requires leverage, because it decides the margin locked up", () => {
    expect(validateOrderDraft(perp({ leverage: "" }))).toMatch(/leverage/i);
  });

  it("refuses leverage that is not a positive number", () => {
    expect(validateOrderDraft(perp({ leverage: "0" }))).toMatch(/leverage/i);
    expect(validateOrderDraft(perp({ leverage: "-2" }))).toMatch(/leverage/i);
    expect(validateOrderDraft(perp({ leverage: "abc" }))).toMatch(/leverage/i);
  });

  it("ignores leverage entirely on a spot draft", () => {
    // A leverage left in the field after switching instruments must not
    // reject an order the platform would happily accept -- the same
    // reasoning that lets a stale limit price through on a MARKET order.
    expect(
      validateOrderDraft({ ...perp({ isPerpetual: false, leverage: "" }), side: "BUY" }),
    ).toBeNull();
  });
});
