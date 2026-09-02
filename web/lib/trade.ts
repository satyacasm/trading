/**
 * Pure helpers for the order ticket and the positions table.
 *
 * Kept out of the components so the rules that decide whether an order is
 * submittable — and what a position is currently worth — can be tested
 * without rendering anything.
 *
 * Deliberately absent: any estimate of charges. `compute_charges` lives on
 * the server, is driven by dated `charge_schedules` rows, and is the thing
 * the golden contract-note tests grade. A second cost model here would
 * drift from it silently, which is the exact failure the backend spent a
 * branch eliminating. The ticket shows notional and says so.
 */

export type OrderSide = "BUY" | "SELL";
export type OrderTypeName = "MARKET" | "LIMIT";

export type OrderDraft = {
  portfolioId: number | null;
  side: OrderSide;
  orderType: OrderTypeName;
  quantity: string;
  limitPrice: string;
  rationale: string;
};

/**
 * The first thing wrong with `draft`, phrased for the person who typed it,
 * or null when it is ready to submit.
 *
 * This mirrors the API's own validation rather than replacing it — the
 * server stays the authority and its rejection is shown verbatim. The
 * point of duplicating the cheap checks is that "Quantity must be greater
 * than zero" next to the quantity field beats a 422 body describing a
 * Pydantic constraint.
 */
export function validateOrderDraft(draft: OrderDraft): string | null {
  if (draft.portfolioId === null) return "Choose a portfolio first.";

  const quantity = Number(draft.quantity);
  if (draft.quantity.trim() === "" || Number.isNaN(quantity)) return "Quantity must be a number.";
  if (quantity <= 0) return "Quantity must be greater than zero.";

  if (draft.rationale.trim() === "") return "Say why you're placing this trade.";

  // Only a limit order carries a limit price, so a stale value left in the
  // field after switching back to MARKET is ignored rather than rejected.
  if (draft.orderType === "LIMIT") {
    const limit = Number(draft.limitPrice);
    if (draft.limitPrice.trim() === "") return "A limit order needs a limit price.";
    if (Number.isNaN(limit)) return "Limit price must be a number.";
    if (limit <= 0) return "Limit price must be greater than zero.";
  }

  return null;
}

/**
 * Quantity times the reference price, or null when either is unknown.
 *
 * Notional only — see this module's header for why no charge estimate
 * appears here.
 */
export function estimatedNotional(quantity: string, referencePrice: number | null): number | null {
  if (referencePrice === null) return null;
  const parsed = Number(quantity);
  if (quantity.trim() === "" || Number.isNaN(parsed)) return null;
  return parsed * referencePrice;
}

/**
 * What this position has gained or lost against its average cost at
 * `mark`, or null when the instrument has not ticked yet.
 *
 * Null rather than zero, deliberately: "we have no mark" and "this
 * position is flat" are different facts, and collapsing them would show a
 * confident 0.00 for a position nobody has priced. It is the same
 * distinction `breaker.compute_equity` makes server-side when it raises
 * `MissingMark` rather than valuing an unmarked holding at zero.
 */
export function unrealisedPnl(
  position: { quantity: number; avgCost: number },
  mark: number | null
): number | null {
  if (mark === null) return null;
  return (mark - position.avgCost) * position.quantity;
}
