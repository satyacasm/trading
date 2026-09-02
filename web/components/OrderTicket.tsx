"use client";

import Link from "next/link";
import { useState } from "react";
import { ApiError, createOrder, type Order } from "@/lib/api";
import { usePortfolios } from "@/lib/usePortfolios";
import {
  estimatedNotional,
  validateOrderDraft,
  type OrderDraft,
  type OrderSide,
  type OrderTypeName,
} from "@/lib/trade";

type Props = {
  instrumentId: number;
  symbol: string;
  /** Latest traded price, or null before the first tick arrives. */
  referencePrice: number | null;
  /** Called after a successful submit, so the page can refresh what it shows. */
  onSubmitted?: (order: Order) => void;
};

const PRODUCTS = ["DELIVERY", "INTRADAY"] as const;

function formatMoney(value: number): string {
  return value.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

export function OrderTicket({ instrumentId, symbol, referencePrice, onSubmitted }: Props) {
  const { portfolios, selected, selectedId, setSelectedId, error: portfolioError } = usePortfolios();

  const [side, setSide] = useState<OrderSide>("BUY");
  const [orderType, setOrderType] = useState<OrderTypeName>("MARKET");
  const [quantity, setQuantity] = useState("");
  const [limitPrice, setLimitPrice] = useState("");
  const [product, setProduct] = useState<(typeof PRODUCTS)[number]>("DELIVERY");
  const [rationale, setRationale] = useState("");
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [placed, setPlaced] = useState<Order | null>(null);

  const draft: OrderDraft = { portfolioId: selectedId, side, orderType, quantity, limitPrice, rationale };
  const notional = estimatedNotional(quantity, orderType === "LIMIT" ? Number(limitPrice) || null : referencePrice);

  async function submit(event: React.FormEvent) {
    event.preventDefault();
    const invalid = validateOrderDraft(draft);
    if (invalid !== null) {
      setError(invalid);
      setPlaced(null);
      return;
    }
    setSubmitting(true);
    setError(null);
    setPlaced(null);
    try {
      const order = await createOrder({
        portfolio_id: selectedId as number,
        instrument_id: instrumentId,
        side,
        order_type: orderType,
        quantity,
        limit_price: orderType === "LIMIT" ? limitPrice : null,
        product,
        time_in_force: orderType === "LIMIT" ? "GTC" : "DAY",
        rationale,
        // One key per submission. The API treats a repeated key as the same
        // order and returns the original, which is what makes a double-click
        // safe -- but a deliberate second identical order must still be a
        // second order.
        idempotency_key: `ui-${instrumentId}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
      });
      setPlaced(order);
      setQuantity("");
      setRationale("");
      onSubmitted?.(order);
    } catch (e) {
      // ApiError carries the server's own sentence -- a currency mismatch, a
      // closed market, a missing charge schedule. Shown as written.
      setError(e instanceof ApiError ? e.message : e instanceof Error ? e.message : String(e));
    } finally {
      setSubmitting(false);
    }
  }

  const sideIsBuy = side === "BUY";
  const currencyMismatch =
    selected !== null && placed === null && error !== null && error.includes("base_currency");

  return (
    <section className="border border-line rounded bg-surface" aria-label={`Order ticket for ${symbol}`}>
      <header className="flex items-center justify-between border-b border-line px-4 py-3">
        <h2 className="font-display text-sm tracking-wide">ORDER</h2>
        <Link href="/portfolio" className="text-muted hover:text-text text-xs">
          Portfolio →
        </Link>
      </header>

      <form onSubmit={submit} className="flex flex-col gap-4 px-4 py-4">
        <label className="flex flex-col gap-1.5">
          <span className="text-muted text-xs uppercase tracking-wide">Portfolio</span>
          <select
            value={selectedId ?? ""}
            onChange={(e) => setSelectedId(Number(e.target.value))}
            className="bg-raised border border-line rounded px-2 py-1.5 text-sm"
          >
            {portfolios.length === 0 && <option value="">No portfolios yet</option>}
            {portfolios.map((p) => (
              <option key={p.portfolio_id} value={p.portfolio_id}>
                {p.name} · {p.base_currency} · {formatMoney(p.cash_balance)}
              </option>
            ))}
          </select>
          {selected !== null && selected.status !== "ACTIVE" && (
            <span className="text-down text-xs">
              This portfolio is {selected.status}. The circuit breaker pauses trading; orders will
              be refused.
            </span>
          )}
        </label>

        <div className="inline-flex border border-line rounded overflow-hidden" role="group">
          {(["BUY", "SELL"] as const).map((s) => {
            const on = s === side;
            const tone = s === "BUY" ? "bg-up/20 text-up" : "bg-down/20 text-down";
            return (
              <button
                key={s}
                type="button"
                aria-pressed={on}
                onClick={() => setSide(s)}
                className={`flex-1 px-3 py-2 text-sm font-display tracking-wide transition-colors ${
                  on ? tone : "text-muted hover:text-text"
                }`}
              >
                {s}
              </button>
            );
          })}
        </div>

        <div className="grid grid-cols-2 gap-3">
          <label className="flex flex-col gap-1.5">
            <span className="text-muted text-xs uppercase tracking-wide">Quantity</span>
            <input
              value={quantity}
              onChange={(e) => setQuantity(e.target.value)}
              inputMode="decimal"
              placeholder="0.00"
              className="num bg-raised border border-line rounded px-2 py-1.5 text-sm"
            />
          </label>

          <label className="flex flex-col gap-1.5">
            <span className="text-muted text-xs uppercase tracking-wide">Type</span>
            <select
              value={orderType}
              onChange={(e) => setOrderType(e.target.value as OrderTypeName)}
              className="bg-raised border border-line rounded px-2 py-1.5 text-sm"
            >
              <option value="MARKET">Market</option>
              <option value="LIMIT">Limit</option>
            </select>
          </label>

          {orderType === "LIMIT" && (
            <label className="flex flex-col gap-1.5">
              <span className="text-muted text-xs uppercase tracking-wide">Limit price</span>
              <input
                value={limitPrice}
                onChange={(e) => setLimitPrice(e.target.value)}
                inputMode="decimal"
                placeholder={referencePrice !== null ? referencePrice.toFixed(2) : "0.00"}
                className="num bg-raised border border-line rounded px-2 py-1.5 text-sm"
              />
            </label>
          )}

          <label className="flex flex-col gap-1.5">
            <span className="text-muted text-xs uppercase tracking-wide">Product</span>
            <select
              value={product}
              onChange={(e) => setProduct(e.target.value as (typeof PRODUCTS)[number])}
              className="bg-raised border border-line rounded px-2 py-1.5 text-sm"
            >
              {PRODUCTS.map((p) => (
                <option key={p} value={p}>
                  {p === "DELIVERY" ? "Delivery" : "Intraday"}
                </option>
              ))}
            </select>
          </label>
        </div>

        <label className="flex flex-col gap-1.5">
          <span className="text-muted text-xs uppercase tracking-wide">Why this trade?</span>
          <input
            value={rationale}
            onChange={(e) => setRationale(e.target.value)}
            placeholder="Breaking out of the range on volume"
            className="bg-raised border border-line rounded px-2 py-1.5 text-sm"
          />
          <span className="text-muted text-xs">
            Recorded with the order. Every trade needs a reason you can read back later.
          </span>
        </label>

        <dl className="flex items-baseline justify-between border-t border-line pt-3 text-sm">
          <dt className="text-muted text-xs uppercase tracking-wide">Notional</dt>
          <dd className="num">
            {notional !== null ? formatMoney(notional) : "--"}
            <span className="text-muted text-xs ml-2">before charges</span>
          </dd>
        </dl>

        <button
          type="submit"
          disabled={submitting || portfolios.length === 0}
          className={`px-3 py-2.5 rounded font-display tracking-wide text-sm transition-colors disabled:opacity-40 disabled:cursor-not-allowed ${
            sideIsBuy ? "bg-up/20 text-up hover:bg-up/30" : "bg-down/20 text-down hover:bg-down/30"
          }`}
        >
          {submitting ? "Placing…" : `${side} ${symbol}`}
        </button>

        {portfolioError !== null && (
          <p className="text-down text-xs">Could not load portfolios: {portfolioError}</p>
        )}

        {error !== null && (
          <p className="text-down text-sm border border-down/30 rounded px-3 py-2" role="alert">
            {error}
            {currencyMismatch && (
              <span className="block text-muted text-xs mt-1">
                Pick a portfolio whose currency matches this instrument, or create one on the
                portfolio page.
              </span>
            )}
          </p>
        )}

        {placed !== null && (
          <p className="text-live text-sm border border-live/30 rounded px-3 py-2" role="status">
            Order #{placed.order_id} placed — {placed.status.toLowerCase()}.{" "}
            <Link href="/portfolio" className="underline hover:text-text">
              Track it
            </Link>
          </p>
        )}
      </form>
    </section>
  );
}
