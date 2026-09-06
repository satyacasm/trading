"use client";

import Link from "next/link";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  ApiError,
  cancelOrder,
  createPortfolio,
  fetchInstruments,
  fetchOrders,
  fetchPerpPositions,
  fetchPositions,
  type InstrumentSummary,
  type Order,
  type OrderStatus,
  type PerpPosition,
  type Position,
} from "@/lib/api";
import { usePortfolios } from "@/lib/usePortfolios";
import { unrealisedPnl } from "@/lib/trade";
import { useTickStream, type Tick } from "@/lib/useTickStream";

// Orders reach their terminal state through the engine, not through the
// request that created them: a market order is PENDING when POST returns and
// FILLED a tick later. There is no order-event socket, so the blotter polls.
const POLL_MS = 2000;
const TICK_FLUSH_MS = 200;

/**
 * Status colour follows the palette's two-channel rule (globals.css):
 * up/down mean *direction*, `live` means *the system is working on this*.
 * An OPEN or PENDING order is exactly the latter -- it is not "good news",
 * it is unfinished -- so it takes `live` rather than a green that would
 * read as a profitable fill at a glance.
 */
const STATUS_TONE: Record<OrderStatus, string> = {
  PENDING: "text-live",
  OPEN: "text-live",
  PARTIALLY_FILLED: "text-live",
  FILLED: "text-up",
  REJECTED: "text-down",
  CANCELLED: "text-muted",
  EXPIRED: "text-muted",
};

const TERMINAL: ReadonlySet<OrderStatus> = new Set<OrderStatus>([
  "FILLED",
  "CANCELLED",
  "REJECTED",
  "EXPIRED",
]);

function formatMoney(value: number): string {
  return value.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

function formatQuantity(value: number): string {
  return value.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 8 });
}

function formatTime(iso: string): string {
  return new Date(iso).toLocaleTimeString();
}

export default function PortfolioPage() {
  const { portfolios, selected, selectedId, setSelectedId, error: portfolioError, reload } =
    usePortfolios();

  const [positions, setPositions] = useState<Position[]>([]);
  const [perps, setPerps] = useState<PerpPosition[]>([]);
  const [orders, setOrders] = useState<Order[]>([]);
  const [instruments, setInstruments] = useState<Map<number, InstrumentSummary>>(new Map());
  const [marks, setMarks] = useState<Map<number, number>>(new Map());
  const [error, setError] = useState<string | null>(null);
  const [showNewForm, setShowNewForm] = useState(false);

  const markBufferRef = useRef<Map<number, number>>(new Map());

  const refresh = useCallback(async () => {
    if (selectedId === null) {
      setPositions([]);
      setOrders([]);
      return;
    }
    try {
      const [pos, ords, perpRows] = await Promise.all([
        fetchPositions(selectedId),
        fetchOrders(selectedId),
        fetchPerpPositions(selectedId),
      ]);
      setPositions(pos);
      setOrders(ords);
      setPerps(perpRows);
      setError(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  }, [selectedId]);

  useEffect(() => {
    // eslint-disable-next-line react-hooks/set-state-in-effect
    refresh();
    const timer = setInterval(() => {
      refresh();
      // Cash moves when a fill lands, so the header has to re-read too.
      reload();
    }, POLL_MS);
    return () => clearInterval(timer);
  }, [refresh, reload]);

  useEffect(() => {
    let cancelled = false;
    fetchInstruments()
      .then((list) => {
        if (cancelled) return;
        setInstruments(new Map(list.map((i) => [i.instrument_id, i])));
      })
      .catch(() => {
        // Symbols degrade to raw ids; not worth an error banner.
      });
    return () => {
      cancelled = true;
    };
  }, []);

  // Mark every held instrument, plus anything with a resting order, so the
  // blotter's limit prices can be read against the current market.
  const streamedIds = useMemo(() => {
    const ids = new Set<number>();
    for (const p of positions) if (p.quantity !== 0) ids.add(p.instrument_id);
    for (const o of orders) if (!TERMINAL.has(o.status)) ids.add(o.instrument_id);
    return [...ids].sort((a, b) => a - b);
  }, [positions, orders]);

  const onTick = useCallback((tick: Tick) => {
    // price is a JSON string on the wire, never a number.
    markBufferRef.current.set(tick.instrument_id, Number(tick.price));
  }, []);

  useTickStream(streamedIds, onTick);

  // Same discipline as the watchlist: ticks land in a ref and flush on an
  // interval, so a fast instrument cannot re-render this page per tick.
  useEffect(() => {
    const timer = setInterval(() => {
      if (markBufferRef.current.size === 0) return;
      const buffered = markBufferRef.current;
      markBufferRef.current = new Map();
      setMarks((prev) => {
        const next = new Map(prev);
        for (const [id, price] of buffered) next.set(id, price);
        return next;
      });
    }, TICK_FLUSH_MS);
    return () => clearInterval(timer);
  }, []);

  async function onCancel(orderId: number) {
    try {
      await cancelOrder(orderId);
      await refresh();
    } catch (e) {
      setError(e instanceof ApiError ? e.message : e instanceof Error ? e.message : String(e));
    }
  }

  function symbolFor(instrumentId: number): string {
    return instruments.get(instrumentId)?.symbol ?? String(instrumentId);
  }

  const held = positions.filter((p) => p.quantity !== 0);
  const currency = selected?.base_currency ?? "";

  return (
    <main className="flex flex-col min-h-full">
      <header className="flex flex-wrap items-end justify-between gap-4 border-b border-line px-4 py-4 sm:px-8">
        <div className="flex flex-col gap-1 min-w-0">
          <Link href="/" className="text-muted hover:text-text text-sm w-fit">
            ← Watchlist
          </Link>
          <h1 className="font-display text-2xl sm:text-3xl">Portfolio</h1>
          <select
            value={selectedId ?? ""}
            onChange={(e) => setSelectedId(Number(e.target.value))}
            className="bg-raised border border-line rounded px-2 py-1.5 text-sm mt-1 w-fit"
          >
            {portfolios.length === 0 && <option value="">No portfolios yet</option>}
            {portfolios.map((p) => (
              <option key={p.portfolio_id} value={p.portfolio_id}>
                {p.name} · {p.base_currency}
              </option>
            ))}
          </select>
        </div>

        {selected !== null && (
          <div className="flex items-end gap-6">
            <div className="flex flex-col items-end">
              <span className="text-muted text-xs uppercase tracking-wide">Cash</span>
              <span className="num text-2xl">{formatMoney(selected.cash_balance)}</span>
              <span className="text-muted text-xs">{currency}</span>
            </div>
            <div className="flex flex-col items-end">
              <span className="text-muted text-xs uppercase tracking-wide">Status</span>
              <span
                className={`font-display text-sm ${
                  selected.status === "ACTIVE" ? "text-up" : "text-down"
                }`}
              >
                {selected.status}
              </span>
              {selected.status !== "ACTIVE" && (
                <span className="text-muted text-xs">breaker tripped</span>
              )}
            </div>
          </div>
        )}
      </header>

      <div className="px-4 py-4 sm:px-8 flex flex-col gap-8">
        {portfolioError !== null && (
          <p className="text-down text-sm">Could not load portfolios: {portfolioError}</p>
        )}
        {error !== null && <p className="text-down text-sm">{error}</p>}

        <NewPortfolioForm
          open={showNewForm}
          onToggle={() => setShowNewForm((v) => !v)}
          onCreated={async (created) => {
            setShowNewForm(false);
            await reload();
            setSelectedId(created.portfolio_id);
          }}
        />

        <section aria-labelledby="positions-heading">
          <h2 id="positions-heading" className="font-display text-sm tracking-wide mb-3">
            POSITIONS
          </h2>
          {held.length === 0 ? (
            <p className="text-muted text-sm">
              Nothing held.{" "}
              <Link href="/" className="underline hover:text-text">
                Pick an instrument
              </Link>{" "}
              and place an order.
            </p>
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full text-sm border border-line rounded">
                <thead>
                  <tr className="text-muted text-xs uppercase tracking-wide bg-surface">
                    <th className="text-left font-normal px-3 py-2">Symbol</th>
                    <th className="text-right font-normal px-3 py-2">Quantity</th>
                    <th className="text-right font-normal px-3 py-2">Avg cost</th>
                    <th className="text-right font-normal px-3 py-2">Mark</th>
                    <th className="text-right font-normal px-3 py-2">Unrealised</th>
                    <th className="text-right font-normal px-3 py-2">Realised</th>
                  </tr>
                </thead>
                <tbody>
                  {held.map((p) => {
                    const mark = marks.get(p.instrument_id) ?? null;
                    const pnl = unrealisedPnl(
                      { quantity: p.quantity, avgCost: p.avg_cost },
                      mark
                    );
                    const tone = pnl === null ? "text-muted" : pnl >= 0 ? "text-up" : "text-down";
                    return (
                      <tr key={p.instrument_id} className="border-t border-line">
                        <td className="px-3 py-2">
                          <Link
                            href={`/instrument/${p.instrument_id}`}
                            className="hover:text-live"
                          >
                            {symbolFor(p.instrument_id)}
                          </Link>
                        </td>
                        <td className="num text-right px-3 py-2">{formatQuantity(p.quantity)}</td>
                        <td className="num text-right px-3 py-2">{formatMoney(p.avg_cost)}</td>
                        <td className="num text-right px-3 py-2">
                          {mark !== null ? (
                            formatMoney(mark)
                          ) : (
                            <span className="text-muted" title="No tick received yet">
                              --
                            </span>
                          )}
                        </td>
                        <td className={`num text-right px-3 py-2 ${tone}`}>
                          {pnl !== null ? formatMoney(pnl) : "--"}
                        </td>
                        <td className="num text-right px-3 py-2">{formatMoney(p.realised_pnl)}</td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          )}
        </section>

        {perps.length > 0 && (
          <section aria-labelledby="perps-heading">
            <h2 id="perps-heading" className="font-display mb-3 text-sm tracking-wide">
              PERPETUALS
            </h2>
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="text-muted border-line border-b text-left text-xs uppercase">
                    <th className="px-3 py-2 font-normal">Contract</th>
                    <th className="px-3 py-2 font-normal">Side</th>
                    <th className="px-3 py-2 text-right font-normal">Size</th>
                    <th className="px-3 py-2 text-right font-normal">Entry</th>
                    <th className="px-3 py-2 text-right font-normal">Mark</th>
                    <th className="px-3 py-2 text-right font-normal">Unrealised</th>
                    <th className="px-3 py-2 text-right font-normal">Margin</th>
                    <th className="px-3 py-2 text-right font-normal">Liquidation</th>
                  </tr>
                </thead>
                <tbody>
                  {perps.map((p) => {
                    const short = Number(p.quantity) < 0;
                    const pnl = p.unrealised_pnl === null ? null : Number(p.unrealised_pnl);
                    return (
                      <tr key={p.instrument_id} className="border-line/60 border-b">
                        <td className="px-3 py-2">{p.symbol}</td>
                        <td className={`px-3 py-2 ${short ? "text-down" : "text-up"}`}>
                          {short ? "SHORT" : "LONG"} {Number(p.leverage)}x
                        </td>
                        {/* Absolute: the side column already carries the
                            sign, and a size shown as -0.01 next to the word
                            SHORT reads as a double negative. */}
                        <td className="num px-3 py-2 text-right">
                          {Math.abs(Number(p.quantity))}
                        </td>
                        <td className="num px-3 py-2 text-right">{formatMoney(Number(p.entry_price))}</td>
                        <td className="num px-3 py-2 text-right">
                          {p.mark === null ? "--" : formatMoney(Number(p.mark))}
                        </td>
                        <td
                          className={`num px-3 py-2 text-right ${
                            pnl === null ? "" : pnl >= 0 ? "text-up" : "text-down"
                          }`}
                        >
                          {pnl === null ? "--" : formatMoney(pnl)}
                        </td>
                        {/* Reserved, not spent: it is still in your cash
                            balance, just unavailable to open anything else. */}
                        <td className="num text-muted px-3 py-2 text-right">
                          {formatMoney(Number(p.reserved_margin))}
                        </td>
                        <td className="num text-down px-3 py-2 text-right">
                          {p.liquidation_price === null
                            ? "--"
                            : formatMoney(Number(p.liquidation_price))}
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
            <p className="text-muted mt-2 max-w-prose text-xs">
              Margin is reserved, not spent — it is still in your cash balance, just
              unavailable to open anything else. If the mark reaches the liquidation
              price the exchange closes the position and charges a 1.25% fee; that is not
              a circuit-breaker halt, and the rest of the portfolio keeps trading.
            </p>
          </section>
        )}

        <section aria-labelledby="orders-heading">
          <h2 id="orders-heading" className="font-display text-sm tracking-wide mb-3">
            ORDERS
          </h2>
          {orders.length === 0 ? (
            <p className="text-muted text-sm">No orders on this portfolio yet.</p>
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full text-sm border border-line rounded">
                <thead>
                  <tr className="text-muted text-xs uppercase tracking-wide bg-surface">
                    <th className="text-left font-normal px-3 py-2">#</th>
                    <th className="text-left font-normal px-3 py-2">Time</th>
                    <th className="text-left font-normal px-3 py-2">Symbol</th>
                    <th className="text-left font-normal px-3 py-2">Side</th>
                    <th className="text-left font-normal px-3 py-2">Type</th>
                    <th className="text-right font-normal px-3 py-2">Qty</th>
                    <th className="text-right font-normal px-3 py-2">Filled</th>
                    <th className="text-right font-normal px-3 py-2">Limit</th>
                    <th className="text-left font-normal px-3 py-2">Status</th>
                    <th className="text-right font-normal px-3 py-2"></th>
                  </tr>
                </thead>
                <tbody>
                  {orders.map((o) => (
                    <tr key={o.order_id} className="border-t border-line align-top">
                      <td className="num px-3 py-2 text-muted">{o.order_id}</td>
                      <td className="num px-3 py-2 text-muted">{formatTime(o.submitted_at)}</td>
                      <td className="px-3 py-2">
                        <Link href={`/instrument/${o.instrument_id}`} className="hover:text-live">
                          {symbolFor(o.instrument_id)}
                        </Link>
                      </td>
                      <td
                        className={`px-3 py-2 ${o.side === "BUY" ? "text-up" : "text-down"}`}
                      >
                        {o.side}
                      </td>
                      <td className="px-3 py-2 text-muted">{o.order_type}</td>
                      <td className="num text-right px-3 py-2">{formatQuantity(o.quantity)}</td>
                      <td className="num text-right px-3 py-2">
                        {formatQuantity(o.filled_quantity)}
                      </td>
                      <td className="num text-right px-3 py-2">
                        {o.limit_price !== null ? formatMoney(o.limit_price) : "--"}
                      </td>
                      <td className={`px-3 py-2 ${STATUS_TONE[o.status]}`}>
                        {o.status}
                        {o.rejection_reason !== null && (
                          <span className="block text-muted text-xs max-w-md mt-0.5">
                            {o.rejection_reason}
                          </span>
                        )}
                        <span className="block text-muted text-xs max-w-md mt-0.5 italic">
                          {o.rationale}
                        </span>
                      </td>
                      <td className="text-right px-3 py-2">
                        {!TERMINAL.has(o.status) && (
                          <button
                            type="button"
                            onClick={() => onCancel(o.order_id)}
                            className="text-muted hover:text-down text-xs"
                          >
                            cancel
                          </button>
                        )}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </section>
      </div>
    </main>
  );
}

function NewPortfolioForm({
  open,
  onToggle,
  onCreated,
}: {
  open: boolean;
  onToggle: () => void;
  onCreated: (created: { portfolio_id: number }) => void;
}) {
  const [name, setName] = useState("");
  const [capital, setCapital] = useState("1000000");
  const [currency, setCurrency] = useState("INR");
  const [maxDailyLoss, setMaxDailyLoss] = useState("");
  const [maxDrawdown, setMaxDrawdown] = useState("");
  const [marginMode, setMarginMode] = useState<"ISOLATED" | "CROSS">("ISOLATED");
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  async function submit(event: React.FormEvent) {
    event.preventDefault();
    if (name.trim() === "") {
      setError("Give the portfolio a name.");
      return;
    }
    setBusy(true);
    setError(null);
    try {
      const created = await createPortfolio({
        // Single-user platform (plan §12 Q1): the seeded local user.
        user_id: 1,
        name,
        initial_capital: capital,
        base_currency: currency,
        max_daily_loss: maxDailyLoss.trim() === "" ? null : maxDailyLoss,
        max_drawdown_pct: maxDrawdown.trim() === "" ? null : maxDrawdown,
        margin_mode: marginMode,
      });
      setName("");
      onCreated(created);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  }

  if (!open) {
    return (
      <button type="button" onClick={onToggle} className="text-muted hover:text-text text-sm w-fit">
        + New portfolio
      </button>
    );
  }

  return (
    <form
      onSubmit={submit}
      className="border border-line rounded bg-surface px-4 py-4 flex flex-col gap-3 max-w-2xl"
    >
      <div className="flex items-center justify-between">
        <h2 className="font-display text-sm tracking-wide">NEW PORTFOLIO</h2>
        <button type="button" onClick={onToggle} className="text-muted hover:text-text text-xs">
          close
        </button>
      </div>

      <div className="grid grid-cols-2 sm:grid-cols-3 gap-3">
        <label className="flex flex-col gap-1.5 col-span-2 sm:col-span-1">
          <span className="text-muted text-xs uppercase tracking-wide">Name</span>
          <input
            value={name}
            onChange={(e) => setName(e.target.value)}
            placeholder="Swing ideas"
            className="bg-raised border border-line rounded px-2 py-1.5 text-sm"
          />
        </label>
        <label className="flex flex-col gap-1.5">
          <span className="text-muted text-xs uppercase tracking-wide">Capital</span>
          <input
            value={capital}
            onChange={(e) => setCapital(e.target.value)}
            inputMode="decimal"
            className="num bg-raised border border-line rounded px-2 py-1.5 text-sm"
          />
        </label>
        <label className="flex flex-col gap-1.5">
          <span className="text-muted text-xs uppercase tracking-wide">Currency</span>
          <select
            value={currency}
            onChange={(e) => setCurrency(e.target.value)}
            className="bg-raised border border-line rounded px-2 py-1.5 text-sm"
          >
            <option value="INR">INR — NSE equities</option>
            <option value="USDT">USDT — crypto</option>
          </select>
        </label>
        <label className="flex flex-col gap-1.5">
          <span className="text-muted text-xs uppercase tracking-wide">Max daily loss</span>
          <input
            value={maxDailyLoss}
            onChange={(e) => setMaxDailyLoss(e.target.value)}
            inputMode="decimal"
            placeholder="optional"
            className="num bg-raised border border-line rounded px-2 py-1.5 text-sm"
          />
        </label>
        <label className="flex flex-col gap-1.5">
          <span className="text-muted text-xs uppercase tracking-wide">Max drawdown %</span>
          <input
            value={maxDrawdown}
            onChange={(e) => setMaxDrawdown(e.target.value)}
            inputMode="decimal"
            placeholder="optional"
            className="num bg-raised border border-line rounded px-2 py-1.5 text-sm"
          />
        </label>
        <label className="col-span-2 flex flex-col gap-1.5 sm:col-span-1">
          <span className="text-muted text-xs tracking-wide uppercase">Margin</span>
          <select
            value={marginMode}
            onChange={(e) => setMarginMode(e.target.value as "ISOLATED" | "CROSS")}
            className="bg-raised border-line rounded border px-2 py-1.5 text-sm"
          >
            <option value="ISOLATED">Isolated</option>
            <option value="CROSS">Cross</option>
          </select>
        </label>
      </div>

      <p className="text-muted text-xs">
        <span className="text-text">Isolated</span> backs each perpetual with only the margin
        posted for it: one position can be liquidated while the rest of the account carries on,
        and the most it can cost is what was put behind it.{" "}
        <span className="text-text">Cross</span> backs every position with the whole balance —
        positions survive much deeper drawdowns, and when the account finally cannot cover its
        total maintenance, all of them go at once. Spot is unaffected either way.
      </p>

      <p className="text-muted text-xs">
        A portfolio holds one currency. The breaker pauses it and cancels resting orders if either
        limit is breached.
      </p>

      {error !== null && <p className="text-down text-sm">{error}</p>}

      <button
        type="submit"
        disabled={busy}
        className="bg-raised border border-line rounded px-3 py-2 text-sm font-display tracking-wide hover:border-live disabled:opacity-40 w-fit"
      >
        {busy ? "Creating…" : "Create portfolio"}
      </button>
    </form>
  );
}
