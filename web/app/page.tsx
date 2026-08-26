"use client";

import Link from "next/link";
import { useEffect, useMemo, useRef, useState } from "react";
import {
  InstrumentSummary,
  WatchlistItem,
  addToWatchlist,
  fetchInstruments,
  fetchWatchlist,
  removeFromWatchlist,
} from "@/lib/api";
import { Tick, useTickStream } from "@/lib/useTickStream";

const FRESH_WINDOW_MS = 90_000;
const FLUSH_INTERVAL_MS = 150;
const CLOCK_INTERVAL_MS = 1000;

type TickInfo = {
  price: number;
  prevPrice: number | null;
  lastTickAt: number;
  flashClass: "flash-up" | "flash-down" | "";
  flashSeq: number;
};

// A minimum of 2 decimals matters more than it looks: without it, 1311.70
// renders as "1,311.7" and the column's width changes every time a trailing
// zero appears or vanishes. Tabular figures pin digit *width*, not digit
// *count* -- the floor is what actually stops the column twitching.
function formatPrice(price: number): string {
  return price.toLocaleString(undefined, {
    minimumFractionDigits: 2,
    maximumFractionDigits: 8,
  });
}

function formatAsOf(ms: number): string {
  return new Date(ms).toLocaleTimeString();
}

export default function WatchlistPage() {
  const [watchlist, setWatchlist] = useState<WatchlistItem[]>([]);
  const [instruments, setInstruments] = useState<InstrumentSummary[]>([]);
  const [query, setQuery] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [ticks, setTicks] = useState<Map<number, TickInfo>>(new Map());
  const [now, setNow] = useState(() => Date.now());

  const tickBufferRef = useRef<Map<number, { price: number; lastTickAt: number }>>(new Map());

  async function reload() {
    try {
      const [wl, inst] = await Promise.all([fetchWatchlist(), fetchInstruments()]);
      setWatchlist(wl);
      setInstruments(inst);
      setError(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  }

  useEffect(() => {
    // Fetch-on-mount: setState happens after the await inside reload(), not
    // synchronously in this effect body. Pre-existing pattern (same shape
    // already present in app/instrument/[id]/page.tsx); disabling the
    // lint rule's static call-graph flag rather than restructuring the
    // fetch architecture, which is out of scope for this change.
    // eslint-disable-next-line react-hooks/set-state-in-effect
    reload();
  }, []);

  // Stable array identity for useTickStream: only changes when the actual
  // set of watched instrument ids changes, not on every watchlist re-fetch.
  const idsKey = watchlist.map((w) => w.instrument_id).join(",");
  const instrumentIds = useMemo(
    () => (idsKey ? idsKey.split(",").map(Number) : []),
    [idsKey]
  );

  function handleTick(tick: Tick) {
    // Runs up to ~107x/sec for a busy crypto feed. Never setState here --
    // just accumulate into the ref; a 150ms interval below flushes it.
    tickBufferRef.current.set(tick.instrument_id, {
      price: Number(tick.price),
      lastTickAt: new Date(tick.ts).getTime(),
    });
  }

  useTickStream(instrumentIds, handleTick);

  useEffect(() => {
    const interval = setInterval(() => {
      if (tickBufferRef.current.size === 0) return;
      const pending = tickBufferRef.current;
      tickBufferRef.current = new Map();
      setTicks((prev) => {
        const next = new Map(prev);
        for (const [instrumentId, { price, lastTickAt }] of pending) {
          const prior = next.get(instrumentId);
          const prevPrice = prior ? prior.price : null;
          let flashClass: TickInfo["flashClass"] = "";
          if (prevPrice !== null) {
            if (price > prevPrice) flashClass = "flash-up";
            else if (price < prevPrice) flashClass = "flash-down";
          }
          next.set(instrumentId, {
            price,
            prevPrice,
            lastTickAt,
            flashClass,
            flashSeq: (prior?.flashSeq ?? 0) + 1,
          });
        }
        return next;
      });
    }, FLUSH_INTERVAL_MS);
    return () => clearInterval(interval);
  }, []);

  // Separate clock so the freshness ("LIVE" vs "as of ...") label keeps
  // advancing even when no new ticks are arriving at all.
  useEffect(() => {
    const clock = setInterval(() => setNow(Date.now()), CLOCK_INTERVAL_MS);
    return () => clearInterval(clock);
  }, []);

  const watchedIds = new Set(watchlist.map((w) => w.instrument_id));
  const matches = query
    ? instruments.filter(
        (i) =>
          !watchedIds.has(i.instrument_id) &&
          i.symbol.toLowerCase().includes(query.toLowerCase())
      )
    : [];

  async function handleAdd(instrumentId: number) {
    await addToWatchlist(instrumentId);
    setQuery("");
    await reload();
  }

  async function handleRemove(instrumentId: number) {
    await removeFromWatchlist(instrumentId);
    await reload();
  }

  // Count only instruments still on the watchlist. Tick entries outlive
  // removal by up to the freshness window, so counting the map directly
  // would keep reporting a symbol as streaming after it was removed.
  const liveCount = watchlist.filter((row) => {
    const t = ticks.get(row.instrument_id);
    return t !== undefined && now - t.lastTickAt < FRESH_WINDOW_MS;
  }).length;

  return (
    <main className="min-h-screen bg-ground text-text font-sans">
      <header className="flex items-center justify-between border-b border-line px-4 py-4 sm:px-8">
        <h1 className="font-display text-xl tracking-wide">TERMINAL</h1>
        <div className="flex items-center gap-2 text-xs text-muted">
          <span className="inline-block h-2 w-2 rounded-full bg-live" aria-hidden="true" />
          <span>{liveCount} streaming</span>
        </div>
      </header>

      <div className="mx-auto max-w-3xl px-4 py-8 sm:px-8">
        {error && (
          <div className="mb-6 rounded-md border border-line bg-surface px-4 py-3 text-down">
            <p>{error} It will retry on reload.</p>
          </div>
        )}

        <div className="relative mb-6">
          <input
            className="w-full rounded border border-line bg-surface px-3 py-2 text-text placeholder:text-muted focus:outline-none"
            placeholder="Add a symbol..."
            value={query}
            onChange={(e) => setQuery(e.target.value)}
          />
          {matches.length > 0 && (
            <ul className="absolute z-10 mt-1 w-full max-h-48 overflow-auto rounded border border-line bg-surface">
              {matches.slice(0, 10).map((m) => (
                <li key={m.instrument_id}>
                  <button
                    type="button"
                    className="flex w-full items-center justify-between px-3 py-2 text-left hover:bg-raised"
                    onClick={() => handleAdd(m.instrument_id)}
                  >
                    <span className="font-display">{m.symbol}</span>
                    <span className="text-xs text-muted">{m.asset_class}</span>
                  </button>
                </li>
              ))}
            </ul>
          )}
        </div>

        {watchlist.length === 0 ? (
          <p className="text-muted">No instruments yet. Search above to start tracking one.</p>
        ) : (
          <div className="overflow-x-auto rounded border border-line">
            <table className="w-full min-w-[480px] border-collapse">
              <thead>
                <tr className="border-b border-line text-left text-xs uppercase tracking-wide text-muted">
                  <th className="px-3 py-2 font-normal">Symbol</th>
                  <th className="px-3 py-2 font-normal">Market</th>
                  <th className="px-3 py-2 font-normal text-right">Last</th>
                  <th className="px-3 py-2 font-normal"></th>
                </tr>
              </thead>
              <tbody>
                {watchlist.map((row) => {
                  const tickInfo = ticks.get(row.instrument_id);
                  const price = tickInfo ? tickInfo.price : row.last_price;
                  const prevPrice = tickInfo ? tickInfo.prevPrice : null;
                  const lastTickAtMs = tickInfo
                    ? tickInfo.lastTickAt
                    : row.last_ts
                      ? new Date(row.last_ts).getTime()
                      : null;

                  let priceColor = "text-text";
                  if (prevPrice !== null && price !== null) {
                    if (price > prevPrice) priceColor = "text-up";
                    else if (price < prevPrice) priceColor = "text-down";
                  }

                  const isLive =
                    lastTickAtMs !== null && now - lastTickAtMs < FRESH_WINDOW_MS;

                  return (
                    <tr
                      key={row.instrument_id}
                      id={`watchlist-row-${row.instrument_id}`}
                      className="border-b border-line last:border-b-0 hover:bg-surface"
                    >
                      <td className="px-3 py-2">
                        <Link
                          href={`/instrument/${row.instrument_id}`}
                          className="font-display"
                        >
                          {row.symbol}
                        </Link>
                      </td>
                      <td className="px-3 py-2 text-xs text-muted">
                        {row.exchange} &middot; {row.asset_class}
                      </td>
                      <td
                        id={`price-${row.instrument_id}`}
                        key={tickInfo?.flashSeq ?? 0}
                        className={`px-3 py-2 text-right ${tickInfo?.flashClass ?? ""}`}
                      >
                        {price !== null ? (
                          <div>
                            <div className={`num ${priceColor}`}>{formatPrice(price)}</div>
                            <div
                              className={`text-xs ${isLive ? "text-live" : "text-muted"}`}
                            >
                              {isLive
                                ? "LIVE"
                                : lastTickAtMs !== null
                                  ? `as of ${formatAsOf(lastTickAtMs)}`
                                  : ""}
                            </div>
                          </div>
                        ) : (
                          <span className="num text-muted">--</span>
                        )}
                      </td>
                      <td className="px-3 py-2 text-right">
                        <button
                          type="button"
                          className="text-xs text-muted hover:text-down"
                          onClick={() => handleRemove(row.instrument_id)}
                        >
                          remove
                        </button>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </main>
  );
}
