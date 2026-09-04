"use client";

import Link from "next/link";
import { useParams } from "next/navigation";
import { useCallback, useEffect, useMemo, useState } from "react";
import { fetchLiveRun, stopLiveRun, type LiveRunDetail } from "@/lib/api";
import {
  UNDEFINED_METRIC,
  formatMoney,
  formatPercent,
  formatSignedMoney,
} from "@/lib/backtests";
import { SeriesChart, type Line } from "@/components/SeriesChart";

const UP = "#26a69a";
const DOWN = "#ef5350";

const STATUS_TONE: Record<string, string> = {
  RUNNING: "text-up",
  STOPPED: "text-muted",
  CRASHED: "text-down",
};

/**
 * Wall-clock time in the reader's own timezone.
 *
 * The API sends UTC with an offset. Slicing the ISO string would render
 * "15:49" for a fill a Mumbai reader saw at 21:19 -- fast, and wrong by
 * five and a half hours. Parse it and let the browser localise.
 */
function localTime(iso: string): string {
  return new Date(iso).toLocaleTimeString();
}

function Stat({
  label,
  value,
  tone,
}: {
  label: string;
  value: string;
  tone?: "up" | "down";
}) {
  const color =
    tone === "up" ? "text-up" : tone === "down" ? "text-down" : "text-text";
  return (
    <div className="flex flex-col gap-1">
      <span className="text-muted text-xs tracking-wide uppercase">
        {label}
      </span>
      <span className={`num text-lg ${color}`}>{value}</span>
    </div>
  );
}

export default function LiveRunPage() {
  const params = useParams<{ id: string }>();
  const liveRunId = Number(params.id);

  const [run, setRun] = useState<LiveRunDetail | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [stopping, setStopping] = useState(false);

  const load = useCallback(() => {
    fetchLiveRun(liveRunId)
      .then((detail) => {
        setRun(detail);
        setError(null);
      })
      .catch((cause: Error) => setError(cause.message));
  }, [liveRunId]);

  useEffect(() => {
    load();
    const timer = setInterval(load, 5000);
    return () => clearInterval(timer);
  }, [load]);

  const equityLines = useMemo<Line[]>(() => {
    if (!run || run.equity_curve.length === 0) return [];
    const ahead =
      Number(run.equity ?? 0) >= Number(run.equity_curve[0]?.equity ?? 0);
    return [
      {
        data: run.equity_curve.map((p) => ({
          ts: p.ts,
          value: Number(p.equity),
        })),
        color: ahead ? UP : DOWN,
        title: "equity",
      },
    ];
  }, [run]);

  async function onStop() {
    setStopping(true);
    try {
      await stopLiveRun(liveRunId);
      load();
    } catch (cause) {
      setError((cause as Error).message);
    } finally {
      setStopping(false);
    }
  }

  if (error && run === null) {
    return (
      <main className="mx-auto max-w-5xl px-6 py-10">
        <p className="text-down text-sm">{error}</p>
        <Link href="/live" className="text-live mt-4 inline-block text-sm">
          Back to live runs
        </Link>
      </main>
    );
  }
  if (run === null) {
    return (
      <main className="mx-auto max-w-5xl px-6 py-10">
        <p className="text-muted text-sm">Loading run {liveRunId}…</p>
      </main>
    );
  }

  const pnl =
    run.equity === null
      ? null
      : Number(run.equity) - Number(run.equity_curve[0]?.equity ?? 0);

  return (
    <main className="mx-auto flex max-w-5xl flex-col gap-8 px-6 py-10">
      <header className="flex flex-col gap-2">
        <Link href="/live" className="text-muted hover:text-text text-xs">
          ← live runs
        </Link>
        <div className="flex flex-wrap items-baseline justify-between gap-4">
          <h1 className="font-display text-2xl">
            {run.strategy_name} {run.strategy_version}
          </h1>
          <div className="flex items-center gap-3">
            <span
              className={`text-sm ${STATUS_TONE[run.status] ?? "text-text"}`}
            >
              {run.status}
            </span>
            {run.status === "RUNNING" ? (
              <button
                type="button"
                onClick={onStop}
                disabled={stopping}
                className="border-down/60 text-down hover:bg-raised rounded border px-3 py-1 text-xs disabled:opacity-50"
              >
                {stopping ? "Stopping…" : "Stop run"}
              </button>
            ) : null}
          </div>
        </div>
        <p className="text-muted text-sm">
          {run.portfolio_name} · {run.base_currency} · {run.bars_seen}{" "}
          {run.bars_seen === 1 ? "bar" : "bars"} · {run.orders_placed}{" "}
          {run.orders_placed === 1 ? "order" : "orders"} · {run.runtime ?? "--"}
          {run.kernel_isolated ? " · kernel isolated" : ""}
        </p>
      </header>

      {run.orders_refused > 0 ? (
        <section className="border-line bg-raised rounded border-l-2 px-4 py-3">
          <h2 className="text-muted text-sm">
            {run.orders_refused} {run.orders_refused === 1 ? "order was" : "orders were"}{" "}
            refused
          </h2>
          <p className="text-muted mt-1 text-xs">
            The gateway checked these the same way it checks a manual order, and said:
          </p>
          <p className="text-text num mt-1 text-xs">{run.last_refusal}</p>
          <p className="text-muted mt-2 text-xs">
            The run continues — a refusal is the rules working, not a crash — but a
            strategy refused on every bar will sit at zero fills until whatever it
            collided with is changed.
          </p>
        </section>
      ) : null}

      {run.stopped_reason ? (
        <section
          className={`bg-raised rounded border-l-2 px-4 py-3 ${
            run.status === "CRASHED" ? "border-down/50" : "border-line"
          }`}
        >
          <h2 className={`text-sm ${run.status === "CRASHED" ? "text-down" : "text-muted"}`}>
            {run.status === "CRASHED" ? "This run crashed" : "This run stopped"}
          </h2>
          <p className="text-muted num mt-1 text-xs break-all">
            {run.stopped_reason}
          </p>
        </section>
      ) : null}

      <section className="grid grid-cols-2 gap-6 sm:grid-cols-4">
        <Stat
          label="Equity"
          value={
            run.equity === null ? UNDEFINED_METRIC : formatMoney(run.equity)
          }
        />
        <Stat
          label="P&L this run"
          value={
            pnl === null ? UNDEFINED_METRIC : formatSignedMoney(String(pnl))
          }
          tone={pnl === null ? undefined : pnl >= 0 ? "up" : "down"}
        />
        <Stat label="Cash" value={formatMoney(run.cash_balance)} />
        <Stat
          label="Drawdown"
          value={
            run.drawdown_pct === null
              ? UNDEFINED_METRIC
              : formatPercent(String(Number(run.drawdown_pct) / 100))
          }
          tone="down"
        />
      </section>

      {equityLines.length > 0 ? (
        <section className="flex flex-col gap-2">
          <h2 className="font-display text-lg">Equity, live</h2>
          <SeriesChart lines={equityLines} height={280} />
          <p className="text-muted text-xs">
            The same number the circuit breaker watches, sampled from
            <span className="num"> portfolio_equity_snapshots</span> — so the
            chart and the limit that would pause this run cannot disagree.
            Updates every five seconds.
          </p>
        </section>
      ) : null}

      <section className="flex flex-col gap-3">
        <h2 className="font-display text-lg">Positions</h2>
        {run.positions.length === 0 ? (
          <p className="text-muted text-sm">Flat.</p>
        ) : (
          <table className="w-full text-sm">
            <thead>
              <tr className="text-muted border-line border-b text-left text-xs uppercase">
                <th className="py-2 font-normal">Instrument</th>
                <th className="py-2 text-right font-normal">Qty</th>
                <th className="py-2 text-right font-normal">Avg cost</th>
                <th className="py-2 text-right font-normal">Last</th>
                <th className="py-2 text-right font-normal">Value</th>
                <th className="py-2 text-right font-normal">Unrealised</th>
              </tr>
            </thead>
            <tbody>
              {run.positions.map((p) => (
                <tr key={p.instrument_id} className="border-line/60 border-b">
                  <td className="py-2">{p.symbol}</td>
                  <td className="num py-2 text-right">{Number(p.quantity)}</td>
                  <td className="num py-2 text-right">
                    {formatMoney(p.avg_cost)}
                  </td>
                  <td className="num py-2 text-right">
                    {p.last_price === null
                      ? UNDEFINED_METRIC
                      : formatMoney(p.last_price)}
                  </td>
                  <td className="num py-2 text-right">
                    {p.market_value === null
                      ? UNDEFINED_METRIC
                      : formatMoney(p.market_value)}
                  </td>
                  <td
                    className={`num py-2 text-right ${
                      p.unrealised_pnl === null
                        ? ""
                        : Number(p.unrealised_pnl) >= 0
                          ? "text-up"
                          : "text-down"
                    }`}
                  >
                    {p.unrealised_pnl === null
                      ? UNDEFINED_METRIC
                      : formatSignedMoney(p.unrealised_pnl)}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </section>

      <section className="flex flex-col gap-3">
        <h2 className="font-display text-lg">Trades</h2>
        {run.recent_fills.length === 0 ? (
          <p className="text-muted text-sm">
            No fills yet. Orders appear here the moment the engine fills them.
          </p>
        ) : (
          <div className="max-h-80 overflow-auto">
            <table className="w-full text-xs">
              <thead className="bg-surface sticky top-0">
                <tr className="text-muted text-left uppercase">
                  <th className="px-2 py-2 font-normal">When</th>
                  <th className="px-2 py-2 font-normal">Instrument</th>
                  <th className="px-2 py-2 font-normal">Side</th>
                  <th className="px-2 py-2 text-right font-normal">Qty</th>
                  <th className="px-2 py-2 text-right font-normal">Price</th>
                  <th className="px-2 py-2 text-right font-normal">Charges</th>
                  <th className="px-2 py-2 font-normal">Why</th>
                </tr>
              </thead>
              <tbody>
                {run.recent_fills.map((f) => (
                  <tr key={f.order_id} className="border-line/50 border-t">
                    <td className="num text-muted px-2 py-1.5">
                      {localTime(f.filled_at)}
                    </td>
                    <td className="px-2 py-1.5">{f.symbol}</td>
                    <td
                      className={`px-2 py-1.5 ${f.side === "BUY" ? "text-up" : "text-down"}`}
                    >
                      {f.side}
                    </td>
                    <td className="num px-2 py-1.5 text-right">
                      {Number(f.quantity)}
                    </td>
                    <td className="num px-2 py-1.5 text-right">
                      {formatMoney(f.price)}
                    </td>
                    <td className="num text-muted px-2 py-1.5 text-right">
                      {formatMoney(f.total_charges)}
                    </td>
                    <td className="text-muted px-2 py-1.5">{f.rationale}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </section>
    </main>
  );
}
