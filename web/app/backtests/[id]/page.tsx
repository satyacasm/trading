"use client";

import Link from "next/link";
import { useParams } from "next/navigation";
import { useEffect, useMemo, useState } from "react";
import { fetchBacktest, type BacktestDetail } from "@/lib/api";
import {
  UNDEFINED_METRIC,
  describeDrawdown,
  describeBreakerHalt,
  describeCostDrag,
  describeStress,
  describeHurdleRate,
  describeRunWindow,
  formatPercent,
  formatRatio,
  lostToTheHurdle,
  riskFreeCurve,
} from "@/lib/backtests";
import { MetricHelp } from "@/components/MetricHelp";
import { MonthlyReturns } from "@/components/MonthlyReturns";
import { SeriesChart, type Line, type Marker } from "@/components/SeriesChart";

const UP = "#26a69a";
const DOWN = "#ef5350";
const MUTED = "#7a8794";
const LIVE = "#4da3ff";

// Beyond this the arrows overlap into noise; the caption says so.
const MAX_MARKERS = 120;

// The table scrolls, but rendering thousands of rows stalls the page.
const MAX_TRADE_ROWS = 500;

function Stat({
  label,
  value,
  tone,
  help,
}: {
  label: string;
  value: string;
  tone?: "up" | "down";
  help?: string;
}) {
  const color = tone === "up" ? "text-up" : tone === "down" ? "text-down" : "text-text";
  return (
    <div className="flex flex-col gap-1">
      <span className="text-muted flex items-center text-xs tracking-wide uppercase">
        {label}
        {help ? <MetricHelp metric={help} /> : null}
      </span>
      <span className={`num text-lg ${color}`}>{value}</span>
    </div>
  );
}

export default function BacktestReportPage() {
  const params = useParams<{ id: string }>();
  const runId = Number(params.id);

  const [run, setRun] = useState<BacktestDetail | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    fetchBacktest(runId)
      .then((detail) => {
        if (!cancelled) setRun(detail);
      })
      .catch((cause: Error) => {
        if (!cancelled) setError(cause.message);
      });
    return () => {
      cancelled = true;
    };
  }, [runId]);

  const metrics = run?.metrics ?? null;

  const hurdleLines = useMemo<Line[]>(() => {
    if (!run || !metrics) return [];
    const equity = run.equity_curve.map((point) => ({
      ts: point.ts,
      value: Number(point.equity),
    }));
    const hurdle = riskFreeCurve(run.equity_curve, metrics.risk_free);
    return [
      { data: hurdle, color: MUTED, title: "risk-free" },
      { data: equity, color: lostToTheHurdle(metrics) ? DOWN : UP, title: "equity" },
    ];
  }, [run, metrics]);

  const tradeMarkers = useMemo<Marker[]>(() => {
    if (!run) return [];
    // Capped: a 659-fill run would put an arrow on nearly every bar and the
    // chart would be unreadable. The cap is stated in the caption rather
    // than silently applied.
    return run.fills_ledger.slice(0, MAX_MARKERS).map((fill) => ({
      ts: fill.ts,
      side: fill.side === "BUY" ? ("BUY" as const) : ("SELL" as const),
      text: `${fill.side} ${Number(fill.quantity)} @ ${Number(fill.price).toLocaleString("en-IN")}`,
    }));
  }, [run]);

  const underwater = useMemo<Line[]>(() => {
    if (!metrics) return [];
    return [
      {
        data: metrics.drawdown_curve.map((point) => ({
          ts: point.ts,
          value: Number(point.drawdown),
        })),
        color: DOWN,
        area: true,
        title: "drawdown",
      },
    ];
  }, [metrics]);

  const rolling = useMemo<Line[]>(() => {
    if (!metrics || metrics.rolling_sharpe.length === 0) return [];
    return [
      {
        data: metrics.rolling_sharpe.map((point) => ({
          ts: point.ts,
          value: Number(point.sharpe),
        })),
        color: LIVE,
        title: "rolling Sharpe",
      },
    ];
  }, [metrics]);

  if (error) {
    return (
      <main className="mx-auto max-w-5xl px-6 py-10">
        <p className="text-down">{error}</p>
        <Link href="/strategies" className="text-live mt-4 inline-block text-sm">
          Back to strategies
        </Link>
      </main>
    );
  }

  if (!run) {
    return (
      <main className="mx-auto max-w-5xl px-6 py-10">
        <p className="text-muted text-sm">Loading run {runId}…</p>
      </main>
    );
  }

  const drawdownLine = describeDrawdown(metrics?.max_drawdown ?? null);
  const halt = describeBreakerHalt(run.breaker_reason, run.bar_calls, run.sessions);
  const behind = metrics ? lostToTheHurdle(metrics) : false;

  return (
    <main className="mx-auto flex max-w-5xl flex-col gap-8 px-6 py-10">
      <header className="flex flex-col gap-2">
        <Link
          href={`/strategies/${run.strategy_id}`}
          className="text-muted hover:text-text text-xs"
        >
          ← strategy {run.strategy_id}
        </Link>
        <div className="flex flex-wrap items-baseline justify-between gap-4">
          <h1 className="font-display text-2xl">Backtest {run.backtest_run_id}</h1>
          <span className="text-muted text-xs">
            {run.status} · {run.runtime}
            {run.kernel_isolated ? " · kernel isolated" : ""}
          </span>
        </div>
        <p className="text-muted text-sm">
          {describeRunWindow(run)}
          {run.max_daily_loss || run.max_drawdown_pct
            ? ` · limits for this run: ${[
                run.max_daily_loss ? `max daily loss ${run.max_daily_loss}` : null,
                run.max_drawdown_pct ? `max drawdown ${run.max_drawdown_pct}%` : null,
              ]
                .filter(Boolean)
                .join(", ")}`
            : ""}
        </p>
      </header>

      {halt ? (
        <section className="border-down/50 bg-raised rounded border-l-2 px-4 py-3">
          <h2 className="text-down text-sm">This run stopped early</h2>
          <p className="text-muted mt-1 text-xs leading-relaxed">{halt}</p>
        </section>
      ) : null}

      {(run.notes ?? []).length > 0 ? (
        <section className="border-line bg-raised rounded border-l-2 px-4 py-3">
          <h2 className="text-muted text-sm">Worth knowing</h2>
          {(run.notes ?? []).map((note, index) => (
            <p key={index} className="text-muted mt-1 text-xs leading-relaxed">
              {note}
            </p>
          ))}
        </section>
      ) : null}

      {run.error ? (
        <section className="border-line bg-surface rounded border p-4">
          <h2 className="text-down text-sm">This run did not finish</h2>
          <p className="num text-muted mt-2 text-xs break-all">{run.error}</p>
        </section>
      ) : null}

      {/* The hero. Growth against the rate it is judged by, not the headline
          return -- +5.59% over 6.6 years reads as a success and is not one. */}
      <section className="flex flex-col gap-3">
        <div className="flex flex-wrap items-baseline gap-x-3">
          <span className={`num text-3xl ${behind ? "text-down" : "text-up"}`}>
            {formatPercent(metrics?.cagr)}
          </span>
          {/* The tested clause, not a re-typing of it: duplicating the
              wording in JSX would let the page and the unit test drift
              while the test stayed green. */}
          <span className="text-muted text-sm">
            {metrics ? describeHurdleRate(metrics) : "no growth figure"}
          </span>
        </div>
        <p className="text-muted text-xs">
          {behind
            ? "This strategy returned less than a government bond over the same period."
            : "This strategy cleared the risk-free rate."}{" "}
          Total return {formatPercent(metrics?.total_return)}.
        </p>
        {hurdleLines.length > 0 ? (
          <SeriesChart lines={hurdleLines} markers={tradeMarkers} height={320} />
        ) : null}
        <p className="text-muted text-xs">
          Equity against the same capital compounded at the risk-free rate. The gap between
          the lines is the result.
          {run.fills_ledger.length > 0
            ? ` Arrows mark trades${
                run.fills_ledger.length > MAX_MARKERS
                  ? ` — the first ${MAX_MARKERS} of ${run.fills_ledger.length}, beyond which they overlap into noise`
                  : ""
              }.`
            : ""}
        </p>
      </section>

      <section className="border-line grid grid-cols-2 gap-6 border-t pt-6 sm:grid-cols-4">
        <Stat label="Sharpe" help="sharpe" value={formatRatio(metrics?.sharpe)} tone={behind ? "down" : "up"} />
        <Stat label="Sortino" help="sortino" value={formatRatio(metrics?.sortino)} />
        <Stat label="Calmar" help="calmar" value={formatRatio(metrics?.calmar)} />
        <Stat label="Volatility" help="volatility" value={formatPercent(metrics?.volatility)} />
        <Stat label="VaR 95" help="value_at_risk_95" value={formatPercent(metrics?.value_at_risk_95)} />
        <Stat
          label="Worst day" help="worst_period"
          value={metrics?.worst_period ? formatPercent(metrics.worst_period.return) : UNDEFINED_METRIC}
        />
        <Stat label="Fills" value={String(run.fills)} />
        <Stat label="Final equity" value={run.final_equity ?? UNDEFINED_METRIC} />
      </section>

      {drawdownLine ? (
        <section className="border-line flex flex-col gap-3 border-t pt-6">
          <div className="flex flex-wrap items-baseline gap-x-3">
            <h2 className="font-display flex items-center text-lg">
              Drawdown
              <MetricHelp metric="max_drawdown" />
            </h2>
            <span className="num text-muted text-sm">{drawdownLine}</span>
          </div>
          {underwater.length > 0 ? (
            <SeriesChart lines={underwater} height={180} priceFormat="percent" />
          ) : null}
        </section>
      ) : null}

      {metrics?.cost_drag && metrics.trades ? (
        <section className="border-line flex flex-col gap-4 border-t pt-6">
          <div className="flex flex-wrap items-baseline gap-x-3">
            <h2 className="font-display flex items-center text-lg">
              What it cost
              <MetricHelp metric="cost_drag" />
            </h2>
            {/* The tested sentence, not a re-typing of it. */}
            <span className="text-muted text-sm">{describeCostDrag(metrics.cost_drag)}</span>
          </div>
          <div className="grid grid-cols-2 gap-6 sm:grid-cols-4">
            <Stat label="Gross P&L" value={metrics.cost_drag.gross_pnl} />
            <Stat label="Charges" value={metrics.cost_drag.total_charges} tone="down" />
            <Stat label="Net P&L" value={metrics.cost_drag.net_pnl} />
            <Stat label="Trades" value={String(metrics.trades.trades)} />
            <Stat label="Win rate" help="win_rate" value={formatPercent(metrics.trades.win_rate)} />
            <Stat label="Profit factor" help="profit_factor" value={formatRatio(metrics.trades.profit_factor)} />
            <Stat label="Average win" value={metrics.trades.average_win ?? UNDEFINED_METRIC} />
            <Stat label="Average loss" value={metrics.trades.average_loss ?? UNDEFINED_METRIC} />
          </div>
          <p className="text-muted text-xs">
            A round trip is a FIFO match: each sell closes the oldest open buy on the same
            instrument, and a position still open at the end is counted neither way. Win and
            loss are measured after charges.
          </p>
        </section>
      ) : null}

      {run.stress || run.reshuffle ? (
        <section className="border-line flex flex-col gap-4 border-t pt-6">
          <h2 className="font-display flex items-center text-lg">
            How fragile is this
            <MetricHelp metric="reshuffle" />
          </h2>
          {run.stress ? (
            <p className="text-muted text-sm">
              {describeStress(run.stress, run.final_equity)}. If the edge dies at{" "}
              {run.stress.multiplier}x, it was never an edge.
            </p>
          ) : null}
          {run.reshuffle ? (
            <>
              <div className="grid grid-cols-3 gap-6">
                {/* The 5th is not smaller or greyer than the 50th: §228 asks
                    for the bad tail shown as prominently as the middle,
                    because the middle is the one that flatters. */}
                <Stat
                  label="Worst 5% drawdown"
                  value={formatPercent(run.reshuffle.max_drawdown.p5)}
                  tone="down"
                />
                <Stat
                  label="Median drawdown"
                  value={formatPercent(run.reshuffle.max_drawdown.p50)}
                />
                <Stat
                  label="Best 5% drawdown"
                  value={formatPercent(run.reshuffle.max_drawdown.p95)}
                />
              </div>
              <p className="text-muted text-xs">
                Across {run.reshuffle.iterations.toLocaleString("en-IN")} reshuffles of the
                trade order. Terminal equity is identical in every ordering — addition does
                not care about sequence — so this spread is about the path, not the outcome.
                Reshuffling also removes serial correlation, so a strategy whose losses
                genuinely cluster will look better here than it was.
              </p>
            </>
          ) : null}
        </section>
      ) : null}

      {run.fills_ledger.length > 0 ? (
        <section className="border-line flex flex-col gap-3 border-t pt-6">
          <div className="flex flex-wrap items-baseline gap-x-3">
            <h2 className="font-display text-lg">Every trade</h2>
            <span className="text-muted text-sm">
              {run.fills_ledger.length.toLocaleString("en-IN")} fills
              {run.fills_ledger.length > MAX_TRADE_ROWS
                ? `, showing the first ${MAX_TRADE_ROWS}`
                : ""}
            </span>
          </div>
          <div className="max-h-96 overflow-auto">
            <table className="w-full text-xs">
              <thead className="bg-surface sticky top-0">
                <tr className="text-muted text-left uppercase">
                  <th className="px-2 py-2 font-normal">When</th>
                  <th className="px-2 py-2 font-normal">Side</th>
                  <th className="px-2 py-2 text-right font-normal">Qty</th>
                  <th className="px-2 py-2 text-right font-normal">Price</th>
                  <th className="px-2 py-2 text-right font-normal">Charges</th>
                  <th className="px-2 py-2 font-normal">Why</th>
                </tr>
              </thead>
              <tbody>
                {run.fills_ledger.slice(0, MAX_TRADE_ROWS).map((fill) => (
                  <tr key={fill.ordinal} className="border-line/50 border-t">
                    <td className="num text-muted px-2 py-1.5">{fill.ts.slice(0, 10)}</td>
                    <td
                      className={`px-2 py-1.5 ${fill.side === "BUY" ? "text-up" : "text-down"}`}
                    >
                      {fill.side}
                    </td>
                    <td className="num px-2 py-1.5 text-right">{Number(fill.quantity)}</td>
                    <td className="num px-2 py-1.5 text-right">{fill.price}</td>
                    <td className="num text-muted px-2 py-1.5 text-right">
                      {fill.total_charges}
                    </td>
                    {/* The strategy's own words. The contract requires a
                        rationale on every order and this is the only place
                        a reader ever sees one. */}
                    <td className="text-muted px-2 py-1.5">{fill.rationale ?? "--"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>
      ) : null}

      {metrics && metrics.monthly_returns.length > 0 ? (
        <section className="border-line flex flex-col gap-3 border-t pt-6">
          <h2 className="font-display text-lg">Monthly returns</h2>
          <MonthlyReturns months={metrics.monthly_returns} />
        </section>
      ) : null}

      {rolling.length > 0 ? (
        <section className="border-line flex flex-col gap-3 border-t pt-6">
          <h2 className="font-display text-lg">Rolling 6-month Sharpe</h2>
          <SeriesChart lines={rolling} height={180} />
          <p className="text-muted text-xs">
            At the same {formatPercent(metrics?.risk_free)} risk-free rate. Starts once six
            months of returns are available.
          </p>
        </section>
      ) : null}
    </main>
  );
}
