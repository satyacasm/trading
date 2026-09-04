import type { BacktestMetrics, BacktestSummary, EquityPoint, MaxDrawdown } from "./api";

/**
 * Presentation logic for the backtest report, kept pure and out of the page
 * component so the sentences a reader sees can be tested against their exact
 * text -- the same reason `lib/strategies.ts` exists.
 */

/**
 * What an undefined metric looks like. A dash, not "null" and not "0": one
 * is noise and the other is a number, and a Sharpe of zero is a claim the
 * data does not support.
 */
export const UNDEFINED_METRIC = "--";

function toNumber(value: string | null | undefined): number | null {
  if (value === null || value === undefined) return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

/** A ratio as a percentage, two decimals. */
export function formatPercent(value: string | null | undefined): string {
  const parsed = toNumber(value);
  if (parsed === null) return UNDEFINED_METRIC;
  return `${(parsed * 100).toFixed(2)}%`;
}

/** A bare ratio -- Sharpe, Sortino, Calmar -- to two decimals. */
export function formatRatio(value: string | null | undefined): string {
  const parsed = toNumber(value);
  if (parsed === null) return UNDEFINED_METRIC;
  return parsed.toFixed(2);
}

/**
 * The report's headline: growth stated against the rate it is judged by.
 *
 * The whole reason the page leads with this rather than with the total
 * return is that "+5.59% over 6.6 years" reads as a success and is not one:
 * the same run is 0.82% a year against a 6.50% risk-free rate.
 */
export function describeHurdle(metrics: BacktestMetrics): string | null {
  const growth = toNumber(metrics.cagr);
  if (growth === null) return null;
  return `${formatPercent(metrics.cagr)} a year against a ${formatPercent(
    metrics.risk_free,
  )} risk-free rate`;
}

/**
 * The comparison clause on its own, for a layout that shows the growth
 * figure large and the rate it is judged against beside it.
 *
 * A separate function rather than trimming a prefix off `describeHurdle`:
 * string surgery on a tested sentence is how the two quietly stop matching.
 */
export function describeHurdleRate(metrics: BacktestMetrics): string {
  return `a year against a ${formatPercent(metrics.risk_free)} risk-free rate`;
}

/** True when the strategy failed to clear the risk-free rate. */
export function lostToTheHurdle(metrics: BacktestMetrics): boolean {
  const growth = toNumber(metrics.cagr);
  const hurdle = toNumber(metrics.risk_free);
  return growth !== null && hurdle !== null && growth < hurdle;
}

/**
 * The drawdown in one line, saying "not recovered" in words.
 *
 * An open drawdown rendered as a closed one understates the risk the
 * strategy is still carrying, which is the single most misleading thing
 * this panel could do -- so the state is spelled out rather than implied by
 * a missing recovery date.
 */
export function describeDrawdown(dd: MaxDrawdown | null): string | null {
  if (dd === null) return null;
  const state = dd.recovered ? "recovered" : "not recovered";
  return `${formatPercent(dd.depth)} over ${dd.sessions} sessions, ${state} after ${dd.days} days`;
}

/**
 * The window as asked for, what ran, and the warm-up actually served.
 *
 * The warm-up shortfall is named when there is one: a strategy warmed on 4
 * of the 200 bars it requested is a different experiment from the one that
 * was requested.
 */
export function describeRunWindow(run: BacktestSummary): string {
  const warmUp =
    run.history_bars_available < run.history_bars_requested
      ? `${run.history_bars_available} of ${run.history_bars_requested} bars warm-up`
      : `${run.history_bars_requested} bars warm-up`;
  return `${run.requested_start} to ${run.requested_end} · ${run.sessions} sessions · ${warmUp}`;
}

/**
 * The risk-free growth line: the starting equity compounded at `riskFree`
 * over calendar time, sampled at each curve timestamp.
 *
 * Calendar time, not sessions, matching how `cagr` is computed server-side.
 * Drawing it beside the equity curve is what turns an abstract Sharpe into
 * a band a reader can see.
 */
export function riskFreeCurve(
  points: EquityPoint[],
  riskFree: string,
): { ts: string; value: number }[] {
  if (points.length === 0) return [];
  const rate = toNumber(riskFree) ?? 0;
  const start = toNumber(points[0].equity) ?? 0;
  const startMs = Date.parse(points[0].ts);
  const msPerYear = 365 * 24 * 60 * 60 * 1000;
  return points.map((point) => {
    const years = (Date.parse(point.ts) - startMs) / msPerYear;
    return { ts: point.ts, value: start * Math.pow(1 + rate, years) };
  });
}
