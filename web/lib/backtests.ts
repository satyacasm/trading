import type {
  BacktestMetrics,
  BacktestSummary,
  CostDrag,
  EquityPoint,
  MaxDrawdown,
  Stress,
} from "./api";

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


/** A money magnitude, grouped, sign dropped -- the surrounding words carry it. */
export function formatMoney(value: string): string {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return value;
  return Math.abs(parsed).toLocaleString("en-IN", {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  });
}

/**
 * A signed money amount, grouped, minus kept.
 *
 * The sibling `formatMoney` drops the sign because prose carries it. A
 * table column has no prose: a position marked below its average cost is
 * a loss, and rendering it as a bare magnitude states the opposite of
 * what the API sent. Use this wherever the number stands on its own.
 */
export function formatSignedMoney(value: string): string {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return value;
  return parsed.toLocaleString("en-IN", {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  });
}

/**
 * What the costs did, in one sentence.
 *
 * Three cases, because one sentence cannot cover them honestly. When the
 * strategy made something gross, costs took a share of it. When it did not,
 * there is no share to take -- and the true statement is the more useful
 * one anyway: a real run turned a 12,313 gross loss into an 80,541 net loss
 * on 68,227 of charges, which is the whole argument for showing this at all.
 */
export function describeCostDrag(drag: CostDrag): string {
  if (drag.drag !== null) {
    return `costs took ${formatPercent(drag.drag)} of the gross result`;
  }
  const gross = Number(drag.gross_pnl);
  if (gross < 0) {
    // Magnitudes, because "loss" already carries the sign: "a -12,313.50
    // gross loss" states it twice and reads as a double negative.
    return `costs turned a ${formatMoney(drag.gross_pnl)} gross loss into a ${formatMoney(
      drag.net_pnl,
    )} net loss`;
  }
  return "no gross result for costs to take a share of";
}


/**
 * Why this strategy cannot be backtested, or null if it can.
 *
 * The strategy page knows the declared interval before the button is
 * pressed, so offering a Run backtest control that can only return
 * BACKTEST_INTERVAL_UNSUPPORTED is a refusal the reader could have been
 * spared. `null` bars means the interval is unknown -- registered before
 * manifests were persisted -- and an unknown interval is not a known
 * problem, so the run is allowed and the backend decides.
 */
export function backtestBlockedReason(bars: string | null): string | null {
  if (bars === null || bars === "1d") return null;
  return `This strategy declares ${bars} bars. Backtests run on daily bars only: a multi-year intraday run is millions of bars and the sandbox receives them as one payload. Re-upload it with bars="1d" under a new version to backtest it.`;
}


/**
 * What the 2x cost-and-slippage rerun says about the edge.
 *
 * §228's argument in one line: "if the edge dies at 2x, it was never an
 * edge." The comparison is against the base run's final equity, so the
 * sentence names both numbers rather than asking the reader to hold one.
 */
export function describeStress(stress: Stress, baseFinalEquity: string | null): string {
  if (!stress.ok) {
    return `at ${stress.multiplier}x costs the run did not complete${
      stress.error ? `: ${stress.error}` : ""
    }`;
  }
  const base = Number(baseFinalEquity);
  const stressed = Number(stress.final_equity);
  if (!Number.isFinite(base) || !Number.isFinite(stressed)) {
    return `at ${stress.multiplier}x costs it ended at ${stress.final_equity}`;
  }
  const survived = stressed > base ? "gained" : "gave up";
  return `at ${stress.multiplier}x costs it ${survived} ${formatMoney(
    String(stressed - base),
  )}, ending at ${formatMoney(stress.final_equity ?? "0")}`;
}


/**
 * Why a run stopped short, when it did.
 *
 * A backtest halted by its own circuit breaker produces a curve that ends
 * mid-window, and a report that does not say so looks like a broken chart.
 * That is exactly how it was read: "the graph only shows till 2021" for a
 * run that had tripped `max_daily_loss` at bar 444 of 1,647 and stopped by
 * design.
 */
export function describeBreakerHalt(
  breakerReason: string | null,
  barCalls: number | null,
  sessions: number | null,
): string | null {
  if (!breakerReason) return null;
  const where =
    barCalls !== null && sessions !== null && barCalls < sessions
      ? ` after ${barCalls.toLocaleString("en-IN")} of ${sessions.toLocaleString("en-IN")} sessions`
      : "";
  return `This run stopped${where} because the strategy's circuit breaker tripped: ${breakerReason}. The chart ends where the run ended.`;
}

/**
 * What each metric means, and which direction is good.
 *
 * Written for someone who has not read a finance textbook, because that is
 * who a personal research lab is for. Each entry says what it measures and
 * how to read it -- a number without a direction is trivia.
 */
export const METRIC_HELP: Record<string, { what: string; good: string }> = {
  cagr: {
    what: "The rate of return per year, compounded, over the whole window.",
    good: "Compare it to the risk-free rate shown beside it, not to zero. Beating zero is easy; beating a government bond is the bar.",
  },
  total_return: {
    what: "How much the portfolio grew in total, start to finish.",
    good: "Higher is better, but a big number over many years can still be a poor annual rate. CAGR is the fairer figure.",
  },
  sharpe: {
    what: "Return above the risk-free rate, divided by how much the returns bounced around.",
    good: "Above 1 is good, above 2 is excellent, below 0 means you did worse than a risk-free asset. Negative here is a real verdict, not a rounding error.",
  },
  sortino: {
    what: "Like Sharpe, but it only counts downside moves as risk — upside volatility is not punished.",
    good: "Higher is better. It is usually above Sharpe; if it is not, the losses are the volatile part.",
  },
  calmar: {
    what: "Annual return divided by the worst peak-to-trough fall.",
    good: "Above 1 means a year of returns exceeds the worst drawdown. Below 0 means the strategy lost money.",
  },
  volatility: {
    what: "How much returns swing about, annualised.",
    good: "Lower is calmer. On its own it is neither good nor bad — it only matters next to the return it bought.",
  },
  value_at_risk_95: {
    what: "On the worst 1 day in 20, the return was at least this bad.",
    good: "Closer to zero is calmer. It describes a normal bad day, not a crisis — the worst day is usually worse.",
  },
  worst_period: {
    what: "The single worst day in the whole run.",
    good: "Closer to zero is better. Compare it to VaR: a worst day far beyond VaR means fat tails.",
  },
  max_drawdown: {
    what: "The deepest fall from a peak, and how long it lasted before recovering.",
    good: "Shallower and shorter is better. 'Not recovered' means the strategy was still underwater when the run ended.",
  },
  win_rate: {
    what: "The share of round-trip trades that made money after charges.",
    good: "High is not automatically good: a strategy can win often and lose more on the rare losses. Read it with profit factor.",
  },
  profit_factor: {
    what: "Money made on winners divided by money lost on losers.",
    good: "Above 1 means the winners outweigh the losers. Below 1 means the strategy loses money however often it wins.",
  },
  expectancy: {
    what: "The average profit or loss per trade, after charges.",
    good: "Must be positive for the strategy to make money. Multiply by trade count to get the total.",
  },
  cost_drag: {
    what: "How much of the gross result went to brokerage, taxes and fees.",
    good: "Lower is better. Frequent trading makes this dominant — it is the difference between a strategy that works on paper and one that works.",
  },
  reshuffle: {
    what: "The same trades in 1,000 different orders, showing how deep the drawdown could have been.",
    good: "If the actual drawdown sits near the best 5%, the run was lucky in its ordering and the real risk is worse than reported.",
  },
  stress: {
    what: "The same strategy re-run with double the costs and slippage.",
    good: "If the result survives, the edge is real. If it dies at 2x, it was never an edge.",
  },
};

/**
 * What funding did to a run, in one sentence.
 *
 * Signed, because funding is a transfer and not a fee: a short in a rising
 * market collects it, and describing that as a cost would invert the whole
 * point of a carry strategy. The wording therefore has to branch, the same
 * way `describeCostDrag` does.
 */
export function describeFunding(rows: { symbol: string; amount: string }[]): string {
  const total = rows.reduce((sum, row) => sum + Number(row.amount), 0);
  if (rows.length === 0 || total === 0) return "no funding settled in this window";
  const magnitude = formatMoney(String(Math.abs(total)));
  const scope = rows.length === 1 ? `on ${rows[0].symbol}` : `across ${rows.length} contracts`;
  return total > 0
    ? `this run paid ${magnitude} in funding ${scope}`
    : `this run collected ${magnitude} in funding ${scope}`;
}

/**
 * Why a position was closed by the exchange.
 *
 * Names both numbers that decided it. "Liquidated" alone tells a reader
 * that something happened; the requirement and what was left of the margin
 * tell them why, which is the question they actually have.
 */
export function describeLiquidation(event: {
  symbol: string;
  quantity: string;
  mark: string;
  equity: string;
  maintenance: string;
}): string {
  const side = Number(event.quantity) < 0 ? "short" : "long";
  return (
    `the ${side} of ${Math.abs(Number(event.quantity))} ${event.symbol} was closed at a mark of ` +
    `${formatMoney(event.mark)}: ${formatSignedMoney(event.equity)} left against a ` +
    `${formatMoney(event.maintenance)} maintenance requirement`
  );
}
