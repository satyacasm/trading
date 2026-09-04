import type { RegisteredStrategy, RunSummary, StrategyWindow } from "@/lib/api";

/**
 * Pure presentation logic for the strategies page. No React, no fetch --
 * kept separate so the sentences a user reads can be unit-tested against
 * the exact strings, independent of how the page renders them.
 */

/**
 * The contract's `DataRequest.bars` permits five intervals. `smoke_test`
 * only actually resolves two of them -- the rest fail bar aggregation and
 * come back REJECTED with a MANIFEST_UNRESOLVABLE finding, not a strategy
 * bug. Both lists exist so the page can say so before a user burns an
 * upload finding out.
 */
export const SERVED_BARS = ["1m", "1d"];
/**
 * Backtests are daily-only, while smoke runs also serve 1m.
 *
 * Stating only the first half is what made the two rules look like a
 * contradiction: "1m is served" on the upload page, then
 * BACKTEST_INTERVAL_UNSUPPORTED on the strategy page, with nothing
 * connecting them.
 */
export const BACKTEST_BARS = ["1d"];
export const UNSERVED_BARS = ["5m", "15m", "1h"];

/**
 * The window line, with the bar interval named alongside the session
 * count. "5 sessions" alone is ambiguous by close to two orders of
 * magnitude: five daily sessions is five `on_bar` calls, five intraday
 * sessions is hundreds of them.
 *
 * Falls back honestly rather than inventing a fact: a run with no window
 * still names the interval it looked for (so "no 1d bars" and "no bars at
 * all" read as different problems), and a run that resolved no interval at
 * all says exactly that. The unresolved case covers two rejections --
 * configure() returned no manifest, and a manifest declaring an interval
 * this platform does not serve (5m/15m/1h) -- so it must not claim the
 * strategy failed to choose one.
 */
export function describeWindow(window: StrategyWindow): string {
  if (window.start === null || window.end === null) {
    if (window.bars === null) {
      return "no window — no bar interval was resolved";
    }
    return `no ${window.bars} bars found for this universe`;
  }
  const start = window.start.slice(0, 10);
  const end = window.end.slice(0, 10);
  return `${start} → ${end} · ${window.sessions} sessions · ${window.bars} bars`;
}

/**
 * `bars_daily` rows are timestamped at session close, while the Bar
 * contract (§ Bar) defines the timestamp as the interval's *start*. A
 * daily strategy therefore runs its simulated clock a uniform one day
 * behind what the contract promises -- worth a caveat every time a daily
 * run is shown, not a one-off release note.
 */
export function dailyClockCaveat(bars: string | null): string | null {
  if (bars !== "1d") return null;
  return "Daily bars are timestamped at session close, not session start, so this run's simulated clock sits a uniform one day behind the contract's definition of the interval.";
}

/**
 * Why a run placed orders but filled none of them. "3 orders · 0 fills"
 * with no reason reads as a strategy that chose not to trade, when in
 * fact every order bounced -- this names the actual cause when one is
 * known, and says "unfilled" rather than guessing when it isn't.
 */
export function explainNoFills(summary: RunSummary): string | null {
  if (summary.fills > 0) return null;
  if (summary.orders === 0) return null;
  if (summary.rejections > 0) {
    return `${summary.rejections} of ${summary.orders} orders rejected: ${summary.rejection_reasons.join(", ")}`;
  }
  if (summary.breaker_reason !== null) {
    return `circuit breaker latched: ${summary.breaker_reason}`;
  }
  return `${summary.orders} orders placed, none filled — they never crossed`;
}

/**
 * Registered strategies in the order they are worth reading.
 *
 * Two rules, and they pull in different directions. Versions of one
 * strategy must sit together, because that adjacency is the only thing
 * that makes the registry's immutability rule visible -- 1.0.0 and 1.1.0
 * are two rows, never one row that changed. But the strategy you just
 * uploaded is the one you came to look at, and sorting groups
 * alphabetically would bury a fresh upload under whatever happens to
 * start with an earlier letter.
 *
 * So: group by name, order the groups by their most recent registration,
 * and put the newest version first inside each group. Copies the input --
 * the caller's array is React state, and sorting it in place would mutate
 * state directly.
 */
export function orderForDisplay(strategies: RegisteredStrategy[]): RegisteredStrategy[] {
  const groups = new Map<string, RegisteredStrategy[]>();
  for (const strategy of strategies) {
    const group = groups.get(strategy.name);
    if (group === undefined) groups.set(strategy.name, [strategy]);
    else group.push(strategy);
  }
  // Ties on `registered_at` are real, not hypothetical: the column
  // defaults to now(), which Postgres scopes to the *transaction*, so
  // rows written in one transaction share a timestamp to the microsecond.
  // strategy_id breaks it, which is exactly how the listing route's
  // ORDER BY breaks the same tie -- the two must not disagree about which
  // version is newer.
  const byRecency = (a: RegisteredStrategy, b: RegisteredStrategy) =>
    b.registered_at.localeCompare(a.registered_at) || b.strategy_id - a.strategy_id;
  const newest = (group: RegisteredStrategy[]) => [...group].sort(byRecency)[0];
  return [...groups.values()]
    .sort((a, b) => byRecency(newest(a), newest(b)))
    .flatMap((group) => [...group].sort(byRecency));
}
