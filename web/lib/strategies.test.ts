import { describe, expect, it } from "vitest";
import {
  SERVED_BARS,
  UNSERVED_BARS,
  dailyClockCaveat,
  describeWindow,
  explainNoFills,
  orderForDisplay,
} from "./strategies";
import type { RegisteredStrategy, RunSummary, StrategyWindow } from "./api";

function window(overrides: Partial<StrategyWindow> = {}): StrategyWindow {
  return {
    start: "2026-08-25T00:00:00+00:00",
    end: "2026-08-29T23:59:59+00:00",
    sessions: 5,
    bars: "1m",
    interval_sec: 60,
    ...overrides,
  };
}

function summary(overrides: Partial<RunSummary> = {}): RunSummary {
  return {
    bar_calls: 491,
    orders: 0,
    fills: 0,
    rejections: 0,
    rejection_reasons: [],
    breaker_reason: null,
    starting_cash: "100000",
    final_cash: "100000",
    final_equity: "100000",
    pnl: "0",
    pnl_pct: "0",
    currency: "INR",
    ...overrides,
  };
}

describe("describeWindow", () => {
  // "5 sessions" alone is ambiguous by a factor of ~100: five daily
  // sessions is five on_bar calls, five intraday sessions is hundreds.
  it("names the interval alongside the sessions", () => {
    expect(describeWindow(window())).toBe("2026-08-25 → 2026-08-29 · 5 sessions · 1m bars");
  });

  it("names a daily window as daily", () => {
    expect(describeWindow(window({ bars: "1d", interval_sec: 86400 }))).toBe(
      "2026-08-25 → 2026-08-29 · 5 sessions · 1d bars"
    );
  });

  // The no-data path is where the interval matters most: "no bars" and
  // "no daily bars" send you to different fixes.
  it("still names the interval when no window was found", () => {
    expect(describeWindow(window({ start: null, end: null, sessions: 0, bars: "1d" }))).toBe(
      "no 1d bars found for this universe"
    );
  });

  // Two different rejections land here: configure() never returned a
  // manifest, and a manifest that declared an interval this platform does
  // not serve. Both resolved no interval, and the wording must fit both --
  // "never got as far as choosing bars" is wrong for the second, which
  // chose one and had it refused.
  it("claims no interval when none was resolved", () => {
    expect(
      describeWindow(window({ start: null, end: null, sessions: 0, bars: null, interval_sec: null }))
    ).toBe("no window — no bar interval was resolved");
  });
});

describe("the served intervals", () => {
  // The contract permits five; this platform serves two. Saying so before
  // upload is the difference between a rejection you understand and one
  // that looks like your own bug.
  it("names only what smoke_test can actually serve", () => {
    expect(SERVED_BARS).toEqual(["1m", "1d"]);
    expect(UNSERVED_BARS).toEqual(["5m", "15m", "1h"]);
  });
});

describe("dailyClockCaveat", () => {
  // 3a's carried-forward limitation: bars_daily rows are timestamped at
  // session close while Bar.ts defines the interval start.
  it("warns on a daily run", () => {
    expect(dailyClockCaveat("1d")).toContain("session close");
  });

  it("stays quiet on an intraday run", () => {
    expect(dailyClockCaveat("1m")).toBeNull();
  });

  it("stays quiet when no interval was resolved", () => {
    expect(dailyClockCaveat(null)).toBeNull();
  });
});

describe("explainNoFills", () => {
  it("says nothing when the run filled", () => {
    expect(explainNoFills(summary({ orders: 1, fills: 1 }))).toBeNull();
  });

  it("says nothing when the strategy never ordered", () => {
    // Covered separately by the NO_ORDERS finding; repeating it here
    // would put the same fact on screen twice.
    expect(explainNoFills(summary({ orders: 0, fills: 0 }))).toBeNull();
  });

  it("names the rejection reason when every order bounced", () => {
    expect(
      explainNoFills(
        summary({ orders: 3, fills: 0, rejections: 3, rejection_reasons: ["insufficient funds"] })
      )
    ).toBe("3 of 3 orders rejected: insufficient funds");
  });

  it("names the breaker when one latched", () => {
    expect(
      explainNoFills(summary({ orders: 2, fills: 0, breaker_reason: "max daily loss exceeded" }))
    ).toBe("circuit breaker latched: max daily loss exceeded");
  });

  // Orders placed, none rejected, none filled, no breaker: the orders
  // were live and never crossed. Saying "unfilled" is the honest reading;
  // guessing at a cause would be fabrication.
  it("reports unfilled orders as unfilled rather than guessing why", () => {
    expect(explainNoFills(summary({ orders: 2, fills: 0 }))).toBe(
      "2 orders placed, none filled — they never crossed"
    );
  });
});

describe("orderForDisplay", () => {
  function registered(over: Partial<RegisteredStrategy> = {}): RegisteredStrategy {
    return {
      strategy_id: 1,
      name: "alpha",
      version: "1.0.0",
      status: "REGISTERED",
      contract_version: "0.1",
      registered_at: "2026-09-01T10:00:00+00:00",
      bars: "1m",
      latest_run: null,
      ...over,
    };
  }

  // Versions of one strategy must sit together: that adjacency is what
  // makes the registry's immutability rule legible -- you see 1.0.0 and
  // 1.1.0 as two rows, not one row that changed.
  it("keeps versions of the same strategy adjacent", () => {
    const rows = orderForDisplay([
      registered({ strategy_id: 1, name: "alpha", version: "1.0.0" }),
      registered({ strategy_id: 2, name: "zeta", version: "1.0.0" }),
      registered({ strategy_id: 3, name: "alpha", version: "1.1.0" }),
    ]);
    expect(rows.map((r) => `${r.name}@${r.version}`)).toEqual([
      "alpha@1.1.0",
      "alpha@1.0.0",
      "zeta@1.0.0",
    ]);
  });

  // The strategy you just uploaded is the one you are looking for, so its
  // group leads -- alphabetical order would bury a fresh upload under
  // whatever happens to sort earlier.
  it("puts the most recently registered strategy's group first", () => {
    const rows = orderForDisplay([
      registered({
        strategy_id: 1,
        name: "alpha",
        version: "1.0.0",
        registered_at: "2026-09-01T10:00:00+00:00",
      }),
      registered({
        strategy_id: 2,
        name: "zeta",
        version: "1.0.0",
        registered_at: "2026-09-03T10:00:00+00:00",
      }),
    ]);
    expect(rows.map((r) => r.name)).toEqual(["zeta", "alpha"]);
  });

  it("orders newest version first within a group", () => {
    const rows = orderForDisplay([
      registered({
        strategy_id: 1,
        version: "1.0.0",
        registered_at: "2026-09-01T10:00:00+00:00",
      }),
      registered({
        strategy_id: 2,
        version: "2.0.0",
        registered_at: "2026-09-02T10:00:00+00:00",
      }),
    ]);
    expect(rows.map((r) => r.version)).toEqual(["2.0.0", "1.0.0"]);
  });

  it("does not mutate the array it was given", () => {
    const input = [
      registered({ strategy_id: 1, name: "zeta", registered_at: "2026-09-01T10:00:00+00:00" }),
      registered({ strategy_id: 2, name: "alpha", registered_at: "2026-09-03T10:00:00+00:00" }),
    ];
    orderForDisplay(input);
    expect(input.map((r) => r.name)).toEqual(["zeta", "alpha"]);
  });
});
