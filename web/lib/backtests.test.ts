import { describe, expect, it } from "vitest";
import {
  UNDEFINED_METRIC,
  backtestBlockedReason,
  METRIC_HELP,
  describeBreakerHalt,
  describeCostDrag,
  describeDrawdown,
  describeStress,
  describeHurdle,
  describeHurdleRate,
  describeRunWindow,
  formatPercent,
  formatRatio,
  lostToTheHurdle,
  riskFreeCurve,
} from "./backtests";
import type {
  BacktestMetrics,
  BacktestSummary,
  CostDrag,
  MaxDrawdown,
  Stress,
} from "./api";

function drawdown(overrides: Partial<MaxDrawdown> = {}): MaxDrawdown {
  return {
    depth: "-0.04048410",
    peak_ts: "2024-07-08T10:00:00+00:00",
    trough_ts: "2025-03-04T10:00:00+00:00",
    recovered_ts: null,
    recovered: false,
    sessions: 526,
    days: 774,
    ...overrides,
  };
}

function metrics(overrides: Partial<BacktestMetrics> = {}): BacktestMetrics {
  return {
    risk_free: "0.065",
    periods_per_year: 252,
    total_return: "0.05588624",
    cagr: "0.00822209",
    volatility: "0.02881832",
    sharpe: "-1.95220512",
    sortino: "-2.79651720",
    calmar: "0.20309436",
    max_drawdown: drawdown(),
    value_at_risk_95: "-0.00265014",
    worst_period: { ts: "2024-06-04T10:00:00+00:00", return: "-0.01051316" },
    drawdown_curve: [],
    monthly_returns: [],
    rolling_sharpe: [],
    ...overrides,
  };
}

function run(overrides: Partial<BacktestSummary> = {}): BacktestSummary {
  return {
    backtest_run_id: 2,
    strategy_id: 17,
    status: "PASSED",
    requested_start: "2020-01-01",
    requested_end: "2026-08-21",
    fetch_start: "2019-12-03T00:00:00+00:00",
    dispatch_from: "2020-01-01T00:00:00+00:00",
    sessions: 1647,
    instruments: [58607],
    history_bars_requested: 20,
    history_bars_available: 20,
    bars: "1d",
    bar_calls: 1647,
    orders_placed: 1,
    fills: 1,
    final_cash: "924286.2400",
    final_equity: "1055886.2400",
    breaker_reason: null,
    error: null,
    findings: [],
    runtime: "runsc",
    kernel_isolated: true,
    contract_version: "0.1",
    ran_at: "2026-09-04T06:34:59+00:00",
    max_daily_loss: null,
    max_drawdown_pct: null,
    ...overrides,
  };
}

describe("formatPercent", () => {
  it("renders a ratio as a percentage with two decimals", () => {
    expect(formatPercent("0.00822209")).toBe("0.82%");
    expect(formatPercent("-0.04048410")).toBe("-4.05%");
  });

  it("shows an undefined metric as a dash, never as null", () => {
    // The same convention /strategies already uses. Printing "null" or "0"
    // would both be claims: one is noise, the other is a number.
    expect(formatPercent(null)).toBe(UNDEFINED_METRIC);
  });
});

describe("formatRatio", () => {
  it("renders a bare ratio to two decimals", () => {
    expect(formatRatio("-1.95220512")).toBe("-1.95");
  });

  it("shows an undefined ratio as a dash", () => {
    expect(formatRatio(null)).toBe(UNDEFINED_METRIC);
  });
});

describe("describeHurdle", () => {
  it("states the growth against the rate it is being judged by", () => {
    expect(describeHurdle(metrics())).toBe(
      "0.82% a year against a 6.50% risk-free rate",
    );
  });

  it("knows when a strategy lost to doing nothing", () => {
    // The whole reason the report leads with this rather than with the
    // total return: the real run made +5.59% over 6.6 years, which reads
    // as a success, and is 0.82% a year against a 6.50% hurdle.
    expect(lostToTheHurdle(metrics())).toBe(true);
    expect(lostToTheHurdle(metrics({ cagr: "0.12000000" }))).toBe(false);
  });

  it("is undefined when CAGR could not be computed", () => {
    expect(describeHurdle(metrics({ cagr: null }))).toBe(null);
  });

  it("offers the comparison clause alone, for a large-number layout", () => {
    expect(describeHurdleRate(metrics())).toBe("a year against a 6.50% risk-free rate");
  });
});

describe("describeDrawdown", () => {
  it("says a recovered drawdown recovered, with its span", () => {
    expect(
      describeDrawdown(drawdown({ recovered: true, recovered_ts: "2025-06-02T10:00:00+00:00" })),
    ).toBe("-4.05% over 526 sessions, recovered after 774 days");
  });

  it("says NOT RECOVERED in words rather than leaving a blank", () => {
    // An open drawdown shown as closed understates the risk still being
    // carried -- the one thing this panel must never do.
    expect(describeDrawdown(drawdown())).toBe(
      "-4.05% over 526 sessions, not recovered after 774 days",
    );
  });

  it("has nothing to say when there was no drawdown", () => {
    expect(describeDrawdown(null)).toBe(null);
  });
});

describe("describeRunWindow", () => {
  it("names the requested window, the sessions, and the warm-up served", () => {
    expect(describeRunWindow(run())).toBe(
      "2020-01-01 to 2026-08-21 · 1647 sessions · 20 bars warm-up",
    );
  });

  it("says when less warm-up was available than the strategy asked for", () => {
    // A strategy warmed on 4 of the 200 bars it asked for is a different
    // experiment from the one requested.
    expect(
      describeRunWindow(run({ history_bars_requested: 200, history_bars_available: 4 })),
    ).toBe("2020-01-01 to 2026-08-21 · 1647 sessions · 4 of 200 bars warm-up");
  });
});

describe("riskFreeCurve", () => {
  it("grows the starting equity at the risk-free rate over calendar time", () => {
    const curve = riskFreeCurve(
      [
        { ts: "2023-01-01T10:00:00+00:00", equity: "100", cash: "100" },
        { ts: "2024-01-01T10:00:00+00:00", equity: "150", cash: "150" },
      ],
      "0.065",
    );
    expect(curve).toHaveLength(2);
    expect(curve[0].value).toBeCloseTo(100, 6);
    // One full year at 6.5%.
    expect(curve[1].value).toBeCloseTo(106.5, 4);
  });

  it("is empty for an empty curve", () => {
    expect(riskFreeCurve([], "0.065")).toEqual([]);
  });
});


describe("describeCostDrag", () => {
  function drag(overrides: Partial<CostDrag> = {}): CostDrag {
    return {
      total_charges: "68227.36",
      gross_pnl: "-12313.50",
      net_pnl: "-80540.86",
      drag: null,
      ...overrides,
    };
  }

  it("states the share when the strategy made something gross", () => {
    expect(describeCostDrag(drag({ gross_pnl: "100", net_pnl: "60", drag: "0.4" }))).toBe(
      "costs took 40.00% of the gross result",
    );
  });

  it("says what costs actually did when the gross result was a loss", () => {
    // From the real five-session-churn run. "costs took -554% of the gross
    // result" is not a sentence; this is, and it is more useful.
    expect(describeCostDrag(drag())).toBe(
      "costs turned a 12,313.50 gross loss into a 80,540.86 net loss",
    );
  });

  it("says there was no gross result when there was none", () => {
    expect(describeCostDrag(drag({ gross_pnl: "0", net_pnl: "-40" }))).toBe(
      "no gross result for costs to take a share of",
    );
  });
});


describe("backtestBlockedReason", () => {
  it("permits a daily strategy", () => {
    expect(backtestBlockedReason("1d")).toBe(null);
  });

  it("explains why a 1m strategy cannot be backtested, before the button is pressed", () => {
    const reason = backtestBlockedReason("1m");
    expect(reason).not.toBe(null);
    expect(reason).toContain("1m");
    expect(reason).toContain('bars="1d"');
  });

  it("permits a strategy whose interval is unknown", () => {
    // Registered before manifests were persisted. An unknown interval is
    // not a known problem, so the backend decides rather than the page
    // refusing something that might work.
    expect(backtestBlockedReason(null)).toBe(null);
  });
});


describe("describeStress", () => {
  function stress(overrides: Partial<Stress> = {}): Stress {
    return {
      multiplier: "2",
      ok: true,
      fills: 659,
      final_equity: "850000.0000",
      breaker_reason: null,
      error: null,
      ...overrides,
    };
  }

  it("names what doubling the costs took, against the base result", () => {
    // Indian grouping: 8,50,000.00, not 850,000.00. `formatMoney` uses
    // en-IN deliberately -- lakhs and crores are how this audience reads
    // money, and the earlier cost-drag figures simply never crossed a lakh
    // boundary so the difference had not shown up before.
    expect(describeStress(stress(), "919559.1400")).toBe(
      "at 2x costs it gave up 69,559.14, ending at 8,50,000.00",
    );
  });

  it("says plainly when the stressed run did not complete", () => {
    expect(describeStress(stress({ ok: false, error: "[SMOKE_OOM] killed" }), "1")).toContain(
      "did not complete",
    );
  });
});


describe("describeBreakerHalt", () => {
  it("explains a chart that ends mid-window", () => {
    // The actual bug: "the graph only shows till 2021" for a run that had
    // tripped max_daily_loss at bar 444 of 1,647 and stopped by design.
    const said = describeBreakerHalt("max_daily_loss: loss of 27746.45 exceeds 20000", 444, 1647);
    expect(said).toContain("444 of 1,647 sessions");
    expect(said).toContain("circuit breaker");
    expect(said).toContain("The chart ends where the run ended.");
  });

  it("says nothing when the run completed", () => {
    expect(describeBreakerHalt(null, 1647, 1647)).toBe(null);
  });

  it("omits the session count when the run was not cut short", () => {
    expect(describeBreakerHalt("max_drawdown: ...", 1647, 1647)).not.toContain("sessions");
  });
});

describe("METRIC_HELP", () => {
  it("gives every entry both a definition and a direction", () => {
    // A number without a direction is trivia. Each entry must say what it
    // measures AND how to read it.
    for (const [key, help] of Object.entries(METRIC_HELP)) {
      expect(help.what.length, key).toBeGreaterThan(20);
      expect(help.good.length, key).toBeGreaterThan(20);
    }
  });

  it("covers the metrics the report actually shows", () => {
    for (const key of ["cagr", "sharpe", "sortino", "calmar", "volatility", "max_drawdown",
      "win_rate", "profit_factor", "cost_drag", "reshuffle", "stress"]) {
      expect(METRIC_HELP[key], key).toBeDefined();
    }
  });
});
