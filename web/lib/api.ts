export type InstrumentSummary = {
  instrument_id: number;
  symbol: string;
  asset_class: string;
  exchange: string;
};

export type WatchlistItem = {
  instrument_id: number;
  symbol: string;
  asset_class: string;
  exchange: string;
  added_at: string;
  last_price: number | null;
  last_ts: string | null;
};

export type Candle = {
  ts: string;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
};

export type CandlesResponse = {
  instrument_id: number;
  interval: string;
  candles: Candle[];
};

export type Interval = "1m" | "5m" | "15m" | "1h" | "1d";

const API_URL = process.env.NEXT_PUBLIC_API_URL ?? "http://localhost:8000";

export async function fetchInstruments(): Promise<InstrumentSummary[]> {
  const res = await fetch(`${API_URL}/instruments`);
  if (!res.ok) throw new Error(`GET /instruments failed: ${res.status}`);
  return res.json();
}

export async function fetchWatchlist(): Promise<WatchlistItem[]> {
  const res = await fetch(`${API_URL}/watchlist`);
  if (!res.ok) throw new Error(`GET /watchlist failed: ${res.status}`);
  return res.json();
}

export async function addToWatchlist(instrumentId: number): Promise<void> {
  const res = await fetch(`${API_URL}/watchlist`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ instrument_id: instrumentId }),
  });
  if (!res.ok) throw new Error(`POST /watchlist failed: ${res.status}`);
}

export async function removeFromWatchlist(instrumentId: number): Promise<void> {
  const res = await fetch(`${API_URL}/watchlist/${instrumentId}`, { method: "DELETE" });
  if (!res.ok) throw new Error(`DELETE /watchlist/${instrumentId} failed: ${res.status}`);
}

export async function fetchCandles(
  instrumentId: number,
  interval: Interval,
  limit = 300
): Promise<CandlesResponse> {
  const res = await fetch(
    `${API_URL}/candles/${instrumentId}?interval=${interval}&limit=${limit}`
  );
  if (!res.ok) throw new Error(`GET /candles/${instrumentId} failed: ${res.status}`);
  return res.json();
}

// --- Paper trading -----------------------------------------------------------

export type Portfolio = {
  portfolio_id: number;
  user_id: number;
  name: string;
  base_currency: string;
  initial_capital: number;
  cash_balance: number;
  status: string;
  max_daily_loss: number | null;
  max_drawdown_pct: number | null;
  /** How perpetuals here are margined. Spot is unaffected either way. */
  margin_mode?: "ISOLATED" | "CROSS";
};

export type Position = {
  portfolio_id: number;
  instrument_id: number;
  quantity: number;
  avg_cost: number;
  realised_pnl: number;
};

export type OrderStatus =
  | "PENDING"
  | "OPEN"
  | "PARTIALLY_FILLED"
  | "FILLED"
  | "CANCELLED"
  | "REJECTED"
  | "EXPIRED";

export type Order = {
  order_id: number;
  portfolio_id: number;
  instrument_id: number;
  side: "BUY" | "SELL";
  order_type: "MARKET" | "LIMIT";
  quantity: number;
  filled_quantity: number;
  limit_price: number | null;
  product: "DELIVERY" | "INTRADAY";
  time_in_force: "DAY" | "GTC";
  status: OrderStatus;
  rationale: string;
  submitted_at: string;
  rejection_reason: string | null;
};

export type CreateOrderBody = {
  portfolio_id: number;
  instrument_id: number;
  side: "BUY" | "SELL";
  order_type: "MARKET" | "LIMIT";
  quantity: string;
  limit_price: string | null;
  product: "DELIVERY" | "INTRADAY";
  time_in_force: "DAY" | "GTC";
  rationale: string;
  /** Required for a perpetual, null otherwise. The platform refuses to
   *  assume one: it decides how much margin the position locks up. */
  leverage?: string | null;
  idempotency_key: string;
};

/**
 * The API's own rejection, surfaced verbatim.
 *
 * The backend rejects for reasons the UI has no business paraphrasing --
 * a currency mismatch, a closed market, no charge schedule covering this
 * instrument today, insufficient cash. Each message names exactly what is
 * wrong, so it is shown as written rather than replaced by a generic
 * "order failed".
 */
export class ApiError extends Error {
  readonly status: number;

  constructor(status: number, detail: string) {
    super(detail);
    this.name = "ApiError";
    this.status = status;
  }
}

async function readError(res: Response, fallback: string): Promise<ApiError> {
  try {
    const body = await res.json();
    const detail = body?.detail;
    if (typeof detail === "string") return new ApiError(res.status, detail);
    // FastAPI's 422 shape: a list of per-field validation errors.
    if (Array.isArray(detail) && detail.length > 0) {
      const first = detail[0];
      const field = Array.isArray(first?.loc) ? first.loc[first.loc.length - 1] : null;
      const msg = typeof first?.msg === "string" ? first.msg : fallback;
      return new ApiError(res.status, field ? `${field}: ${msg}` : msg);
    }
  } catch {
    // Body was not JSON; fall through to the generic message.
  }
  return new ApiError(res.status, fallback);
}

export async function fetchPortfolios(): Promise<Portfolio[]> {
  const res = await fetch(`${API_URL}/portfolios`);
  if (!res.ok) throw await readError(res, `GET /portfolios failed: ${res.status}`);
  return res.json();
}

export async function createPortfolio(body: {
  user_id: number;
  name: string;
  initial_capital: string;
  base_currency: string;
  max_daily_loss: string | null;
  max_drawdown_pct: string | null;
  margin_mode?: "ISOLATED" | "CROSS";
}): Promise<Portfolio> {
  const res = await fetch(`${API_URL}/portfolios`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) throw await readError(res, `POST /portfolios failed: ${res.status}`);
  return res.json();
}

export type PerpPosition = {
  instrument_id: number;
  symbol: string;
  /** Signed: negative is short. There is no side field. */
  quantity: string;
  entry_price: string;
  leverage: string;
  /** Reserved, not spent -- it is not gone from your cash. */
  reserved_margin: string;
  realised_pnl: string;
  /** Positive means this position has paid funding; negative means it collected. */
  funding_paid: string;
  mark: string | null;
  unrealised_pnl: string | null;
  liquidation_price: string | null;
};

export async function fetchPerpPositions(portfolioId: number): Promise<PerpPosition[]> {
  const res = await fetch(`${API_URL}/portfolios/${portfolioId}/perp-positions`);
  if (!res.ok) throw await readError(res, `GET perp positions failed: ${res.status}`);
  return res.json();
}

export async function fetchPositions(portfolioId: number): Promise<Position[]> {
  const res = await fetch(`${API_URL}/portfolios/${portfolioId}/positions`);
  if (!res.ok) throw await readError(res, `GET positions failed: ${res.status}`);
  return res.json();
}

export async function fetchOrders(portfolioId: number, limit = 50): Promise<Order[]> {
  const res = await fetch(`${API_URL}/orders?portfolio_id=${portfolioId}&limit=${limit}`);
  if (!res.ok) throw await readError(res, `GET /orders failed: ${res.status}`);
  return res.json();
}

export async function createOrder(body: CreateOrderBody): Promise<Order> {
  const res = await fetch(`${API_URL}/orders`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) throw await readError(res, `POST /orders failed: ${res.status}`);
  return res.json();
}

export async function cancelOrder(orderId: number): Promise<Order> {
  const res = await fetch(`${API_URL}/orders/${orderId}`, { method: "DELETE" });
  if (!res.ok) throw await readError(res, `DELETE /orders/${orderId} failed: ${res.status}`);
  return res.json();
}

/**
 * The §9 upload pipeline: validate, smoke, register, in one request.
 *
 * Slow by construction -- three containers run before this resolves (one
 * to resolve the manifest, then the smoke payload twice so the two order
 * sequences can be compared), so a pending state is mandatory rather than
 * polish.
 *
 * A rejection comes back as 200 with `accepted: false`, not as an HTTP
 * error: the verdict is the payload. `readError` therefore only ever fires
 * here for a genuine transport or schema failure.
 */
export type StrategyVerdict = "PASSED" | "PASSED_WITH_WARNINGS" | "REJECTED";

export type StrategyFinding = {
  code: string;
  message: string;
  line: number | null;
  contract_section: string;
};

export type StrategyWindow = {
  start: string | null;
  end: string | null;
  sessions: number;
  instruments?: Record<string, { bars: number }>;
  bars: string | null;
  interval_sec: number | null;
};

/**
 * Money arrives as strings: JSON has no decimal type, so a number field
 * would be a float by the time it got here. Parsed for display only.
 */
export type RunSummary = {
  bar_calls: number;
  orders: number;
  fills: number;
  rejections: number;
  rejection_reasons: string[];
  breaker_reason: string | null;
  starting_cash: string | null;
  final_cash: string | null;
  final_equity: string | null;
  pnl: string | null;
  pnl_pct: string | null;
  currency: string;
};

export type UploadStrategyResult = {
  accepted: boolean;
  verdict: StrategyVerdict;
  strategy_id: number | null;
  feedback: string;
  findings: StrategyFinding[];
  window: StrategyWindow | null;
  runtime: string | null;
  kernel_isolated: boolean | null;
  summary: RunSummary | null;
};

export async function uploadStrategy(body: {
  name: string;
  version: string;
  source: string;
}): Promise<UploadStrategyResult> {
  const res = await fetch(`${API_URL}/strategies`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) throw await readError(res, `POST /strategies failed: ${res.status}`);
  return res.json();
}

/**
 * The contract and SDK stub, for handing to an external agent.
 *
 * Fetched rather than bundled: a copy compiled into this app would drift
 * from the file the validator actually enforces, and an agent writing
 * against stale rules produces rejections that look like its own fault.
 */
export type ContractBundle = {
  contract: string;
  sdk_stub: string;
  contract_version: string;
};

export async function fetchContractBundle(): Promise<ContractBundle> {
  const res = await fetch(`${API_URL}/strategies/contract`);
  if (!res.ok) throw await readError(res, `GET /strategies/contract failed: ${res.status}`);
  return res.json();
}

/**
 * The most recent smoke run for a registered strategy, as summarised by
 * `GET /strategies` -- one row's worth, not the full upload payload.
 */
export type LatestRun = {
  smoke_run_id: number;
  verdict: StrategyVerdict;
  window_start: string | null;
  window_end: string | null;
  sessions: number;
  bar_calls: number;
  orders_placed: number;
  fills: number;
  rejections: number;
  final_equity: string | null;
  breaker_reason: string | null;
  runtime: string;
  kernel_isolated: boolean;
  contract_version: string;
  ran_at: string;
};

/**
 * A registered strategy. `latest_run` is only ever the run that got it
 * registered: `strategy_smoke_runs.strategy_id` is NOT NULL, so a
 * rejected upload never gets a row to hang off a strategy in the first
 * place -- only PASSING uploads appear here at all.
 */
export type RegisteredStrategy = {
  strategy_id: number;
  name: string;
  version: string;
  status: string;
  contract_version: string;
  registered_at: string;
  bars: string | null;
  latest_run: LatestRun | null;
};

export async function fetchStrategies(limit?: number): Promise<RegisteredStrategy[]> {
  const url =
    limit === undefined ? `${API_URL}/strategies` : `${API_URL}/strategies?limit=${limit}`;
  const res = await fetch(url);
  if (!res.ok) throw await readError(res, `GET /strategies failed: ${res.status}`);
  return res.json();
}

/**
 * `GET /backtests/{id}` -- one stored run with its curve and metrics.
 *
 * Every number arrives as a string: JSON has no decimal type, and this
 * platform refuses to let money reach a client as a double. Parse for
 * charts, never for arithmetic that is then displayed.
 */
export type EquityPoint = { ts: string; equity: string; cash: string };

export type MaxDrawdown = {
  depth: string;
  peak_ts: string;
  trough_ts: string;
  recovered_ts: string | null;
  recovered: boolean;
  sessions: number;
  days: number;
};

export type TradeMetrics = {
  trades: number;
  wins: number;
  losses: number;
  win_rate: string | null;
  profit_factor: string | null;
  average_win: string | null;
  average_loss: string | null;
  expectancy: string | null;
};

export type FundingPaid = {
  instrument_id: number;
  symbol: string;
  /** Positive means the run paid; negative means it was paid. */
  amount: string;
};

export type Liquidation = {
  ordinal: number;
  symbol: string;
  ts: string;
  instrument_id: number;
  quantity: string;
  /** The mark that triggered it -- never the last traded price. */
  mark: string;
  /** Capped at the bankruptcy price when the market gapped past it. */
  fill_price: string;
  equity: string;
  maintenance: string;
  fee: string;
};

export type CostDrag = {
  total_charges: string;
  gross_pnl: string;
  net_pnl: string;
  /** Charges over the gross result. Null when there was no gross result. */
  drag: string | null;
};

export type Fill = {
  ordinal: number;
  ts: string;
  instrument_id: string;
  side: string;
  product: string;
  quantity: string;
  price: string;
  brokerage: string;
  stt: string;
  exchange_txn: string;
  sebi_fee: string;
  stamp_duty: string;
  ipft: string;
  gst: string;
  dp_charges: string;
  tds: string;
  total_charges: string;
  /** The strategy's own words for why it traded. */
  rationale: string | null;
};

export type BacktestMetrics = {
  trades?: TradeMetrics;
  cost_drag?: CostDrag;
  risk_free: string;
  periods_per_year: number | null;
  total_return: string | null;
  cagr: string | null;
  volatility: string | null;
  sharpe: string | null;
  sortino: string | null;
  calmar: string | null;
  max_drawdown: MaxDrawdown | null;
  value_at_risk_95: string | null;
  worst_period: { ts: string; return: string } | null;
  drawdown_curve: { ts: string; drawdown: string }[];
  monthly_returns: { month: string; return: string }[];
  rolling_sharpe: { ts: string; sharpe: string }[];
};

export type BacktestSummary = {
  backtest_run_id: number;
  strategy_id: number;
  status: string;
  requested_start: string;
  requested_end: string;
  fetch_start: string;
  dispatch_from: string;
  sessions: number;
  instruments: number[];
  history_bars_requested: number;
  history_bars_available: number;
  bars: string | null;
  bar_calls: number;
  orders_placed: number;
  fills: number;
  final_cash: string | null;
  final_equity: string | null;
  breaker_reason: string | null;
  error: string | null;
  findings: StrategyFinding[];
  runtime: string;
  kernel_isolated: boolean;
  contract_version: string;
  ran_at: string;
  /** Limits chosen for this run; null means the strategy's own applied. */
  max_daily_loss: string | null;
  max_drawdown_pct: string | null;
};

export type Percentiles = { p5: string; p50: string; p95: string };

export type Reshuffle = {
  iterations: number;
  terminal_equity: Percentiles;
  max_drawdown: Percentiles;
};

export type Stress = {
  multiplier: string;
  ok: boolean;
  fills: number | null;
  final_equity: string | null;
  breaker_reason: string | null;
  error: string | null;
};

export type BacktestDetail = BacktestSummary & {
  notes?: string[];
  equity_curve: EquityPoint[];
  fills_ledger: Fill[];
  /** What the run paid (positive) or was paid (negative) per contract. */
  funding?: FundingPaid[];
  /** Positions the exchange closed. Empty for every non-perpetual run. */
  liquidations?: Liquidation[];
  stress: Stress | null;
  reshuffle: Reshuffle | null;
  metrics: BacktestMetrics | null;
};

/** What `POST /strategies/{id}/backtests` returns: a run, or a refusal. */
export type BacktestRunResult = {
  strategy_id: number;
  status: string;
  notes?: string[];
  backtest_run_id: number | null;
  bars: string | null;
  bar_calls: number | null;
  fills: number | null;
  final_cash: string | null;
  final_equity: string | null;
  breaker_reason: string | null;
  equity_curve: EquityPoint[];
  findings: StrategyFinding[];
  runtime: string | null;
  kernel_isolated: boolean | null;
};

export async function fetchStrategy(strategyId: number): Promise<RegisteredStrategy> {
  const res = await fetch(`${API_URL}/strategies/${strategyId}`);
  if (!res.ok) throw await readError(res, `GET /strategies/${strategyId} failed: ${res.status}`);
  return res.json();
}

export async function fetchBacktests(strategyId: number): Promise<BacktestSummary[]> {
  const res = await fetch(`${API_URL}/strategies/${strategyId}/backtests`);
  if (!res.ok)
    throw await readError(res, `GET /strategies/${strategyId}/backtests failed: ${res.status}`);
  return res.json();
}

export async function fetchBacktest(
  runId: number,
  riskFree?: string,
): Promise<BacktestDetail> {
  const query = riskFree === undefined ? "" : `?risk_free=${encodeURIComponent(riskFree)}`;
  const res = await fetch(`${API_URL}/backtests/${runId}${query}`);
  if (!res.ok) throw await readError(res, `GET /backtests/${runId} failed: ${res.status}`);
  return res.json();
}

export async function runBacktest(
  strategyId: number,
  body: {
    start: string;
    end: string;
    starting_cash?: string;
    max_daily_loss?: string;
    max_drawdown_pct?: string;
  },
): Promise<BacktestRunResult> {
  const res = await fetch(`${API_URL}/strategies/${strategyId}/backtests`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok)
    throw await readError(res, `POST /strategies/${strategyId}/backtests failed: ${res.status}`);
  return res.json();
}

/** A strategy running forward against live prices. */
export type LiveRun = {
  live_run_id: number;
  strategy_id: number;
  portfolio_id: number;
  status: string;
  stopped_reason: string | null;
  runtime: string | null;
  kernel_isolated: boolean | null;
  bars_seen: number;
  orders_placed: number;
  orders_refused: number;
  last_refusal: string | null;
  started_at: string;
  stopped_at: string | null;
};

export type LivePosition = {
  instrument_id: number;
  symbol: string;
  quantity: string;
  avg_cost: string;
  last_price: string | null;
  market_value: string | null;
  unrealised_pnl: string | null;
};

export type LiveFill = {
  order_id: number;
  symbol: string;
  side: string;
  quantity: string;
  price: string;
  total_charges: string;
  rationale: string;
  filled_at: string;
};

export type LiveRunDetail = LiveRun & {
  strategy_name: string;
  strategy_version: string;
  portfolio_name: string;
  base_currency: string;
  cash_balance: string;
  equity: string | null;
  peak_equity: string | null;
  drawdown_pct: string | null;
  positions: LivePosition[];
  recent_fills: LiveFill[];
  equity_curve: { ts: string; equity: string }[];
};

export async function fetchLiveRuns(): Promise<LiveRun[]> {
  const res = await fetch(`${API_URL}/live`);
  if (!res.ok) throw await readError(res, `GET /live failed: ${res.status}`);
  return res.json();
}

export async function fetchLiveRun(liveRunId: number): Promise<LiveRunDetail> {
  const res = await fetch(`${API_URL}/live/${liveRunId}`);
  if (!res.ok) throw await readError(res, `GET /live/${liveRunId} failed: ${res.status}`);
  return res.json();
}

export async function startLiveRun(
  strategyId: number,
  portfolioId: number,
): Promise<LiveRun> {
  const res = await fetch(`${API_URL}/strategies/${strategyId}/live`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ portfolio_id: portfolioId }),
  });
  if (!res.ok) throw await readError(res, `POST /strategies/${strategyId}/live failed`);
  return res.json();
}

export async function stopLiveRun(liveRunId: number): Promise<LiveRun> {
  const res = await fetch(`${API_URL}/live/${liveRunId}/stop`, { method: "POST" });
  if (!res.ok) throw await readError(res, `POST /live/${liveRunId}/stop failed`);
  return res.json();
}
