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
