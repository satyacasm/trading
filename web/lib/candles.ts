import type { UTCTimestamp } from "lightweight-charts";

export type ChartCandle = {
  time: UTCTimestamp;
  open: number;
  high: number;
  low: number;
  close: number;
};

const INTERVAL_SECONDS: Record<string, number> = {
  "1m": 60,
  "5m": 300,
  "15m": 900,
  "1h": 3600,
  "1d": 86400,
};

function intervalWidthSeconds(interval: string): number {
  const width = INTERVAL_SECONDS[interval];
  if (width === undefined) {
    throw new Error(`Unknown interval: ${interval}`);
  }
  return width;
}

export function bucketStart(epochSeconds: number, interval: string): UTCTimestamp {
  const width = intervalWidthSeconds(interval);
  return (Math.floor(epochSeconds / width) * width) as UTCTimestamp;
}

export function mergeTickIntoCandles(
  candles: ChartCandle[],
  price: number,
  tickTimeSeconds: number,
  interval: string
): ChartCandle[] {
  const bucket = bucketStart(tickTimeSeconds, interval);

  if (candles.length === 0) {
    return [{ time: bucket, open: price, high: price, low: price, close: price }];
  }

  const lastCandle = candles[candles.length - 1];

  if (bucket === lastCandle.time) {
    const updatedCandle: ChartCandle = {
      time: lastCandle.time,
      open: lastCandle.open,
      high: Math.max(lastCandle.high, price),
      low: Math.min(lastCandle.low, price),
      close: price,
    };
    return [...candles.slice(0, -1), updatedCandle];
  }

  if (bucket > lastCandle.time) {
    const newCandle: ChartCandle = { time: bucket, open: price, high: price, low: price, close: price };
    return [...candles, newCandle];
  }

  return [...candles];
}
