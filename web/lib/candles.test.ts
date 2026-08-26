import { describe, expect, it } from "vitest";
import { bucketStart, mergeTickIntoCandles, type ChartCandle } from "./candles";

describe("bucketStart", () => {
  it("floors a 1m timestamp to the start of its minute", () => {
    // 2026-01-01T00:00:45Z = 1767225645
    expect(bucketStart(1767225645, "1m")).toBe(1767225600);
  });

  it("floors a 5m timestamp to the start of its 5-minute bucket", () => {
    // 1767225600 is a 5m boundary (divisible by 300); +301s lands in the next bucket
    expect(bucketStart(1767225901, "5m")).toBe(1767225900);
  });

  it("floors a 15m timestamp to the start of its 15-minute bucket", () => {
    expect(bucketStart(1767225600 + 901, "15m")).toBe(1767225600 + 900);
  });

  it("floors an hour timestamp to the start of its hour", () => {
    // 1767225600 is midnight UTC; +3601s lands one second into the next hour bucket
    expect(bucketStart(1767225600 + 3601, "1h")).toBe(1767225600 + 3600);
  });

  it("floors a day timestamp to the start of its day", () => {
    expect(bucketStart(1767225600 + 86401, "1d")).toBe(1767225600 + 86400);
  });

  it("leaves a timestamp exactly on a bucket boundary unchanged", () => {
    expect(bucketStart(1767225600, "1m")).toBe(1767225600);
  });

  it("floors a timestamp one second before a boundary to the previous bucket", () => {
    expect(bucketStart(1767225659, "1m")).toBe(1767225600);
  });
});

describe("mergeTickIntoCandles", () => {
  it("returns a single fresh candle when given an empty array", () => {
    const result = mergeTickIntoCandles([], 100, 1767225600, "1m");
    expect(result).toEqual([
      { time: 1767225600, open: 100, high: 100, low: 100, close: 100 },
    ]);
  });

  it("updates the last candle's close, high and low when the tick falls in the same bucket", () => {
    const candles: ChartCandle[] = [
      { time: 1767225600 as ChartCandle["time"], open: 10, high: 12, low: 9, close: 11 },
    ];
    const result = mergeTickIntoCandles(candles, 15, 1767225630, "1m");
    expect(result).toEqual([
      { time: 1767225600, open: 10, high: 15, low: 9, close: 15 },
    ]);
  });

  it("lowers the last candle's low when a same-bucket tick price is below the current low", () => {
    const candles: ChartCandle[] = [
      { time: 1767225600 as ChartCandle["time"], open: 10, high: 12, low: 9, close: 11 },
    ];
    const result = mergeTickIntoCandles(candles, 5, 1767225630, "1m");
    expect(result).toEqual([
      { time: 1767225600, open: 10, high: 12, low: 5, close: 5 },
    ]);
  });

  it("appends a fresh candle when the tick's bucket is later than the last candle's", () => {
    const candles: ChartCandle[] = [
      { time: 1767225600 as ChartCandle["time"], open: 10, high: 12, low: 9, close: 11 },
    ];
    const result = mergeTickIntoCandles(candles, 20, 1767225660, "1m");
    expect(result).toEqual([
      { time: 1767225600, open: 10, high: 12, low: 9, close: 11 },
      { time: 1767225660, open: 20, high: 20, low: 20, close: 20 },
    ]);
  });

  it("opens a new bucket for a tick exactly on the next boundary", () => {
    const candles: ChartCandle[] = [
      { time: 1767225600 as ChartCandle["time"], open: 10, high: 12, low: 9, close: 11 },
    ];
    const result = mergeTickIntoCandles(candles, 20, 1767225660, "1m");
    expect(result).toHaveLength(2);
    expect(result[1].time).toBe(1767225660);
  });

  it("does not open a new bucket for a tick one second before the next boundary", () => {
    const candles: ChartCandle[] = [
      { time: 1767225600 as ChartCandle["time"], open: 10, high: 12, low: 9, close: 11 },
    ];
    const result = mergeTickIntoCandles(candles, 20, 1767225659, "1m");
    expect(result).toHaveLength(1);
    expect(result[0].close).toBe(20);
  });

  it("ignores a late tick whose bucket is earlier than the last candle's", () => {
    const candles: ChartCandle[] = [
      { time: 1767225660 as ChartCandle["time"], open: 20, high: 20, low: 20, close: 20 },
    ];
    const result = mergeTickIntoCandles(candles, 999, 1767225600, "1m");
    expect(result).toEqual([
      { time: 1767225660, open: 20, high: 20, low: 20, close: 20 },
    ]);
  });

  it("does not mutate the input array", () => {
    const candles: ChartCandle[] = [
      { time: 1767225600 as ChartCandle["time"], open: 10, high: 12, low: 9, close: 11 },
    ];
    const original = candles.map((c) => ({ ...c }));
    mergeTickIntoCandles(candles, 15, 1767225630, "1m");
    expect(candles).toEqual(original);
  });

  it("does not mutate the candle objects inside the input array", () => {
    const candle = { time: 1767225600 as ChartCandle["time"], open: 10, high: 12, low: 9, close: 11 };
    const candles: ChartCandle[] = [candle];
    mergeTickIntoCandles(candles, 15, 1767225630, "1m");
    expect(candle).toEqual({ time: 1767225600, open: 10, high: 12, low: 9, close: 11 });
  });

  it("returns a new array instance rather than the same reference", () => {
    const candles: ChartCandle[] = [
      { time: 1767225600 as ChartCandle["time"], open: 10, high: 12, low: 9, close: 11 },
    ];
    const result = mergeTickIntoCandles(candles, 15, 1767225630, "1m");
    expect(result).not.toBe(candles);
  });

  it("correctly buckets an hourly tick that starts a new hour candle", () => {
    const candles: ChartCandle[] = [
      { time: 1767225600 as ChartCandle["time"], open: 10, high: 12, low: 9, close: 11 },
    ];
    const result = mergeTickIntoCandles(candles, 30, 1767225600 + 3600, "1h");
    expect(result).toEqual([
      { time: 1767225600, open: 10, high: 12, low: 9, close: 11 },
      { time: 1767225600 + 3600, open: 30, high: 30, low: 30, close: 30 },
    ]);
  });

  it("correctly buckets a daily tick that starts a new day candle", () => {
    const candles: ChartCandle[] = [
      { time: 1767225600 as ChartCandle["time"], open: 10, high: 12, low: 9, close: 11 },
    ];
    const result = mergeTickIntoCandles(candles, 30, 1767225600 + 86400, "1d");
    expect(result).toEqual([
      { time: 1767225600, open: 10, high: 12, low: 9, close: 11 },
      { time: 1767225600 + 86400, open: 30, high: 30, low: 30, close: 30 },
    ]);
  });
});
