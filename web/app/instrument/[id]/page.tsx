"use client";

import Link from "next/link";
import { useParams } from "next/navigation";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import type {
  CandlestickData,
  IChartApi,
  ISeriesApi,
  UTCTimestamp,
} from "lightweight-charts";
import {
  fetchCandles,
  fetchInstruments,
  type Interval,
  type InstrumentSummary,
} from "@/lib/api";
import { mergeTickIntoCandles, type ChartCandle } from "@/lib/candles";
import { useTickStream, type Tick } from "@/lib/useTickStream";

const INTERVALS: Interval[] = ["1m", "5m", "15m", "1h", "1d"];

const LIVE_WINDOW_MS = 90_000;
const HEADER_FLUSH_MS = 150;

type Direction = "up" | "down" | null;

type HeaderState = {
  price: number | null;
  direction: Direction;
  lastTickMs: number | null;
};

export default function InstrumentPage() {
  const params = useParams<{ id: string }>();
  const instrumentId = Number(params.id);

  const [instrument, setInstrument] = useState<InstrumentSummary | null>(null);
  const [interval, setSelectedInterval] = useState<Interval>("1m");
  const [candlesError, setCandlesError] = useState<string | null>(null);
  const [instrumentsError, setInstrumentsError] = useState<string | null>(null);
  const [isEmpty, setIsEmpty] = useState(false);
  const [header, setHeader] = useState<HeaderState>({
    price: null,
    direction: null,
    lastTickMs: null,
  });
  const [isLive, setIsLive] = useState(false);

  const containerRef = useRef<HTMLDivElement | null>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const seriesRef = useRef<ISeriesApi<"Candlestick"> | null>(null);
  const candlesRef = useRef<ChartCandle[]>([]);
  const intervalRef = useRef<Interval>(interval);

  // Header state must stay off the per-tick hot path: every tick updates
  // this ref synchronously (so direction comparisons never see a stale
  // React-state price), and a 150ms interval flushes it into React state
  // only when it actually changed.
  const headerRef = useRef<HeaderState>({ price: null, direction: null, lastTickMs: null });
  const headerDirtyRef = useRef(false);

  useEffect(() => {
    intervalRef.current = interval;
  }, [interval]);

  // Resolve the instrument's display info.
  useEffect(() => {
    let cancelled = false;
    fetchInstruments()
      .then((list) => {
        if (cancelled) return;
        const match = list.find((i) => i.instrument_id === instrumentId) ?? null;
        setInstrument(match);
        setInstrumentsError(null);
      })
      .catch((e) => {
        if (cancelled) return;
        setInstrumentsError(e instanceof Error ? e.message : String(e));
      });
    return () => {
      cancelled = true;
    };
  }, [instrumentId]);

  // Create the chart once on mount.
  useEffect(() => {
    const container = containerRef.current;
    if (!container) return;

    let disposed = false;
    let chart: IChartApi | null = null;
    let series: ISeriesApi<"Candlestick"> | null = null;
    let resizeObserver: ResizeObserver | null = null;

    import("lightweight-charts").then(({ createChart }) => {
      if (disposed || !container) return;

      chart = createChart(container, {
        width: container.clientWidth,
        height: 440,
        layout: {
          background: { color: "#0b0f14" },
          textColor: "#7a8794",
          fontFamily: "var(--font-jetbrains-mono), ui-monospace, 'SF Mono', monospace",
        },
        grid: {
          vertLines: { color: "#1f2a35" },
          horzLines: { color: "#1f2a35" },
        },
        rightPriceScale: { borderColor: "#1f2a35" },
        timeScale: { borderColor: "#1f2a35", timeVisible: true, secondsVisible: false },
        crosshair: { mode: 0 },
      });

      series = chart.addCandlestickSeries({
        upColor: "#26a69a",
        downColor: "#ef5350",
        borderUpColor: "#26a69a",
        borderDownColor: "#ef5350",
        wickUpColor: "#26a69a",
        wickDownColor: "#ef5350",
      });

      chartRef.current = chart;
      seriesRef.current = series;

      if (candlesRef.current.length > 0) {
        // The fetch can resolve before this dynamic import does; when it has,
        // paint what it left in the ref rather than waiting for a refetch.
        series.setData(candlesRef.current as CandlestickData[]);
        chart.timeScale().fitContent();
      }

      resizeObserver = new ResizeObserver((entries) => {
        const entry = entries[0];
        if (!entry || !chartRef.current) return;
        chartRef.current.applyOptions({ width: entry.contentRect.width });
      });
      resizeObserver.observe(container);
    });

    return () => {
      disposed = true;
      resizeObserver?.disconnect();
      chartRef.current?.remove();
      chartRef.current = null;
      seriesRef.current = null;
    };
  }, []);

  // Fetch candles whenever the instrument or timeframe changes.
  useEffect(() => {
    let cancelled = false;

    // Drop the previous timeframe's candles *before* awaiting the new ones.
    // intervalRef has already flipped to the new width, so a tick landing in
    // the gap would otherwise be bucketed at the new width and merged into
    // bars built at the old one -- appending a bogus candle to the chart for
    // the ~100ms until the fetch resolves.
    candlesRef.current = [];

    fetchCandles(instrumentId, interval)
      .then((res) => {
        if (cancelled) return;
        const mapped: ChartCandle[] = res.candles.map((c) => ({
          time: (new Date(c.ts).getTime() / 1000) as UTCTimestamp,
          open: c.open,
          high: c.high,
          low: c.low,
          close: c.close,
        }));
        candlesRef.current = mapped;
        seriesRef.current?.setData(mapped as CandlestickData[]);
        // Without this the series keeps the previous timeframe's bar spacing,
        // so a coarse interval with few bars (say 40 hourly candles) renders
        // as a narrow clump against the right edge instead of using the width.
        chartRef.current?.timeScale().fitContent();
        setIsEmpty(mapped.length === 0);
        setCandlesError(null);

        // Seed the header from the last bar's close when no tick has arrived
        // yet. Outside market hours no tick ever will, and a bare "--" next to
        // a fully-drawn chart reads as broken. lastTickMs stays null so this
        // never counts as LIVE -- it is the last known close, labelled as such.
        const lastBar = mapped[mapped.length - 1];
        if (lastBar && headerRef.current.price === null) {
          headerRef.current = { price: lastBar.close, direction: null, lastTickMs: null };
          headerDirtyRef.current = true;
        }
      })
      .catch((e) => {
        if (cancelled) return;
        setCandlesError(e instanceof Error ? e.message : String(e));
        setIsEmpty(false);
      });

    return () => {
      cancelled = true;
    };
  }, [instrumentId, interval]);

  // Flush the pending header state (and re-derive the LIVE tag's freshness)
  // on a fixed cadence, independent of tick rate. Date.now() is read here,
  // inside an effect callback, never during render.
  useEffect(() => {
    const id = window.setInterval(() => {
      if (headerDirtyRef.current) {
        setHeader(headerRef.current);
        headerDirtyRef.current = false;
      }
      const lastTickMs = headerRef.current.lastTickMs;
      setIsLive(lastTickMs !== null && Date.now() - lastTickMs < LIVE_WINDOW_MS);
    }, HEADER_FLUSH_MS);
    return () => window.clearInterval(id);
  }, []);

  const instrumentIds = useMemo(() => [instrumentId], [instrumentId]);

  const handleTick = useCallback(
    (tick: Tick) => {
      if (tick.instrument_id !== instrumentId) return;
      const price = Number(tick.price);
      if (!Number.isFinite(price)) return;
      const tickTimeSeconds = new Date(tick.ts).getTime() / 1000;

      const merged = mergeTickIntoCandles(
        candlesRef.current,
        price,
        tickTimeSeconds,
        intervalRef.current
      );
      candlesRef.current = merged;
      const last = merged[merged.length - 1];
      if (last) {
        seriesRef.current?.update(last as CandlestickData);
      }
      if (isEmpty) setIsEmpty(false);

      const previousPrice = headerRef.current.price;
      let direction: Direction = headerRef.current.direction;
      if (previousPrice !== null) {
        if (price > previousPrice) direction = "up";
        else if (price < previousPrice) direction = "down";
      }

      headerRef.current = { price, direction, lastTickMs: Date.now() };
      headerDirtyRef.current = true;
    },
    [instrumentId, isEmpty]
  );

  useTickStream(instrumentIds, handleTick);

  const priceColorClass =
    header.direction === "up"
      ? "text-up"
      : header.direction === "down"
        ? "text-down"
        : "text-text";

  const symbolLabel = instrument ? instrument.symbol : String(instrumentId);

  return (
    <main className="flex flex-col min-h-full">
      <header className="flex items-center justify-between gap-4 border-b border-line px-4 py-4 sm:px-8">
        <div className="flex flex-col gap-1 min-w-0">
          <Link href="/" className="text-muted hover:text-text text-sm w-fit">
            ← Watchlist
          </Link>
          <div className="flex items-baseline gap-2 min-w-0">
            <h1 className="font-display text-2xl sm:text-3xl truncate">{symbolLabel}</h1>
          </div>
          {instrument && (
            <p className="text-muted text-xs">
              {instrument.exchange} · {instrument.asset_class}
            </p>
          )}
          {instrumentsError && (
            <p className="text-down text-xs">Failed to resolve instrument: {instrumentsError}</p>
          )}
        </div>

        <div className="flex flex-col items-end gap-1 shrink-0">
          <div className="flex items-center gap-2">
            {isLive && (
              <span className="text-live text-xs font-display tracking-wide border border-live/40 rounded px-1.5 py-0.5">
                LIVE
              </span>
            )}
            <span className={`num text-2xl sm:text-3xl ${priceColorClass}`}>
              {header.price !== null ? header.price.toFixed(2) : "--"}
            </span>
          </div>
          {header.price !== null && !isLive && (
            <span className="text-muted text-xs">last close</span>
          )}
        </div>
      </header>

      <div className="flex items-center gap-2 px-4 py-3 sm:px-8">
        <div className="inline-flex border border-line rounded overflow-hidden" role="group">
          {INTERVALS.map((iv) => {
            const selected = iv === interval;
            return (
              <button
                key={iv}
                type="button"
                aria-pressed={selected}
                onClick={() => setSelectedInterval(iv)}
                className={`px-3 py-1.5 text-sm font-mono transition-colors ${
                  selected ? "bg-raised text-text" : "text-muted hover:text-text"
                }`}
              >
                {iv}
              </button>
            );
          })}
        </div>
      </div>

      <div className="relative flex-1 px-4 pb-8 sm:px-8">
        {candlesError && (
          <p className="text-down text-sm mb-2">Failed to load candles: {candlesError}</p>
        )}
        <div className="relative w-full max-w-full overflow-hidden" style={{ height: 440 }}>
          <div ref={containerRef} className="w-full h-full" />
          {isEmpty && !candlesError && (
            <div className="absolute inset-0 flex items-center justify-center bg-ground/80 pointer-events-none">
              <p className="text-muted text-sm text-center px-4">
                No {interval} bars for this instrument yet.
              </p>
            </div>
          )}
        </div>
      </div>
    </main>
  );
}
