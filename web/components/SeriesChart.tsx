"use client";

import { useEffect, useRef } from "react";
import type { IChartApi, LineData, UTCTimestamp } from "lightweight-charts";

export type Line = {
  /** Points in ascending time order. */
  data: { ts: string; value: number }[];
  color: string;
  /** Shade from the line down to the baseline. Used for the underwater chart. */
  area?: boolean;
  title?: string;
};

/**
 * One or more time series on a shared axis.
 *
 * Deliberately thin: the report needs an equity/risk-free pair, an
 * underwater area, and a rolling-Sharpe line, and all three are the same
 * chart with different series. `lightweight-charts` is imported dynamically
 * for the same reason the instrument page does it -- it is a large module
 * and nothing on this page needs it before paint.
 *
 * Colours are passed in rather than chosen here so every one traces back to
 * a token in `globals.css`; a component that picked its own would be the
 * first place the palette drifted.
 */
export function SeriesChart({
  lines,
  height = 280,
  priceFormat = "price",
}: {
  lines: Line[];
  height?: number;
  priceFormat?: "price" | "percent";
}) {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const chartRef = useRef<IChartApi | null>(null);

  useEffect(() => {
    const container = containerRef.current;
    if (!container) return;

    let disposed = false;
    let chart: IChartApi | null = null;
    let resizeObserver: ResizeObserver | null = null;

    import("lightweight-charts").then(({ createChart }) => {
      if (disposed || !container) return;

      chart = createChart(container, {
        width: container.clientWidth,
        height,
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
        timeScale: { borderColor: "#1f2a35", timeVisible: false, secondsVisible: false },
        crosshair: { mode: 0 },
        handleScale: false,
        handleScroll: false,
      });

      for (const line of lines) {
        const points = line.data.map((point) => ({
          time: (Date.parse(point.ts) / 1000) as UTCTimestamp,
          value: point.value,
        })) as LineData[];
        const series = line.area
          ? chart.addAreaSeries({
              lineColor: line.color,
              topColor: `${line.color}00`,
              bottomColor: `${line.color}66`,
              lineWidth: 1,
              priceLineVisible: false,
              lastValueVisible: false,
            })
          : chart.addLineSeries({
              color: line.color,
              lineWidth: 2,
              priceLineVisible: false,
              lastValueVisible: false,
            });
        if (priceFormat === "percent") {
          series.applyOptions({ priceFormat: { type: "percent" } });
        }
        series.setData(points);
      }

      chart.timeScale().fitContent();
      chartRef.current = chart;

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
    };
  }, [lines, height, priceFormat]);

  return <div ref={containerRef} className="w-full" />;
}
