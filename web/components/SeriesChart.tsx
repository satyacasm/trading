"use client";

import { useEffect, useRef, useState } from "react";
import type { IChartApi, ISeriesApi, LineData, UTCTimestamp } from "lightweight-charts";

export type Line = {
  /** Points in ascending time order. */
  data: { ts: string; value: number }[];
  color: string;
  /** Shade from the line down to the baseline. Used for the underwater chart. */
  area?: boolean;
  /** Shown in the crosshair readout. */
  title: string;
};

export type Marker = {
  ts: string;
  side: "BUY" | "SELL";
  text: string;
};

function formatValue(value: number, format: "price" | "percent"): string {
  if (format === "percent") return `${(value * 100).toFixed(2)}%`;
  return value.toLocaleString("en-IN", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

/**
 * One or more time series on a shared axis, with a crosshair that reads out
 * both axes.
 *
 * The readout is the point. A chart you can only look at answers "roughly
 * what shape" and nothing else; a reader wanting "what was equity on 12 March
 * 2022" had no way to ask. `subscribeCrosshairMove` gives the date and every
 * series' value at the pointer, which is the question people actually have.
 *
 * Colours are passed in rather than chosen here, so every one traces back to
 * a token in `globals.css`; a component picking its own would be the first
 * place the palette drifted.
 */
export function SeriesChart({
  lines,
  markers = [],
  height = 280,
  priceFormat = "price",
}: {
  lines: Line[];
  markers?: Marker[];
  height?: number;
  priceFormat?: "price" | "percent";
}) {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const [readout, setReadout] = useState<{ ts: string; values: [string, string, string][] } | null>(
    null,
  );

  useEffect(() => {
    const container = containerRef.current;
    if (!container) return;

    let disposed = false;
    let chart: IChartApi | null = null;
    let resizeObserver: ResizeObserver | null = null;

    import("lightweight-charts").then(({ createChart, CrosshairMode }) => {
      if (disposed || !container) return;

      chart = createChart(container, {
        width: container.clientWidth,
        height,
        layout: {
          background: { color: "#0b0f14" },
          textColor: "#7a8794",
          fontFamily: "var(--font-jetbrains-mono), ui-monospace, 'SF Mono', monospace",
        },
        grid: { vertLines: { color: "#1f2a35" }, horzLines: { color: "#1f2a35" } },
        rightPriceScale: { borderColor: "#1f2a35" },
        timeScale: { borderColor: "#1f2a35", timeVisible: false, secondsVisible: false },
        // Magnet mode snaps the crosshair to the nearest data point, so the
        // readout is always a value that exists rather than one interpolated
        // between two that do.
        crosshair: {
          mode: CrosshairMode.Magnet,
          vertLine: { color: "#4da3ff", width: 1, style: 2, labelBackgroundColor: "#1a232e" },
          horzLine: { color: "#4da3ff", width: 1, style: 2, labelBackgroundColor: "#1a232e" },
        },
        // Pan and zoom: a six-year daily chart is unreadable without them.
        handleScale: { mouseWheel: true, pinch: true, axisPressedMouseMove: true },
        handleScroll: { mouseWheel: true, pressedMouseMove: true },
      });

      const series: [string, string, ISeriesApi<"Line" | "Area">][] = [];
      for (const line of lines) {
        const points = line.data.map((point) => ({
          time: (Date.parse(point.ts) / 1000) as UTCTimestamp,
          value: point.value,
        })) as LineData[];
        const created = line.area
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
        if (priceFormat === "percent") created.applyOptions({ priceFormat: { type: "percent" } });
        created.setData(points);
        series.push([line.title, line.color, created]);
      }

      if (markers.length > 0 && series.length > 0) {
        series[series.length - 1][2].setMarkers(
          markers.map((marker) => ({
            time: (Date.parse(marker.ts) / 1000) as UTCTimestamp,
            position: marker.side === "BUY" ? "belowBar" : "aboveBar",
            color: marker.side === "BUY" ? "#26a69a" : "#ef5350",
            shape: marker.side === "BUY" ? "arrowUp" : "arrowDown",
            text: marker.text,
          })),
        );
      }

      chart.subscribeCrosshairMove((param) => {
        if (!param.time || !param.point) {
          setReadout(null);
          return;
        }
        const stamp = new Date((param.time as number) * 1000).toISOString().slice(0, 10);
        const values: [string, string, string][] = [];
        for (const [title, color, api] of series) {
          const point = param.seriesData.get(api) as { value?: number } | undefined;
          if (point?.value !== undefined) {
            values.push([title, color, formatValue(point.value, priceFormat)]);
          }
        }
        setReadout({ ts: stamp, values });
      });

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
  }, [lines, markers, height, priceFormat]);

  return (
    <div className="relative w-full">
      <div ref={containerRef} className="w-full" />
      {/* Reserved height, so the chart does not jump as the readout appears
          and disappears under the pointer. */}
      <div className="num text-muted mt-1 flex h-5 flex-wrap items-center gap-x-4 text-xs">
        {readout ? (
          <>
            <span className="text-text">{readout.ts}</span>
            {readout.values.map(([title, color, value]) => (
              <span key={title} className="flex items-center gap-1.5">
                <span
                  aria-hidden
                  className="inline-block h-2 w-2 rounded-full"
                  style={{ backgroundColor: color }}
                />
                {title} <span className="text-text">{value}</span>
              </span>
            ))}
          </>
        ) : (
          <span>Hover for values · scroll to zoom · drag to pan</span>
        )}
      </div>
    </div>
  );
}
