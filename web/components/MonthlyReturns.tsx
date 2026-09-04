"use client";

import { useMemo, useState } from "react";

const MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];

type Cell = { year: string; month: number; value: number };

/**
 * Monthly returns as a years x months grid.
 *
 * A CSS grid rather than a charting library: this is a table of numbers with
 * a colour ramp, and a dependency for one heatmap would be the wrong trade.
 * Shading uses `color-mix(in oklab, ...)` against `--up` and `--down`, the
 * technique `globals.css` already uses for its flash animations, so the ramp
 * is the same green and red as everything else on screen.
 *
 * Intensity scales against the largest absolute move in the grid, so a quiet
 * strategy is not uniformly grey and a volatile one does not saturate.
 *
 * Selecting a cell holds its detail in place. Hover alone is unusable for
 * comparing two months -- you cannot look at a tooltip and the grid at once
 * -- and unreachable by keyboard.
 */
export function MonthlyReturns({ months }: { months: { month: string; return: string }[] }) {
  const [selected, setSelected] = useState<Cell | null>(null);
  const [hovered, setHovered] = useState<Cell | null>(null);

  const { years, byYear, largest, yearTotals, best, worst } = useMemo(() => {
    const map = new Map<string, Map<number, number>>();
    let biggest = 0;
    let bestCell: Cell | null = null;
    let worstCell: Cell | null = null;
    for (const entry of months) {
      const [year, month] = entry.month.split("-");
      const value = Number(entry.return);
      if (!Number.isFinite(value)) continue;
      biggest = Math.max(biggest, Math.abs(value));
      if (!map.has(year)) map.set(year, new Map());
      map.get(year)!.set(Number(month) - 1, value);
      const cell = { year, month: Number(month) - 1, value };
      if (bestCell === null || value > bestCell.value) bestCell = cell;
      if (worstCell === null || value < worstCell.value) worstCell = cell;
    }
    // A year's return is its months compounded, not summed: returns
    // multiply, and adding them overstates a good year and understates a bad
    // one.
    const totals = new Map<string, number>();
    for (const [year, cells] of map) {
      let compounded = 1;
      for (const value of cells.values()) compounded *= 1 + value;
      totals.set(year, compounded - 1);
    }
    return {
      years: [...map.keys()].sort(),
      byYear: map,
      largest: biggest,
      yearTotals: totals,
      best: bestCell as Cell | null,
      worst: worstCell as Cell | null,
    };
  }, [months]);

  if (months.length === 0) return null;

  function shade(value: number | undefined): string {
    if (value === undefined || largest === 0) return "transparent";
    const weight = Math.round((Math.abs(value) / largest) * 70) + 8;
    return `color-mix(in oklab, ${value >= 0 ? "var(--up)" : "var(--down)"} ${weight}%, transparent)`;
  }

  const shown = hovered ?? selected;

  return (
    <div className="flex flex-col gap-3">
      <div className="overflow-x-auto">
        <table className="border-separate border-spacing-[2px] text-xs">
          <thead>
            <tr>
              <th className="text-muted pr-2 text-left font-normal" />
              {MONTHS.map((label) => (
                <th key={label} className="text-muted w-11 text-center font-normal">
                  {label.slice(0, 1)}
                </th>
              ))}
              <th className="text-muted pl-3 text-right font-normal">Year</th>
            </tr>
          </thead>
          <tbody>
            {years.map((year) => (
              <tr key={year}>
                <td className="text-muted num pr-2 text-right">{year}</td>
                {MONTHS.map((_, index) => {
                  const value = byYear.get(year)?.get(index);
                  const cell = value === undefined ? null : { year, month: index, value };
                  const isSelected =
                    selected?.year === year && selected?.month === index && value !== undefined;
                  return (
                    <td key={index} className="p-0">
                      <button
                        type="button"
                        disabled={value === undefined}
                        aria-label={
                          value === undefined
                            ? `${MONTHS[index]} ${year}, no data`
                            : `${MONTHS[index]} ${year}, ${(value * 100).toFixed(2)} percent`
                        }
                        onMouseEnter={() => setHovered(cell)}
                        onMouseLeave={() => setHovered(null)}
                        onFocus={() => setHovered(cell)}
                        onBlur={() => setHovered(null)}
                        onClick={() => setSelected(isSelected ? null : cell)}
                        className={`num h-8 w-11 rounded-sm text-center text-[10px] transition-[outline] ${
                          isSelected ? "outline outline-2 outline-[var(--live)]" : ""
                        } ${value === undefined ? "cursor-default" : "cursor-pointer"}`}
                        style={{ backgroundColor: shade(value) }}
                      >
                        {value === undefined ? "" : (value * 100).toFixed(1)}
                      </button>
                    </td>
                  );
                })}
                <td
                  className={`num pl-3 text-right ${
                    (yearTotals.get(year) ?? 0) >= 0 ? "text-up" : "text-down"
                  }`}
                >
                  {((yearTotals.get(year) ?? 0) * 100).toFixed(1)}%
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <div className="text-muted flex min-h-[1.25rem] flex-wrap items-center gap-x-5 text-xs">
        {shown ? (
          <span className="text-text num">
            {MONTHS[shown.month]} {shown.year}:{" "}
            <span className={shown.value >= 0 ? "text-up" : "text-down"}>
              {(shown.value * 100).toFixed(2)}%
            </span>
          </span>
        ) : (
          <span>Hover or click a month. Year column compounds that year&apos;s months.</span>
        )}
        {best ? (
          <span className="num">
            best {MONTHS[best.month]} {best.year}{" "}
            <span className="text-up">{(best.value * 100).toFixed(2)}%</span>
          </span>
        ) : null}
        {worst ? (
          <span className="num">
            worst {MONTHS[worst.month]} {worst.year}{" "}
            <span className="text-down">{(worst.value * 100).toFixed(2)}%</span>
          </span>
        ) : null}
      </div>
    </div>
  );
}
