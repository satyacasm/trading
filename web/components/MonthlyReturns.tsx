"use client";

const MONTHS = ["J", "F", "M", "A", "M", "J", "J", "A", "S", "O", "N", "D"];

/**
 * Monthly returns as a years x months grid.
 *
 * A CSS grid rather than a charting library: this is a table of numbers
 * with a colour ramp, and adding a dependency for one heatmap would be the
 * wrong trade. The shading uses `color-mix(in oklab, ...)` against `--up`
 * and `--down`, the technique `globals.css` already uses for its flash
 * animations, so the ramp is the same green and red as everything else on
 * screen rather than a second palette.
 *
 * Intensity is scaled against the largest absolute move in the grid, so a
 * quiet strategy is not rendered as uniformly grey and a volatile one does
 * not saturate to a wall of colour.
 */
export function MonthlyReturns({
  months,
}: {
  months: { month: string; return: string }[];
}) {
  if (months.length === 0) return null;

  const byYear = new Map<string, Map<number, number>>();
  let largest = 0;
  for (const entry of months) {
    const [year, month] = entry.month.split("-");
    const value = Number(entry.return);
    if (!Number.isFinite(value)) continue;
    largest = Math.max(largest, Math.abs(value));
    if (!byYear.has(year)) byYear.set(year, new Map());
    byYear.get(year)!.set(Number(month) - 1, value);
  }
  const years = [...byYear.keys()].sort();

  function shade(value: number | undefined): string {
    if (value === undefined) return "transparent";
    if (largest === 0) return "transparent";
    const weight = Math.round((Math.abs(value) / largest) * 70) + 8;
    const token = value >= 0 ? "var(--up)" : "var(--down)";
    return `color-mix(in oklab, ${token} ${weight}%, transparent)`;
  }

  return (
    <div className="overflow-x-auto">
      <table className="border-separate border-spacing-[2px] text-xs">
        <thead>
          <tr>
            <th className="text-muted pr-2 text-left font-normal" />
            {MONTHS.map((label, index) => (
              <th key={index} className="text-muted w-8 text-center font-normal">
                {label}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {years.map((year) => (
            <tr key={year}>
              <td className="text-muted num pr-2 text-right">{year}</td>
              {MONTHS.map((_, index) => {
                const value = byYear.get(year)?.get(index);
                return (
                  <td
                    key={index}
                    className="num h-7 w-8 rounded-sm text-center text-[10px]"
                    style={{ backgroundColor: shade(value) }}
                    title={
                      value === undefined
                        ? `${year}-${String(index + 1).padStart(2, "0")}: no data`
                        : `${year}-${String(index + 1).padStart(2, "0")}: ${(value * 100).toFixed(2)}%`
                    }
                  >
                    {value === undefined ? "" : (value * 100).toFixed(1)}
                  </td>
                );
              })}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
