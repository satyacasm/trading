"use client";

import Link from "next/link";
import { useParams, useRouter } from "next/navigation";
import { useCallback, useEffect, useState } from "react";
import {
  fetchBacktests,
  fetchStrategy,
  runBacktest,
  startLiveRun,
  type BacktestSummary,
  type RegisteredStrategy,
  type StrategyFinding,
} from "@/lib/api";
import { UNDEFINED_METRIC, backtestBlockedReason } from "@/lib/backtests";
import { usePortfolios } from "@/lib/usePortfolios";

export default function StrategyPage() {
  const params = useParams<{ id: string }>();
  const router = useRouter();
  const strategyId = Number(params.id);

  const [strategy, setStrategy] = useState<RegisteredStrategy | null>(null);
  const [runs, setRuns] = useState<BacktestSummary[]>([]);
  const [error, setError] = useState<string | null>(null);

  const [start, setStart] = useState("2020-01-01");
  const [end, setEnd] = useState("2026-08-21");
  const [capital, setCapital] = useState("");
  const [dailyLoss, setDailyLoss] = useState("");
  const [drawdownPct, setDrawdownPct] = useState("");
  const [running, setRunning] = useState(false);
  const [refusal, setRefusal] = useState<StrategyFinding[]>([]);

  const { portfolios, selectedId, setSelectedId } = usePortfolios();
  const [starting, setStarting] = useState(false);
  const [liveError, setLiveError] = useState<string | null>(null);

  const blocked = strategy ? backtestBlockedReason(strategy.bars) : null;

  const load = useCallback(() => {
    fetchStrategy(strategyId).then(setStrategy).catch((c: Error) => setError(c.message));
    fetchBacktests(strategyId).then(setRuns).catch((c: Error) => setError(c.message));
  }, [strategyId]);

  useEffect(load, [load]);

  async function onRun(event: React.FormEvent) {
    event.preventDefault();
    setRunning(true);
    setRefusal([]);
    setError(null);
    try {
      const result = await runBacktest(strategyId, {
        start,
        end,
        // Blank means "use what the strategy declared" -- an empty field is
        // not a request for zero capital.
        ...(capital.trim() === "" ? {} : { starting_cash: capital.trim() }),
        ...(dailyLoss.trim() === "" ? {} : { max_daily_loss: dailyLoss.trim() }),
        ...(drawdownPct.trim() === "" ? {} : { max_drawdown_pct: drawdownPct.trim() }),
      });
      if (result.backtest_run_id !== null) {
        router.push(`/backtests/${result.backtest_run_id}`);
        return;
      }
      // Refused before it ran. The findings 3b writes for an agent read
      // perfectly to a person, so they are shown as written rather than
      // rewritten into a second text that could drift from the first.
      setRefusal(result.findings);
      load();
    } catch (cause) {
      setError((cause as Error).message);
    } finally {
      setRunning(false);
    }
  }

  async function onGoLive() {
    if (selectedId === null) return;
    setStarting(true);
    setLiveError(null);
    try {
      const run = await startLiveRun(strategyId, selectedId);
      router.push(`/live/${run.live_run_id}`);
    } catch (cause) {
      setLiveError((cause as Error).message);
      setStarting(false);
    }
  }

  return (
    <main className="mx-auto flex max-w-5xl flex-col gap-8 px-6 py-10">
      <header className="flex flex-col gap-2">
        <Link href="/strategies" className="text-muted hover:text-text text-xs">
          ← strategies
        </Link>
        <h1 className="font-display text-2xl">
          {strategy ? `${strategy.name} ${strategy.version}` : `Strategy ${strategyId}`}
        </h1>
        {strategy ? (
          <p className="text-muted text-sm">
            {strategy.status} · contract {strategy.contract_version} · bars{" "}
            {strategy.bars ?? UNDEFINED_METRIC}
          </p>
        ) : null}
      </header>

      <section className="border-line bg-surface flex flex-col gap-3 rounded border p-4">
        <h2 className="font-display text-lg">Run backtest</h2>
        {blocked ? (
          <p className="border-down/40 bg-raised text-muted rounded border-l-2 px-3 py-2 text-xs">
            {blocked}
          </p>
        ) : (
          <p className="text-muted text-xs">
            Daily bars only. Data runs to 2026-08-21; a window past that is refused rather
            than run on nothing. Leave starting equity blank to use what the strategy
            declared, and the same for the risk limits. A run that stops early was
            halted by whichever limit applied — widening it here beats re-uploading the
            strategy under a new version.
          </p>
        )}
        <form onSubmit={onRun} className="flex flex-wrap items-end gap-3">
          <label className="flex flex-col gap-1">
            <span className="text-muted text-xs tracking-wide uppercase">Start</span>
            <input
              type="date"
              value={start}
              onChange={(e) => setStart(e.target.value)}
              className="border-line bg-raised num rounded border px-2 py-1 text-sm"
            />
          </label>
          <label className="flex flex-col gap-1">
            <span className="text-muted text-xs tracking-wide uppercase">End</span>
            <input
              type="date"
              value={end}
              onChange={(e) => setEnd(e.target.value)}
              className="border-line bg-raised num rounded border px-2 py-1 text-sm"
            />
          </label>
          <label className="flex flex-col gap-1">
            <span className="text-muted text-xs tracking-wide uppercase">Starting equity</span>
            <input
              type="text"
              inputMode="decimal"
              value={capital}
              placeholder="as declared"
              onChange={(e) => setCapital(e.target.value)}
              className="border-line bg-raised num w-36 rounded border px-2 py-1 text-sm"
            />
          </label>
          <label className="flex flex-col gap-1">
            <span className="text-muted text-xs tracking-wide uppercase">Max daily loss</span>
            <input
              type="text"
              inputMode="decimal"
              value={dailyLoss}
              placeholder="as declared"
              onChange={(e) => setDailyLoss(e.target.value)}
              className="border-line bg-raised num w-32 rounded border px-2 py-1 text-sm"
            />
          </label>
          <label className="flex flex-col gap-1">
            <span className="text-muted text-xs tracking-wide uppercase">Max drawdown %</span>
            <input
              type="text"
              inputMode="decimal"
              value={drawdownPct}
              placeholder="as declared"
              onChange={(e) => setDrawdownPct(e.target.value)}
              className="border-line bg-raised num w-32 rounded border px-2 py-1 text-sm"
            />
          </label>
          <button
            type="submit"
            disabled={running || blocked !== null}
            className="border-live text-live hover:bg-raised rounded border px-4 py-1.5 text-sm disabled:opacity-50"
          >
            {running ? "Running…" : "Run backtest"}
          </button>
        </form>
        {refusal.length > 0 ? (
          <ul className="flex flex-col gap-2">
            {refusal.map((finding, index) => (
              <li key={index} className="border-down/40 bg-raised rounded border-l-2 px-3 py-2">
                <span className="num text-down text-xs">{finding.code}</span>
                <p className="text-muted mt-1 text-xs">{finding.message}</p>
              </li>
            ))}
          </ul>
        ) : null}
        {error ? <p className="text-down text-xs">{error}</p> : null}
      </section>

      <section className="border-line bg-surface flex flex-col gap-3 rounded border p-4">
        <h2 className="font-display text-lg">Trade live</h2>
        <p className="text-muted text-xs">
          Runs the same code forward against live prices, in the same sandbox, placing
          simulated orders in the portfolio you pick. Dispatch is one-minute closed bars
          whatever the strategy declared for backtests, and one portfolio holds one live
          run at a time.
        </p>
        <div className="flex flex-wrap items-end gap-3">
          <label className="flex flex-col gap-1">
            <span className="text-muted text-xs tracking-wide uppercase">Portfolio</span>
            <select
              value={selectedId ?? ""}
              onChange={(e) => setSelectedId(Number(e.target.value))}
              className="border-line bg-raised rounded border px-2 py-1 text-sm"
            >
              {portfolios.map((p) => (
                <option key={p.portfolio_id} value={p.portfolio_id}>
                  {p.name} ({p.base_currency})
                </option>
              ))}
            </select>
          </label>
          <button
            type="button"
            onClick={onGoLive}
            disabled={starting || selectedId === null}
            className="border-live text-live hover:bg-raised rounded border px-4 py-1.5 text-sm disabled:opacity-50"
          >
            {starting ? "Starting…" : "Start live run"}
          </button>
          <Link href="/live" className="text-muted hover:text-text py-1.5 text-xs">
            all live runs →
          </Link>
        </div>
        {liveError ? <p className="text-down text-xs">{liveError}</p> : null}
      </section>

      <section className="flex flex-col gap-3">
        <h2 className="font-display text-lg">Runs</h2>
        {runs.length === 0 ? (
          <p className="text-muted text-sm">
            {blocked
              ? "No backtests, and none possible until this strategy declares daily bars."
              : "No backtests yet. Pick a window above and run one."}
          </p>
        ) : (
          <table className="w-full text-sm">
            <thead>
              <tr className="text-muted border-line border-b text-left text-xs uppercase">
                <th className="py-2 font-normal">Run</th>
                <th className="py-2 font-normal">Window</th>
                <th className="py-2 text-right font-normal">Sessions</th>
                <th className="py-2 text-right font-normal">Fills</th>
                <th className="py-2 text-right font-normal">Final equity</th>
                <th className="py-2 font-normal">Status</th>
              </tr>
            </thead>
            <tbody>
              {runs.map((run) => (
                <tr key={run.backtest_run_id} className="border-line/60 border-b">
                  <td className="py-2">
                    <Link href={`/backtests/${run.backtest_run_id}`} className="text-live num">
                      {run.backtest_run_id}
                    </Link>
                  </td>
                  <td className="num text-muted py-2 text-xs">
                    {run.requested_start} → {run.requested_end}
                  </td>
                  <td className="num py-2 text-right">{run.sessions}</td>
                  <td className="num py-2 text-right">{run.fills}</td>
                  <td className="num py-2 text-right">
                    {run.final_equity ?? UNDEFINED_METRIC}
                  </td>
                  <td className="py-2">
                    <span className={run.status === "PASSED" ? "text-up" : "text-down"}>
                      {run.status}
                    </span>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </section>
    </main>
  );
}
