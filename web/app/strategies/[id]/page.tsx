"use client";

import Link from "next/link";
import { useParams, useRouter } from "next/navigation";
import { useCallback, useEffect, useState } from "react";
import {
  fetchBacktests,
  fetchStrategy,
  runBacktest,
  type BacktestSummary,
  type RegisteredStrategy,
  type StrategyFinding,
} from "@/lib/api";
import { UNDEFINED_METRIC } from "@/lib/backtests";

export default function StrategyPage() {
  const params = useParams<{ id: string }>();
  const router = useRouter();
  const strategyId = Number(params.id);

  const [strategy, setStrategy] = useState<RegisteredStrategy | null>(null);
  const [runs, setRuns] = useState<BacktestSummary[]>([]);
  const [error, setError] = useState<string | null>(null);

  const [start, setStart] = useState("2020-01-01");
  const [end, setEnd] = useState("2026-08-21");
  const [running, setRunning] = useState(false);
  const [refusal, setRefusal] = useState<StrategyFinding[]>([]);

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
      const result = await runBacktest(strategyId, { start, end });
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
        <p className="text-muted text-xs">
          Daily bars only. Data runs to 2026-08-21; a window past that is refused rather
          than run on nothing.
        </p>
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
          <button
            type="submit"
            disabled={running}
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

      <section className="flex flex-col gap-3">
        <h2 className="font-display text-lg">Runs</h2>
        {runs.length === 0 ? (
          <p className="text-muted text-sm">
            No backtests yet. Pick a window above and run one.
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
