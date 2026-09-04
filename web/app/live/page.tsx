"use client";

import Link from "next/link";
import { useCallback, useEffect, useState } from "react";
import { fetchLiveRuns, type LiveRun } from "@/lib/api";

const STATUS_TONE: Record<string, string> = {
  RUNNING: "text-up",
  STOPPED: "text-muted",
  CRASHED: "text-down",
};

/** Live runs, newest first. Polls, because a run's state changes without you. */
export default function LiveRunsPage() {
  const [runs, setRuns] = useState<LiveRun[] | null>(null);
  const [error, setError] = useState<string | null>(null);

  const load = useCallback(() => {
    fetchLiveRuns()
      .then((rows) => {
        setRuns(rows);
        setError(null);
      })
      .catch((cause: Error) => setError(cause.message));
  }, []);

  useEffect(() => {
    load();
    // Five seconds: the supervisor reconciles on the same cadence, so a
    // faster poll would show states that have not been acted on yet.
    const timer = setInterval(load, 5000);
    return () => clearInterval(timer);
  }, [load]);

  return (
    <main className="mx-auto flex max-w-5xl flex-col gap-6 px-6 py-10">
      <header className="flex flex-col gap-2">
        <Link href="/" className="text-muted hover:text-text w-fit text-sm">
          ← terminal
        </Link>
        <h1 className="font-display text-2xl">Live runs</h1>
        <p className="text-muted text-sm">
          Strategies trading forward against live prices. Start one from a
          strategy&apos;s page; the supervisor picks it up within a few seconds.
        </p>
      </header>

      {error ? <p className="text-down text-sm">{error}</p> : null}

      {runs === null ? (
        <p className="text-muted text-sm">Loading…</p>
      ) : runs.length === 0 ? (
        <p className="text-muted text-sm">
          Nothing running. Open a strategy and start it against a portfolio.
        </p>
      ) : (
        <table className="w-full text-sm">
          <thead>
            <tr className="text-muted border-line border-b text-left text-xs uppercase">
              <th className="py-2 font-normal">Run</th>
              <th className="py-2 font-normal">Status</th>
              <th className="py-2 text-right font-normal">Bars</th>
              <th className="py-2 text-right font-normal">Orders</th>
              <th className="py-2 font-normal">Isolation</th>
              <th className="py-2 font-normal">Started</th>
            </tr>
          </thead>
          <tbody>
            {runs.map((run) => (
              <tr
                key={run.live_run_id}
                className="border-line/60 hover:bg-surface border-b"
              >
                <td className="py-2">
                  <Link
                    href={`/live/${run.live_run_id}`}
                    className="text-live num"
                  >
                    {run.live_run_id}
                  </Link>
                </td>
                <td
                  className={`py-2 ${STATUS_TONE[run.status] ?? "text-text"}`}
                >
                  {run.status}
                  {run.stopped_reason ? (
                    <span className="text-muted ml-2 text-xs">
                      {run.stopped_reason}
                    </span>
                  ) : null}
                </td>
                <td className="num py-2 text-right">{run.bars_seen}</td>
                <td className="num py-2 text-right">
                  {run.orders_placed}
                  {run.orders_refused > 0 ? (
                    <span className="text-down ml-1 text-xs" title={run.last_refusal ?? ""}>
                      +{run.orders_refused} refused
                    </span>
                  ) : null}
                </td>
                <td className="text-muted num py-2 text-xs">
                  {run.runtime ?? "--"}
                  {run.kernel_isolated ? " · kernel isolated" : ""}
                </td>
                <td className="text-muted num py-2 text-xs">
                  {new Date(run.started_at).toLocaleString()}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </main>
  );
}
