"use client";

import Link from "next/link";
import { useState } from "react";
import { ApiError, uploadStrategy, type StrategyVerdict, type UploadStrategyResult } from "@/lib/api";

/**
 * Upload → validate → smoke → register, in one blocking request.
 *
 * The report is the product, not a side effect. §9 is built around handing
 * an agent findings it can act on, so the verdict block is the page's
 * centre of gravity and carries a copy action -- its destination is
 * usually another model's prompt, not this screen.
 *
 * Verdict colour follows the palette's two-channel rule (globals.css):
 * up/down mean *direction*, `live` means *the system wants your attention*.
 * A warned pass is not good news and not a failure -- it is a pass with
 * something unexercised -- so it takes `live` rather than a green that
 * would read as clean at a glance, exactly as PARTIALLY_FILLED does in the
 * blotter.
 */
const VERDICT_TONE: Record<StrategyVerdict, string> = {
  PASSED: "text-up",
  PASSED_WITH_WARNINGS: "text-live",
  REJECTED: "text-down",
};

const VERDICT_LABEL: Record<StrategyVerdict, string> = {
  PASSED: "Passed",
  PASSED_WITH_WARNINGS: "Passed with warnings",
  REJECTED: "Rejected",
};

const STARTER = `from decimal import Decimal
from platform_sdk import DataRequest, InstrumentRef, Strategy, StrategyManifest


class MyStrategy(Strategy):
    def configure(self):
        # Every symbol here must exist in \`instruments\` and have 1-minute
        # bars, or the smoke run rejects with NO_DATA.
        return StrategyManifest(
            name="my-strategy",
            version="1.0.0",
            universe=[
                InstrumentRef(exchange="NSE", segment="CM", symbol="RELIANCE"),
            ],
            data=DataRequest(bars="1m", history_bars=20),
            capital=Decimal("1000000"),
            base_currency="INR",
        )

    def initialize(self, ctx):
        self.bought = False

    def on_bar(self, ctx, bars):
        if not self.bought:
            self.bought = True
            ctx.order(
                list(bars)[0],
                side="BUY",
                quantity=Decimal("1"),
                rationale="first bar of the window",
            )
`;

export default function StrategiesPage() {
  const [name, setName] = useState("my-strategy");
  const [version, setVersion] = useState("1.0.0");
  const [source, setSource] = useState(STARTER);
  const [result, setResult] = useState<UploadStrategyResult | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [running, setRunning] = useState(false);
  const [copied, setCopied] = useState(false);

  async function submit(event: React.FormEvent) {
    event.preventDefault();
    setRunning(true);
    setError(null);
    setResult(null);
    setCopied(false);
    try {
      setResult(await uploadStrategy({ name, version, source }));
    } catch (err) {
      setError(err instanceof ApiError ? err.message : String(err));
    } finally {
      setRunning(false);
    }
  }

  async function copyFeedback() {
    if (result === null) return;
    await navigator.clipboard.writeText(result.feedback);
    setCopied(true);
  }

  return (
    <main className="flex flex-col min-h-full">
      <header className="flex flex-wrap items-end justify-between gap-4 border-b border-line px-4 py-4 sm:px-8">
        <div className="flex flex-col gap-1 min-w-0">
          <Link href="/" className="text-muted hover:text-text text-sm w-fit">
            ← Watchlist
          </Link>
          <h1 className="font-display text-2xl sm:text-3xl">Strategies</h1>
          <p className="text-muted text-sm max-w-prose">
            Your code is checked, then run against five sessions of real bars — twice, so its
            orders can be compared. Only a passing strategy is registered.
          </p>
        </div>
      </header>

      <div className="grid gap-6 px-4 py-6 sm:px-8 lg:grid-cols-2">
        <form onSubmit={submit} className="flex flex-col gap-4 min-w-0">
          <div className="flex flex-wrap gap-4">
            <label className="flex flex-col gap-1 flex-1 min-w-40">
              <span className="text-muted text-xs uppercase tracking-wide">Name</span>
              <input
                value={name}
                onChange={(e) => setName(e.target.value)}
                required
                className="bg-raised border border-line rounded px-2 py-1.5 text-sm"
              />
            </label>
            <label className="flex flex-col gap-1 w-32">
              <span className="text-muted text-xs uppercase tracking-wide">Version</span>
              <input
                value={version}
                onChange={(e) => setVersion(e.target.value)}
                required
                className="num bg-raised border border-line rounded px-2 py-1.5 text-sm"
              />
            </label>
          </div>

          <label className="flex flex-col gap-1">
            <span className="text-muted text-xs uppercase tracking-wide">Source</span>
            <textarea
              value={source}
              onChange={(e) => setSource(e.target.value)}
              spellCheck={false}
              rows={26}
              className="font-mono bg-surface border border-line rounded p-3 text-xs leading-relaxed resize-y"
            />
          </label>

          <div className="flex items-center gap-4">
            <button
              type="submit"
              disabled={running}
              className="bg-raised border border-line rounded px-4 py-2 text-sm hover:border-live disabled:opacity-50 disabled:hover:border-line"
            >
              {running ? "Running…" : "Upload and smoke-test"}
            </button>
            {running && (
              <span className="text-live text-xs">
                Three containers: configure, then the smoke payload twice. This takes a few
                seconds.
              </span>
            )}
          </div>

          <p className="text-muted text-xs max-w-prose">
            A version is immutable. Re-uploading {name || "a strategy"} {version} with different
            code is refused — publish a change as a new version.
          </p>
        </form>

        <section className="flex flex-col gap-3 min-w-0">
          {error !== null && (
            <p className="text-down text-sm border border-line rounded p-3 bg-surface">{error}</p>
          )}

          {result === null && error === null && !running && (
            <p className="text-muted text-sm border border-line border-dashed rounded p-6">
              No run yet. Paste a strategy and upload it to see what the platform makes of it.
            </p>
          )}

          {result !== null && (
            <>
              <div className="flex flex-wrap items-baseline justify-between gap-3">
                <span className={`font-display text-xl ${VERDICT_TONE[result.verdict]}`}>
                  {VERDICT_LABEL[result.verdict]}
                </span>
                {result.strategy_id !== null && (
                  <span className="text-muted text-xs">
                    Registered as strategy <span className="num text-text">#{result.strategy_id}</span>
                  </span>
                )}
              </div>

              {result.window !== null && result.window.start !== null && (
                <p className="text-muted text-xs">
                  Window: <span className="num">{result.window.start.slice(0, 10)}</span> to{" "}
                  <span className="num">{result.window.end?.slice(0, 10)}</span> ·{" "}
                  <span className="num">{result.window.sessions}</span> sessions
                </p>
              )}

              {/*
                Always shown, never only on a pass. The sandbox records
                `kernel_isolated` precisely so a result can never be read as
                better confined than it was, and hiding it in the UI would
                undo that at the last step.
              */}
              {result.runtime !== null && (
                <p className="text-xs">
                  <span className="text-muted">Isolation: </span>
                  {result.kernel_isolated ? (
                    <span className="text-live">
                      runtime={result.runtime} — syscalls mediated by a user-space kernel
                    </span>
                  ) : (
                    <span className="text-muted">
                      runtime={result.runtime} — namespaces and seccomp only; the host kernel is
                      shared
                    </span>
                  )}
                </p>
              )}

              <div className="flex items-center justify-between gap-3">
                <span className="text-muted text-xs uppercase tracking-wide">Report</span>
                <button
                  type="button"
                  onClick={copyFeedback}
                  className="text-muted hover:text-text text-xs border border-line rounded px-2 py-1"
                >
                  {copied ? "Copied" : "Copy for your agent"}
                </button>
              </div>
              <pre className="font-mono bg-surface border border-line rounded p-3 text-xs leading-relaxed whitespace-pre-wrap overflow-x-auto">
                {result.feedback}
              </pre>
            </>
          )}
        </section>
      </div>
    </main>
  );
}
