"use client";

import Link from "next/link";
import { useEffect, useMemo, useState } from "react";
import {
  ApiError,
  fetchContractBundle,
  fetchStrategies,
  uploadStrategy,
  type ContractBundle,
  type RegisteredStrategy,
  type RunSummary,
  type StrategyVerdict,
  type UploadStrategyResult,
} from "@/lib/api";
import {
  SERVED_BARS,
  UNSERVED_BARS,
  dailyClockCaveat,
  describeWindow,
  explainNoFills,
  orderForDisplay,
} from "@/lib/strategies";
import {
  FIX_VARIATION_ID,
  VARIATIONS,
  composeFixPrompt,
  composePrompt,
  type PromptVariation,
} from "@/lib/prompts";

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
        # Every symbol here must exist in \`instruments\`, and \`data.bars\`
        # must be an interval this platform actually serves -- see the note
        # below the Source box -- or the smoke run rejects.
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

type CopyTarget = "prompt" | "contract" | "sdk";

/**
 * Money for display. Parsed here and nowhere else: these arrive as strings
 * precisely so no float exists in the path, and the moment one is used for
 * arithmetic that guarantee is gone.
 */
function money(value: string): string {
  return Number(value).toLocaleString(undefined, {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  });
}

function signed(value: string): string {
  const n = Number(value);
  return `${n >= 0 ? "+" : ""}${money(value)}`;
}

/** Bytes as an at-a-glance size, so a 42KB paste is not a surprise. */
function formatSize(text: string): string {
  return `${Math.round(new Blob([text]).size / 1024)}KB`;
}

/**
 * Equity and P&L. Shown for every completed run, pass or fail: orders and
 * fills say the code ran, and only this says whether it was worth running
 * -- which is the number you compare two agents on.
 *
 * P&L is omitted rather than zeroed when the baseline is unknown. Showing
 * "+0.00" for "we do not know" would be a fabricated number in the
 * direction that flatters.
 */
function PerformanceRow({ summary }: { summary: RunSummary }) {
  const pnl = summary.pnl === null ? null : Number(summary.pnl);
  const tone = pnl === null ? "text-text" : pnl > 0 ? "text-up" : pnl < 0 ? "text-down" : "text-muted";
  const noFills = explainNoFills(summary);
  return (
    <div className="flex flex-wrap items-baseline gap-x-6 gap-y-1">
      {summary.final_equity !== null && (
        <span className="flex items-baseline gap-2">
          <span className="text-muted text-xs uppercase tracking-wide">Equity</span>
          <span className="num text-lg">{money(summary.final_equity)}</span>
          <span className="text-muted text-xs">{summary.currency}</span>
        </span>
      )}
      {summary.pnl !== null && (
        <span className="flex items-baseline gap-2">
          <span className="text-muted text-xs uppercase tracking-wide">P&amp;L</span>
          <span className={`num text-lg ${tone}`}>{signed(summary.pnl)}</span>
          {summary.pnl_pct !== null && (
            <span className={`num text-xs ${tone}`}>{signed(summary.pnl_pct)}%</span>
          )}
        </span>
      )}
      <span className="num text-muted text-xs">
        {summary.bar_calls.toLocaleString()} bars · {summary.orders} orders · {summary.fills} fills
      </span>
      {/*
        `text-live` here, not `text-down`: nothing failed -- the run
        completed -- this is the system flagging something that needs a
        look. Without it, "3 orders · 0 fills" reads as a strategy that
        chose not to trade, when every order in fact bounced.
      */}
      {noFills !== null && <span className="text-live text-xs">{noFills}</span>}
    </div>
  );
}

export default function StrategiesPage() {
  const [name, setName] = useState("my-strategy");
  const [version, setVersion] = useState("1.0.0");
  const [source, setSource] = useState(STARTER);
  const [result, setResult] = useState<UploadStrategyResult | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [running, setRunning] = useState(false);
  const [copied, setCopied] = useState(false);
  const [variationId, setVariationId] = useState(VARIATIONS[0].id);
  const [kitOpen, setKitOpen] = useState(true);
  const [kitCopied, setKitCopied] = useState<CopyTarget | null>(null);
  const [kitError, setKitError] = useState<string | null>(null);

  // Fetched on first copy rather than on mount: the bundle is ~42KB and
  // only a session that actually reaches for a prompt needs it. Held in
  // state rather than a ref because its size and version are rendered,
  // and a ref cannot be read during render.
  const [bundle, setBundle] = useState<ContractBundle | null>(null);

  // The registered-strategies list. `null` means "still loading" so the
  // empty state ("nothing registered yet") is never shown for the instant
  // before the first response arrives.
  const [strategies, setStrategies] = useState<RegisteredStrategy[] | null>(null);
  const [strategiesError, setStrategiesError] = useState<string | null>(null);

  async function reloadStrategies() {
    try {
      const list = await fetchStrategies();
      setStrategies(list);
      setStrategiesError(null);
    } catch (err) {
      setStrategiesError(err instanceof ApiError ? err.message : String(err));
    }
  }

  useEffect(() => {
    // eslint-disable-next-line react-hooks/set-state-in-effect
    reloadStrategies();
  }, []);

  const variation = useMemo(
    () => VARIATIONS.find((v) => v.id === variationId) ?? VARIATIONS[0],
    [variationId],
  );

  async function ensureBundle(): Promise<ContractBundle> {
    if (bundle !== null) return bundle;
    const fetched = await fetchContractBundle();
    setBundle(fetched);
    return fetched;
  }

  async function copyKit(target: CopyTarget) {
    setKitError(null);
    try {
      let text: string;
      if (target === "prompt" && variation.id === FIX_VARIATION_ID) {
        if (result === null) {
          setKitError("Upload something first — this prompt carries the report you got back.");
          return;
        }
        text = composeFixPrompt(result.feedback);
      } else {
        const bundle = await ensureBundle();
        if (target === "contract") text = bundle.contract;
        else if (target === "sdk") text = bundle.sdk_stub;
        else text = composePrompt(bundle.contract, bundle.contract_version, variation);
      }
      await navigator.clipboard.writeText(text);
      setKitCopied(target);
    } catch (err) {
      setKitError(err instanceof ApiError ? err.message : String(err));
    }
  }

  async function submit(event: React.FormEvent) {
    event.preventDefault();
    setRunning(true);
    setError(null);
    setResult(null);
    setCopied(false);
    try {
      const uploaded = await uploadStrategy({ name, version, source });
      setResult(uploaded);
      // After a rejection the next prompt is always the follow-up, and it
      // embeds the report that just arrived. Selected here, where the
      // verdict is known, rather than in an effect watching for it.
      if (!uploaded.accepted) {
        setVariationId(FIX_VARIATION_ID);
        setKitCopied(null);
        setKitOpen(true);
      } else {
        // Only an accepted upload can have changed the registered list --
        // a rejection stores nothing (`strategy_smoke_runs.strategy_id` is
        // NOT NULL, so a rejected run has no row to hang on) -- but
        // reloading unconditionally would be harmless too; this just
        // avoids a request that can never return something new.
        await reloadStrategies();
      }
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

      <RegisteredStrategiesSection
        strategies={strategies}
        error={strategiesError}
      />

      <section className="border-b border-line px-4 py-4 sm:px-8">
        <button
          type="button"
          onClick={() => setKitOpen((open) => !open)}
          className="flex items-center gap-2 text-sm text-muted hover:text-text"
          aria-expanded={kitOpen}
        >
          <span className="font-display text-text">Prompt kit</span>
          <span>— hand the contract to an external agent</span>
          <span aria-hidden="true">{kitOpen ? "▾" : "▸"}</span>
        </button>

        {kitOpen && (
          <div className="mt-4 flex flex-col gap-4">
            <ol className="text-muted text-xs flex flex-col gap-1 max-w-prose list-decimal pl-4">
              <li>
                Pick a variation, hit <span className="text-text">Copy full prompt</span>, and paste
                the whole thing into a fresh chat with any capable model.
              </li>
              <li>
                Paste what it writes into the Source box below and upload. It is checked, then run
                against real bars.
              </li>
              <li>
                On a rejection, this panel switches to <span className="text-text">Fix a rejection</span>{" "}
                with the report already in it. Send that in the same chat, then resubmit with a
                bumped version.
              </li>
            </ol>

            <div className="flex flex-wrap gap-2">
              {VARIATIONS.map((v: PromptVariation) => (
                <button
                  key={v.id}
                  type="button"
                  onClick={() => {
                    setVariationId(v.id);
                    setKitCopied(null);
                  }}
                  className={`rounded border px-2.5 py-1 text-xs ${
                    v.id === variation.id
                      ? "border-live text-text"
                      : "border-line text-muted hover:text-text"
                  }`}
                >
                  {v.label}
                </button>
              ))}
            </div>

            <p className="text-muted text-xs max-w-prose">
              <span className="text-text">{variation.group}.</span> {variation.blurb}
            </p>

            <div className="flex flex-wrap items-center gap-3">
              <button
                type="button"
                onClick={() => copyKit("prompt")}
                className="bg-raised border border-line rounded px-3 py-1.5 text-sm hover:border-live"
              >
                {kitCopied === "prompt" ? "Copied" : "Copy full prompt"}
              </button>
              <button
                type="button"
                onClick={() => copyKit("contract")}
                className="text-muted hover:text-text text-xs border border-line rounded px-2 py-1"
              >
                {kitCopied === "contract" ? "Copied" : "Contract only"}
              </button>
              <button
                type="button"
                onClick={() => copyKit("sdk")}
                className="text-muted hover:text-text text-xs border border-line rounded px-2 py-1"
              >
                {kitCopied === "sdk" ? "Copied" : "SDK stub"}
              </button>
              {bundle !== null && (
                <span className="num text-muted text-xs">
                  contract {formatSize(bundle.contract)} · v{bundle.contract_version}
                </span>
              )}
            </div>

            {kitError !== null && <p className="text-down text-xs">{kitError}</p>}

            <p className="text-muted text-xs max-w-prose">
              The contract is sent first and the ask last — an instruction placed above 600 lines of
              specification competes with it for attention. Where your agent accepts file
              attachments, <span className="text-text">Contract only</span> plus a one-line ask works
              just as well. <span className="text-text">SDK stub</span> is for agents that can run
              code: importing against it catches a misspelled method before you spend a round trip,
              and every call raising <span className="num">NotOnThisPlatform</span> is the expected
              result, not a failure.
            </p>
          </div>
        )}
      </section>

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

          {/*
            Said before upload, not just after a rejection: without this, a
            user writes `data.bars="5m"`, uploads, and reads the resulting
            MANIFEST_UNRESOLVABLE as their own bug rather than a platform
            limit. The contract permits all five; this platform serves two.
          */}
          <p className="text-muted text-xs max-w-prose">
            <span className="text-text">{SERVED_BARS.join(" and ")}</span> bars are served.{" "}
            <span className="num">{UNSERVED_BARS.join(", ")}</span> are contract-legal but rejected
            — the platform has no bar aggregation for them yet.
          </p>

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

              {result.summary !== null && <PerformanceRow summary={result.summary} />}

              {result.window !== null && (
                <>
                  <p className="num text-muted text-xs">{describeWindow(result.window)}</p>
                  {/*
                    Daily bars are timestamped at session close while the
                    Bar contract defines the interval start, so a daily run's
                    simulated clock sits a uniform one day behind -- worth
                    surfacing every time, not just in a release note.
                  */}
                  {dailyClockCaveat(result.window.bars) !== null && (
                    <p className="text-muted text-xs">{dailyClockCaveat(result.window.bars)}</p>
                  )}
                </>
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

              {/*
                The structured findings, above the prose. `feedback` is the
                narrative report; this is the same facts as data -- a code
                plus a contract section is what lets someone spot
                "MANIFEST_UNRESOLVABLE §3" at a glance instead of reading a
                paragraph to find it.
              */}
              {result.findings.length > 0 && (
                <div className="flex flex-col gap-1.5">
                  <span className="text-muted text-xs uppercase tracking-wide">Findings</span>
                  <ul className="flex flex-col gap-1.5">
                    {result.findings.map((f, i) => (
                      <li
                        key={i}
                        className="flex flex-wrap items-baseline gap-x-2 gap-y-0.5 text-xs border border-line rounded px-2 py-1.5 bg-surface"
                      >
                        {/*
                          A report's findings share one class: `smoke_test`
                          only reaches REJECTED when at least one finding is
                          a hard-fail code, and PASSED_WITH_WARNINGS only
                          exists when none are -- so the overall verdict's
                          tone is the correct tone for every finding in it,
                          without the frontend needing its own copy of the
                          backend's fail-code list.
                        */}
                        <span className={`num ${VERDICT_TONE[result.verdict]}`}>{f.code}</span>
                        {f.contract_section !== "" && (
                          <span className="text-muted">§{f.contract_section}</span>
                        )}
                        {f.line !== null && <span className="num text-muted">line {f.line}</span>}
                        <span className="text-text w-full">{f.message}</span>
                      </li>
                    ))}
                  </ul>
                </div>
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

/**
 * What's already registered, above the upload form.
 *
 * Only PASSING uploads ever appear here: `strategy_smoke_runs.strategy_id`
 * is a NOT NULL foreign key to `strategies`, so a rejected upload never
 * gets a run row to hang off a strategy in the first place -- there is
 * nothing to list.
 *
 * Sorted by name, then by registration time, so two versions of the same
 * strategy sit on adjacent rows. That adjacency is the point: it is what
 * makes the platform's immutability rule (re-registering a version with
 * different code is refused; ship a bump instead) visible rather than
 * asserted -- you can see both attempts side by side.
 */
function RegisteredStrategiesSection({
  strategies,
  error,
}: {
  strategies: RegisteredStrategy[] | null;
  error: string | null;
}) {
  const sorted = useMemo(
    () => (strategies === null ? null : orderForDisplay(strategies)),
    [strategies],
  );

  return (
    <section className="border-b border-line px-4 py-4 sm:px-8">
      <h2 className="font-display text-sm tracking-wide mb-3">REGISTERED STRATEGIES</h2>

      {error !== null && (
        <p className="text-down text-sm">Could not load registered strategies: {error}</p>
      )}

      {error === null && sorted === null && (
        <p className="text-muted text-sm">Loading…</p>
      )}

      {error === null && sorted !== null && sorted.length === 0 && (
        <p className="text-muted text-sm">Nothing registered yet.</p>
      )}

      {error === null && sorted !== null && sorted.length > 0 && (
        <div className="overflow-x-auto">
          <table className="w-full text-sm border border-line rounded">
            <thead>
              <tr className="text-muted text-xs uppercase tracking-wide bg-surface">
                <th className="text-left font-normal px-3 py-2">Name</th>
                <th className="text-left font-normal px-3 py-2">Version</th>
                <th className="text-left font-normal px-3 py-2">Bars</th>
                <th className="text-left font-normal px-3 py-2">Latest run</th>
                <th className="text-right font-normal px-3 py-2">Sessions</th>
                <th className="text-right font-normal px-3 py-2">Final equity</th>
                <th className="text-left font-normal px-3 py-2">Contract</th>
                <th className="text-right font-normal px-3 py-2">Backtest</th>
              </tr>
            </thead>
            <tbody>
              {sorted.map((s) => (
                <tr key={s.strategy_id} className="border-t border-line hover:bg-surface">
                  <td className="px-3 py-2">
                    {/* The way into backtesting: without this the strategy
                        page and its reports exist but nothing reaches them. */}
                    <Link href={`/strategies/${s.strategy_id}`} className="text-live">
                      {s.name}
                    </Link>
                  </td>
                  <td className="num px-3 py-2 text-muted">{s.version}</td>
                  <td className="num px-3 py-2 text-muted">{s.bars ?? "--"}</td>
                  <td className="px-3 py-2">
                    {s.latest_run === null ? (
                      <span className="text-muted">--</span>
                    ) : (
                      <span className={VERDICT_TONE[s.latest_run.verdict]}>
                        {VERDICT_LABEL[s.latest_run.verdict]}
                      </span>
                    )}
                  </td>
                  <td className="num text-right px-3 py-2">
                    {s.latest_run !== null ? s.latest_run.sessions : "--"}
                  </td>
                  <td className="num text-right px-3 py-2">
                    {s.latest_run?.final_equity !== null && s.latest_run?.final_equity !== undefined
                      ? money(s.latest_run.final_equity)
                      : "--"}
                  </td>
                  <td className="num px-3 py-2 text-muted">v{s.contract_version}</td>
                  <td className="px-3 py-2 text-right">
                    <Link href={`/strategies/${s.strategy_id}`} className="text-live text-xs">
                      run &amp; reports →
                    </Link>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </section>
  );
}
