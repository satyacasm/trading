"use client";

import Link from "next/link";
import { useMemo, useState } from "react";
import {
  ApiError,
  fetchContractBundle,
  uploadStrategy,
  type ContractBundle,
  type StrategyVerdict,
  type UploadStrategyResult,
} from "@/lib/api";
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

type CopyTarget = "prompt" | "contract" | "sdk";

/** Bytes as an at-a-glance size, so a 42KB paste is not a surprise. */
function formatSize(text: string): string {
  return `${Math.round(new Blob([text]).size / 1024)}KB`;
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
