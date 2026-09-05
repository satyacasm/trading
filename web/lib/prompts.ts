/**
 * Prompts for handing this platform's contract to an external agent.
 *
 * The contract goes FIRST and the instruction last. With a document this
 * long, an instruction buried above 600 lines of specification competes
 * with the specification for attention; one that follows it reads as the
 * thing to act on. The same reason a brief ends with the ask.
 *
 * Every task below names the rules that generated strategies actually
 * break -- Decimal money, `ctx.now`, the import allowlist, a non-empty
 * rationale, a universe the platform has bars for. They are repeated
 * outside the contract deliberately: they are already in there, and
 * restating the five that account for most rejections is cheaper than a
 * round trip that discovers them one at a time.
 */

export type VariationGroup = "Archetype" | "Risk" | "Adversarial" | "Follow-up";

export type PromptVariation = {
  id: string;
  group: VariationGroup;
  label: string;
  /** Why you would reach for this one. Shown under the picker. */
  blurb: string;
  /** The ask, appended after the contract. Empty for the follow-up. */
  task: string;
};

/**
 * Symbols the platform has 1-minute bars for. A manifest naming anything
 * else is rejected with NO_DATA after three containers have already run,
 * so it is worth telling the agent up front.
 *
 * The daily universe is far larger -- the whole NSE bhavcopy, thousands of
 * instruments back to 2016 -- but a `bars="1m"` strategy is confined to
 * what this platform has actually recorded minute by minute.
 */
export const INSTRUMENTS_WITH_BARS = [
  "NSE/CM RELIANCE",
  "NSE/CM TCS",
  "NSE/CM INFY",
  "NSE/CM HDFCBANK",
  "NSE/CM ICICIBANK",
] as const;

const HOUSE_RULES = `Rules that generated strategies break most often. All are in the contract
above; they are repeated here because each one costs a round trip:

- Money and quantities are \`decimal.Decimal\`. A float raises TypeError.
- Read the time from \`ctx.now\`. \`datetime.now()\` is rejected outright --
  a strategy that reads the wall clock cannot be replayed.
- Import nothing outside the allowlist. There is no network and no
  filesystem inside the sandbox.
- Every order needs a non-empty \`rationale\`.
- Your universe must name instruments this platform has bars for. With
  \`bars="1m"\`, that is: ${INSTRUMENTS_WITH_BARS.join(", ")}. With
  \`bars="1d"\`, any NSE equity in the bhavcopy back to 2016.
- One asset class per strategy -- equities and crypto cannot be mixed,
  because a portfolio is single-currency.
- Declare \`bars="1d"\` if you want the strategy backtested. Backtests run
  on daily bars only, and a \`1m\` manifest is refused with
  BACKTEST_INTERVAL_UNSUPPORTED. A \`1m\` strategy still smoke-tests and
  still runs forward against live prices.`;

export const CUSTOM_VARIATION_ID = "custom";

export const VARIATIONS: PromptVariation[] = [
  {
    id: "buy-and-hold",
    group: "Archetype",
    label: "Buy and hold (control)",
    blurb:
      "Start here. It is the control: if the simplest possible strategy cannot pass, the fault is in the contract or the data, not in the agent.",
    task: `Write the simplest strategy that passes: buy a fixed quantity of one
instrument on the first bar you see, then hold it for the rest of the run.

This is a control. Prefer boring correctness over cleverness -- its job is
to prove the pipeline works end to end.`,
  },
  {
    id: "mean-reversion",
    group: "Archetype",
    label: "Mean reversion",
    blurb:
      "Exercises rolling history and a two-sided entry/exit rule. The most common archetype and a fair first real test.",
    task: `Write a mean-reversion strategy on a single NSE equity.

Track a rolling 20-bar mean of the close. Buy a fixed quantity when the
close is more than 1.5% below that mean, and sell the whole position when
it comes back to within 0.25% of it. Hold at most one open position at a
time, and do nothing until you have 20 bars of history.`,
  },
  {
    id: "sma-crossover",
    group: "Archetype",
    label: "SMA crossover",
    blurb:
      "Tests whether the agent acts on the crossing bar rather than on every bar the fast average happens to be above — the classic bug in this archetype.",
    task: `Write an SMA-crossover strategy on a single NSE equity.

Buy when a 10-bar simple moving average crosses above a 30-bar one, and
sell the whole position when it crosses back below.

Act only on the bar where the crossing happens, not on every bar where the
fast average is already above the slow one. Getting this wrong produces an
order on every subsequent bar.`,
  },
  {
    id: "opening-range-breakout",
    group: "Archetype",
    label: "Opening-range breakout",
    blurb:
      "The hardest archetype here: it needs session boundaries, so the agent has to use ctx.now correctly rather than counting bars.",
    task: `Write an opening-range-breakout strategy on a single NSE equity.

For each session, define the opening range as the high and low of that
session's first 15 bars. Buy a fixed quantity on the first close above the
range high, and flatten the position before the session ends.

Reset the range at the start of every new session. \`ctx.now\` is your only
clock -- work out session boundaries from it, not from a bar counter.`,
  },
  {
    id: "risk-constrained",
    group: "Risk",
    label: "Risk-constrained",
    blurb:
      "Declares circuit-breaker limits in the manifest, which a plain strategy never touches. This is how you make BREAKER_TRIPPED reachable.",
    task: `Write a mean-reversion strategy on a single NSE equity that declares its
own risk limits.

In the manifest, set \`max_daily_loss\` and \`max_drawdown_pct\` to values you
can justify against the declared capital. Size each position so no single
one exceeds 10% of capital.

The platform enforces these limits and will halt the run if you breach
them. Your job is to declare them honestly and stay inside them -- not to
declare limits so wide they can never bind.`,
  },
  {
    id: "adversarial",
    group: "Adversarial",
    label: "Contract stress-test",
    blurb:
      "Asks for code the contract forbids. You want this REJECTED — it tests your gate rather than the agent. Expect IMPORT_NOT_ALLOWED and WALL_CLOCK.",
    task: `I am testing whether this platform's validator actually rejects what its
contract forbids, so for this one request I want code that deliberately
breaks it.

Write a strategy that: fetches a price over HTTP, reads \`datetime.now()\`
instead of \`ctx.now\`, and passes a float as an order quantity.

I expect the platform to refuse this. Do not correct any of it.`,
  },
  {
    id: CUSTOM_VARIATION_ID,
    group: "Archetype",
    label: "Your own idea",
    blurb:
      "Describe the strategy in your own words. The contract and the house rules are attached for you -- write the idea, not the boilerplate.",
    task: "",
  },
  {
    id: "fix-rejection",
    group: "Follow-up",
    label: "Fix a rejection",
    blurb:
      "Round two. Send this in the same chat, right after a rejection — it frames the report so the agent patches what was named instead of rewriting everything.",
    task: "",
  },
];

export const FIX_VARIATION_ID = "fix-rejection";

/**
 * Placeholder task for an empty box.
 *
 * An empty custom prompt must not end mid-sentence and silently ask an
 * agent for nothing -- it should still read as a request, so a prompt
 * copied before the idea was typed produces a question rather than
 * confident nonsense.
 */
const CUSTOM_FALLBACK = `The operator has not written the strategy yet. Ask them what they want,
in one short question, and describe the strategy you would write once they
answer. Do not write code yet.`;

/**
 * A prompt built from the operator's own words.
 *
 * The presets cover the archetypes worth testing a contract against; this
 * covers the reason somebody opened the page, which is usually an idea the
 * presets do not have. The house rules stay attached, and matter more here
 * than anywhere else: a preset was written against the contract's
 * constraints, and something typed in a hurry was not.
 */
export function customVariation(task: string): PromptVariation {
  return {
    id: CUSTOM_VARIATION_ID,
    group: "Archetype",
    label: "Your own idea",
    blurb: "Describe the strategy in your own words. The contract and the house rules go with it.",
    task: task.trim() === "" ? CUSTOM_FALLBACK : task.trim(),
  };
}

function preamble(contractVersion: string): string {
  return `You have just read the complete strategy contract for a trading platform
(contract version ${contractVersion}). It is the only specification: there is no
other documentation, and anything it does not permit is rejected
automatically before your code ever runs.`;
}

/**
 * The full payload for a fresh conversation: contract, then the ask.
 */
export function composePrompt(
  contract: string,
  contractVersion: string,
  variation: PromptVariation,
): string {
  return [
    contract.trimEnd(),
    "",
    "--- END OF CONTRACT ---",
    "",
    preamble(contractVersion),
    "",
    variation.task.trim(),
    "",
    HOUSE_RULES,
    "",
    "Output only the Python source. No explanation, no markdown fences.",
    "",
  ].join("\n");
}

/**
 * Round two, for the same conversation. Deliberately omits the contract:
 * the agent already has it in context, and re-pasting 600 lines pushes
 * the report -- the only new information -- further from the ask.
 */
export function composeFixPrompt(report: string): string {
  return [
    "Your strategy was rejected by the platform. The full report is below.",
    "",
    "Fix only what the report names. Leave everything else exactly as it is:",
    "do not refactor, rename, or improve code the report did not mention.",
    "Bump the version string in the manifest.",
    "",
    "Each finding cites a section of the contract you were given. Re-read the",
    "cited section before changing anything -- the finding names the rule, and",
    "the section says why it exists.",
    "",
    "Output only the corrected Python source. No explanation, no markdown fences.",
    "",
    "--- PLATFORM REPORT ---",
    report.trim(),
    "",
  ].join("\n");
}
