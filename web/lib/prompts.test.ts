import { describe, expect, it } from "vitest";
import {
  FIX_VARIATION_ID,
  INSTRUMENTS_WITH_BARS,
  VARIATIONS,
  composeFixPrompt,
  CUSTOM_VARIATION_ID,
  composePrompt,
  customVariation,
} from "./prompts";

const CONTRACT = "# Strategy Contract\n\nSection 1: everything.\n";

function variation(id: string) {
  const found = VARIATIONS.find((v) => v.id === id);
  if (found === undefined) throw new Error(`no variation ${id}`);
  return found;
}

describe("composePrompt", () => {
  it("puts the contract before the ask", () => {
    // With a 600-line specification, an instruction placed above it
    // competes with it for attention; one that follows it reads as the
    // thing to act on.
    const prompt = composePrompt(CONTRACT, "0.1", variation("mean-reversion"));
    expect(prompt.indexOf("Section 1: everything.")).toBeLessThan(
      prompt.indexOf("Write a mean-reversion strategy"),
    );
  });

  it("carries the whole contract, not a summary of it", () => {
    const prompt = composePrompt(CONTRACT, "0.1", variation("buy-and-hold"));
    expect(prompt).toContain(CONTRACT.trimEnd());
  });

  it("names the contract version it was built from", () => {
    // A prompt that cannot say which contract it encodes is one you
    // cannot later explain a rejection against.
    expect(composePrompt(CONTRACT, "0.2", variation("buy-and-hold"))).toContain(
      "contract version 0.2",
    );
  });

  it("tells the agent which instruments actually have bars", () => {
    // Otherwise NO_DATA arrives only after three containers have run.
    const prompt = composePrompt(CONTRACT, "0.1", variation("sma-crossover"));
    for (const symbol of INSTRUMENTS_WITH_BARS) {
      expect(prompt).toContain(symbol);
    }
  });

  it("forbids markdown fences, since the source is posted verbatim", () => {
    const prompt = composePrompt(CONTRACT, "0.1", variation("buy-and-hold"));
    expect(prompt).toContain("no markdown fences");
  });

  it("gives every non-follow-up variation a real task", () => {
    // Both of these get their task at runtime -- one from the rejection
    // report, one from the operator's own words -- so a baked-in task is
    // exactly what they must not have.
    const runtimeTasks = new Set([FIX_VARIATION_ID, CUSTOM_VARIATION_ID]);
    for (const v of VARIATIONS.filter((v) => !runtimeTasks.has(v.id))) {
      expect(v.task.trim().length, `${v.id} has no task`).toBeGreaterThan(80);
      expect(composePrompt(CONTRACT, "0.1", v)).toContain(v.task.trim());
    }
  });
});

describe("composeFixPrompt", () => {
  const REPORT = "REJECTED: 1 problem.\n\n  [WALL_CLOCK] line 7: calls datetime.now().";

  it("embeds the report it was given", () => {
    expect(composeFixPrompt(REPORT)).toContain("[WALL_CLOCK] line 7");
  });

  it("omits the contract, which the agent already has in context", () => {
    // Re-pasting 600 lines would push the report -- the only new
    // information in this turn -- further from the ask.
    expect(composeFixPrompt(REPORT)).not.toContain("Section 1: everything.");
  });

  it("asks for a patch rather than a rewrite", () => {
    const prompt = composeFixPrompt(REPORT);
    expect(prompt).toContain("Fix only what the report names");
    expect(prompt).toContain("Bump the version");
  });
});

describe("the custom variation", () => {
  it("carries the words the user typed, not a canned task", () => {
    const prompt = composePrompt(
      CONTRACT,
      "0.1",
      customVariation("Buy RELIANCE when RSI drops below 30 and sell above 70."),
    );
    expect(prompt).toContain("Buy RELIANCE when RSI drops below 30");
  });

  it("keeps the house rules, because a typed idea needs them most", () => {
    // The presets were written against the contract's constraints. Something
    // typed in a hurry was not, so it is the case where restating Decimal
    // money and ctx.now earns its space rather than the case to drop them.
    const prompt = composePrompt(CONTRACT, "0.1", customVariation("Do something clever."));
    expect(prompt).toContain("decimal.Decimal");
    expect(prompt).toContain("ctx.now");
  });

  it("is still a complete prompt when the box is empty", () => {
    // An empty box must not produce a prompt that ends mid-sentence and
    // silently asks an agent for nothing.
    const prompt = composePrompt(CONTRACT, "0.1", customVariation("   "));
    expect(prompt).toContain("END OF CONTRACT");
    expect(prompt).toContain("Ask them what they want");
  });

  it("is listed so the picker can show it", () => {
    expect(VARIATIONS.some((v) => v.id === CUSTOM_VARIATION_ID)).toBe(true);
  });
});
