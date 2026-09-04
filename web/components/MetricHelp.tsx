"use client";

import { useId, useState } from "react";
import { METRIC_HELP } from "@/lib/backtests";

/**
 * A small "i" that explains a metric and says which direction is good.
 *
 * Most of these numbers mean nothing without a finance background, and a
 * figure whose direction you cannot judge is trivia rather than
 * information. Each entry therefore says both what it measures and how to
 * read it.
 *
 * Opens on hover *and* on focus, and closes on Escape: hover-only would put
 * the explanation out of reach of anyone using a keyboard, which is exactly
 * the reader most likely to want it.
 */
export function MetricHelp({ metric }: { metric: keyof typeof METRIC_HELP | string }) {
  const help = METRIC_HELP[metric];
  const [open, setOpen] = useState(false);
  const id = useId();

  if (!help) return null;

  return (
    <span className="relative inline-flex">
      <button
        type="button"
        aria-label={`What is this metric?`}
        aria-describedby={open ? id : undefined}
        className="border-line text-muted hover:border-live hover:text-live ml-1 inline-flex h-3.5 w-3.5 items-center justify-center rounded-full border text-[9px] leading-none"
        onMouseEnter={() => setOpen(true)}
        onMouseLeave={() => setOpen(false)}
        onFocus={() => setOpen(true)}
        onBlur={() => setOpen(false)}
        onKeyDown={(event) => {
          if (event.key === "Escape") setOpen(false);
        }}
        onClick={() => setOpen((was) => !was)}
      >
        i
      </button>
      {open ? (
        <span
          id={id}
          role="tooltip"
          className="border-line bg-raised absolute bottom-full left-0 z-20 mb-2 w-72 rounded border p-3 text-xs leading-relaxed shadow-lg"
        >
          <span className="text-text block">{help.what}</span>
          <span className="text-muted mt-2 block">{help.good}</span>
        </span>
      ) : null}
    </span>
  );
}
