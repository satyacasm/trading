# Dogfood log — §10's acceptance bar

> *"The contract is not done until three different frontier agents, each given
> only this file, each produce a working strategy on the first try."*

Each round: a subagent with **no repo access** reads exactly one file —
`prompt-buy-and-hold.txt`, generated from the shipped `/strategies/contract`
endpoint — and returns source. It is submitted verbatim. Buy-and-hold is the
control: the simplest strategy the contract can express, so a failure implicates
the contract or the data, never the model.

| # | Date | Model | Verdict | Defect found |
|---|---|---|---|---|
| 1 | 2026-09-03 | Claude Opus | `MANIFEST_UNRESOLVABLE` | §2 said "a class **named** `Strategy`"; the runner skips that name and requires a subclass. Stage 1 encoded the document's rule, so both agreed with each other and disagreed with the runtime. Fixed in `2fbcfc5`. |
| 2 | 2026-09-03 | Claude Opus | `SMOKE_CRASH` | `OrderUpdate` appeared twice in 624 lines — an import list and a signature — and §5 never described it. The agent read `update.status`; the runtime has `update.order.status`. Fixed in `2daf103`. |
| 3 | 2026-09-03 | Claude Opus | **PASSED** | — 491 `on_bar` calls, 1 order, 1 fill, +118.99 INR over 5 sessions. |

**Status: 1 of 3 model families cleared.** Round 3 is the first strategy on this
platform written entirely from the contract by something that had never seen the
code. Two other frontier families still have to clear it before §10 is met, and
they must be given the *current* prompt — rounds 1 and 2 ran against a contract
that has since been corrected twice.

Both defects were the same shape: prose internally coherent and disagreeing with
the code, invisible to a suite whose every strategy was written by someone who
already knew the rule. Each is now pinned by a test that reads the **document**
and compares it to the **runtime** — see
`test_the_contract_example_passes_the_validator_it_documents` and
`test_the_documented_OrderUpdate_matches_the_object_strategies_receive`.

`passing-buy-and-hold.py` is round 3's output, unedited. It is the first
candidate for D4's worked examples, and unlike the four that were withheld, it
has actually been run.
