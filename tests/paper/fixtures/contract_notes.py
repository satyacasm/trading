"""Ground-truth charge figures for the golden tests.

SOURCE: replace this line with either
  "Upstox contract note, order <id>, <date>"  (preferred), or
  "Upstox brokerage calculator, checked <date>"  (fallback oracle).

Acceptable sources, in order of preference:

1. A real Upstox contract note for an order that was actually executed.
   This is the only figure that reflects what was actually billed.
2. Upstox's own brokerage calculator, https://upstox.com/brokerage-calculator/
   -- enter the same quantity, price, and product and record its itemised
   output. This is weaker than a contract note: it is the broker's
   *published* model, not what was actually billed on a real order, so it
   can still miss a fee the calculator doesn't model. If used, say so in
   the source line above (e.g. "Upstox brokerage calculator, checked
   2026-09-01").

Third-party sources -- blog posts, forum threads, "worked example" articles
-- are NOT acceptable oracles here. One such widely-cited example (a
Zerodha delivery trade: buy 100 @ Rs.1,000, sell @ Rs.1,050, total
Rs.242.77) was checked against this calculator and disagrees by Rs.0.79,
most likely from an IPFT omission or a different DP/GST assumption in
that author's model. Grading our calculator against a stranger's stale
assumptions would just pressure us to "fix" a correct implementation to
match someone else's error.

Every value below is the broker's own figure, not ours. If our calculator
disagrees with these, our calculator is wrong.

The numbers currently here are PLACEHOLDERS derived from our own seeded
rate table (i.e. they were computed BY this codebase, not obtained from
a broker) and MUST be replaced with real broker output before the golden
tests are meaningful -- grading the calculator against its own arithmetic
proves nothing. They are kept only to show the expected shape of the
fixture (the field names each test checks).

Once real figures are filled in (and the `source` fields above updated
away from "REPLACE ME"), run the golden tests with:

    uv run pytest -m golden

They are excluded from the default `uv run pytest` run (see the `golden`
marker in pyproject.toml) precisely because, until then, they cannot pass
honestly.
"""

from decimal import Decimal

DELIVERY_BUY = {
    "source": "REPLACE ME",
    "quantity": Decimal("100"),
    "price": Decimal("1310.50"),
    "product": "DELIVERY",
    "side": "BUY",
    "expected": {
        "brokerage": Decimal("20.00"),
        "stt": Decimal("131"),
        "exchange_txn": Decimal("4.02"),
        "sebi_fee": Decimal("0.13"),
        "stamp_duty": Decimal("19.66"),
        "ipft": Decimal("0.00"),
        "gst": Decimal("4.32"),
        "dp_charges": Decimal("0.00"),
    },
}

DELIVERY_SELL = {
    "source": "REPLACE ME",
    "quantity": Decimal("100"),
    "price": Decimal("1350.00"),
    "product": "DELIVERY",
    "side": "SELL",
    "expected": {
        "brokerage": Decimal("20.00"),
        "stt": Decimal("135"),
        "exchange_txn": Decimal("4.14"),
        "sebi_fee": Decimal("0.14"),
        "stamp_duty": Decimal("0.00"),
        "ipft": Decimal("0.00"),
        "gst": Decimal("7.95"),
        "dp_charges": Decimal("20.00"),
    },
}
