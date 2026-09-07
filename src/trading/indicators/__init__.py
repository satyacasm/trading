"""Technical indicators over bar series.

`Decimal` throughout, and shared rather than private to the MCP layer on
purpose: a strategy script and the live agent must compute RSI with the
same code. If they diverged, every lesson carried from a backtest into a
live decision would be measuring a subtly different thing, invisibly in
both places.
"""

from __future__ import annotations
