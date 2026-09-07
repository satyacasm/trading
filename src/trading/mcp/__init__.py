"""An MCP surface over the trading platform.

Every tool calls the gateway's HTTP routes. Nothing here opens a database
connection: the routes already enforce market hours, sufficient cash,
contract filters, the charge model and the breaker, and a second
enforcement path is a fork that an agent exploring the surface would
eventually find.

It also keeps blocking psycopg off an async event loop. The gateway's
routes are plain `def` because `async def` plus a blocking driver
deadlocked it once; MCP's handlers are async and get no threadpool.
"""

from __future__ import annotations
