"""Shared fixtures for the upload-pipeline tests.

`registered_strategy_id` exists because a smoke run is meaningless
without something to hang it on: `strategy_smoke_runs.strategy_id` is a
FK with ON DELETE CASCADE, so a run cannot be recorded against a
strategy that was never registered. The source below is the same one
`test_registry.py` uses, so the two files cannot drift into disagreeing
about what stage 1 accepts.
"""

from __future__ import annotations

import textwrap

import pytest
from psycopg import Connection

from trading.agent_contract.registry import register_strategy

VALID_SOURCE = (
    textwrap.dedent(
        """
        from decimal import Decimal


        class MyStrategy(Strategy):
            def configure(self):
                return None

            def on_bar(self, ctx, bars):
                ctx.order(1, side="BUY", quantity=Decimal("1"), rationale="demo")
        """
    ).strip()
    + "\n"
)


@pytest.fixture
def local_user_id(db_conn: Connection) -> int:
    row = db_conn.execute("SELECT user_id FROM users WHERE email='local@paper.trading'").fetchone()
    assert row is not None, "migration 0007 seeds the local user"
    return int(row[0])


@pytest.fixture
def registered_strategy_id(db_conn: Connection, local_user_id: int) -> int:
    registered = register_strategy(
        db_conn,
        user_id=local_user_id,
        name="smoke-persistence-fixture",
        version="1.0.0",
        source=VALID_SOURCE,
    )
    return registered.strategy_id
