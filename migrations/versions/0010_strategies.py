"""Add `strategies`: the Agent Contract's registry (contract §9 stage 3).

A registered strategy is what a backtest report, a forward paper run, and
a future leaderboard row all point at. The schema's one opinionated
decision is that **`(user_id, name, version)` is unique and a registered
version is never updated in place.**

That is not tidiness, it is the same point-in-time discipline the data
layer keeps, applied to code. If `demo 1.0.0` could be overwritten, every
result already attributed to `demo 1.0.0` would silently describe source
that no longer exists -- an equity curve nobody can reproduce, which is
precisely the class of quiet lie this project's correctness doctrine
exists to prevent. Publishing a change means publishing a new version.

`source_sha256` is stored alongside so a result can assert it ran the
exact bytes it claims, and so a re-upload of identical source can be
recognised as the retry it usually is rather than a conflict.

`contract_version` records which revision of `STRATEGY_CONTRACT.md` the
source was validated against. The contract is a draft and will change; a
strategy accepted under v0.1 is not automatically valid under v1.0, and
storing the version is cheaper than inferring it later from a date.

Revision ID: 0010
Revises: 0009
Create Date: 2026-09-03
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "0010"
down_revision: str | None = "0009"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "strategies",
        sa.Column("strategy_id", sa.BigInteger, primary_key=True),
        # user_id on everything from the start: multi-tenancy stays in the
        # schema even while V1 serves one user (plan §12 Q1), because the
        # column costs nothing today and saves a migration later.
        sa.Column("user_id", sa.BigInteger, sa.ForeignKey("users.user_id"), nullable=False),
        sa.Column("name", sa.Text, nullable=False),
        sa.Column("version", sa.Text, nullable=False),
        sa.Column("source", sa.Text, nullable=False),
        sa.Column("source_sha256", sa.Text, nullable=False),
        # The manifest as returned by configure(). Nullable because
        # producing it means *running* configure(), which needs the sandbox
        # (§9 stage 2) -- a strategy can be registered before that exists.
        sa.Column("manifest", postgresql.JSONB, nullable=True),
        sa.Column("status", sa.Text, nullable=False, server_default="REGISTERED"),
        sa.Column("contract_version", sa.Text, nullable=False),
        sa.Column(
            "registered_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.UniqueConstraint("user_id", "name", "version", name="uq_strategy_version"),
        sa.CheckConstraint(
            "status IN ('REGISTERED', 'SMOKE_PASSED', 'RETIRED')",
            name="ck_strategy_status",
        ),
        sa.CheckConstraint("length(source) > 0", name="ck_strategy_source_not_empty"),
    )
    op.create_index("ix_strategies_user", "strategies", ["user_id", "registered_at"])


def downgrade() -> None:
    op.drop_index("ix_strategies_user", table_name="strategies")
    op.drop_table("strategies")
