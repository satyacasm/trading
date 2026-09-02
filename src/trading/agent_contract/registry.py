"""Registration -- stage 3 of the upload pipeline (contract §9).

Stores a validated strategy so a backtest, a forward paper run, or a
report can refer to the exact code that produced a result.

Two rules carry the weight here.

**Nothing that fails static validation is stored.** `register_strategy`
runs stage 1 first and raises rather than writing. A registry holding
strategies that cannot run is worse than an empty one: every consumer
downstream would have to re-validate, and the one that forgets ships a
broken run.

**A registered version is immutable.** Re-registering `demo 1.0.0` with
different source is a `VersionConflict`, not an update. If a version could
be overwritten, every result already attributed to it would silently
describe source that no longer exists -- an equity curve nobody can
reproduce. It is the same point-in-time discipline the data layer keeps,
applied to code. Re-registering *identical* source is idempotent, because
that is a retried upload rather than a change.

Following this codebase's convention, nothing here commits: the caller
owns the transaction boundary, so a registration and whatever else belongs
with it land as one unit.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from psycopg import Connection

from trading.agent_contract.validation import ValidationReport, validate_strategy

__all__ = [
    "CONTRACT_VERSION",
    "RegisteredStrategy",
    "StrategyRejected",
    "VersionConflict",
    "get_strategy",
    "list_strategies",
    "register_strategy",
]

# The revision of STRATEGY_CONTRACT.md this module validates against. Bump
# it with the contract, so a strategy's row records which rules it was
# actually accepted under -- a strategy passing under v0.1 is not
# automatically valid under v1.0.
CONTRACT_VERSION = "0.1"

_STATUS_REGISTERED = "REGISTERED"

_COLUMNS = (
    "strategy_id, user_id, name, version, source, source_sha256,"
    " manifest, status, contract_version, registered_at"
)


class StrategyRejected(Exception):
    """The source failed static validation, so nothing was stored.

    Carries the `ValidationReport` so a caller can hand the agent-facing
    feedback straight back without re-running validation -- §9's loop is
    only closed if the rejection travels with the reason.
    """

    def __init__(self, report: ValidationReport) -> None:
        super().__init__(
            f"strategy failed static validation with {len(report.findings)} finding(s); "
            "nothing was registered"
        )
        self.report = report


class VersionConflict(Exception):
    """This `(name, version)` is already registered with different source.

    Raised rather than updating: see the module docstring on immutability.
    """


@dataclass(frozen=True)
class RegisteredStrategy:
    strategy_id: int
    user_id: int
    name: str
    version: str
    source: str
    source_sha256: str
    manifest: dict[str, Any] | None
    status: str
    contract_version: str
    registered_at: datetime


def _row_to_record(row: tuple[Any, ...]) -> RegisteredStrategy:
    return RegisteredStrategy(
        strategy_id=row[0],
        user_id=row[1],
        name=row[2],
        version=row[3],
        source=row[4],
        source_sha256=row[5],
        manifest=row[6],
        status=row[7],
        contract_version=row[8],
        registered_at=row[9],
    )


def source_digest(source: str) -> str:
    """Content address for the source, so a result can assert it ran the
    exact bytes it names."""
    return hashlib.sha256(source.encode("utf-8")).hexdigest()


def register_strategy(
    conn: Connection,
    *,
    user_id: int,
    name: str,
    version: str,
    source: str,
    manifest: dict[str, Any] | None = None,
) -> RegisteredStrategy:
    """Validate, then store. Does not commit.

    Raises `StrategyRejected` if stage 1 finds anything, and
    `VersionConflict` if this version already exists with different source.
    An identical re-registration returns the existing record.
    """
    report = validate_strategy(source, manifest)
    if not report.ok:
        raise StrategyRejected(report)

    digest = source_digest(source)

    existing = conn.execute(
        f"SELECT {_COLUMNS} FROM strategies WHERE user_id = %s AND name = %s AND version = %s",
        (user_id, name, version),
    ).fetchone()
    if existing is not None:
        record = _row_to_record(existing)
        if record.source_sha256 == digest:
            # A retried upload, not a change. Same answer as the order
            # API gives a repeated idempotency key.
            return record
        raise VersionConflict(
            f"strategy {name!r} version {version!r} is already registered with different "
            f"source (stored sha256 {record.source_sha256[:12]}…, submitted "
            f"{digest[:12]}…). A registered version is immutable, because results already "
            "attributed to it must keep describing the code that produced them. "
            "Publish this change as a new version."
        )

    row = conn.execute(
        "INSERT INTO strategies"
        " (user_id, name, version, source, source_sha256, manifest, status, contract_version)"
        " VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
        f" RETURNING {_COLUMNS}",
        (
            user_id,
            name,
            version,
            source,
            digest,
            json.dumps(manifest) if manifest is not None else None,
            _STATUS_REGISTERED,
            CONTRACT_VERSION,
        ),
    ).fetchone()
    assert row is not None
    return _row_to_record(row)


def get_strategy(conn: Connection, strategy_id: int) -> RegisteredStrategy:
    """One registered strategy. Raises `KeyError` if it does not exist --
    a missing strategy is a caller bug, not an empty result."""
    row = conn.execute(
        f"SELECT {_COLUMNS} FROM strategies WHERE strategy_id = %s", (strategy_id,)
    ).fetchone()
    if row is None:
        raise KeyError(f"no strategy with strategy_id={strategy_id}")
    return _row_to_record(row)


def list_strategies(conn: Connection, *, user_id: int, limit: int = 50) -> list[RegisteredStrategy]:
    """This user's strategies, newest first."""
    rows = conn.execute(
        f"SELECT {_COLUMNS} FROM strategies WHERE user_id = %s ORDER BY strategy_id DESC LIMIT %s",
        (user_id, limit),
    ).fetchall()
    return [_row_to_record(row) for row in rows]
