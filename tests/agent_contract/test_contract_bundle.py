"""The Agent Contract bundle's two mechanical promises.

`STRATEGY_CONTRACT.md` claims `schema.json` lets an agent check its output
for conformance, and that `platform_sdk.py` lets generated code be
lint-checked and dry-run before upload. Both claims are only worth making
if something enforces them, because the bundle's whole value is that an
agent with no other context can trust it.

These tests are that enforcement. They are deliberately about *agreement*
-- between the schema and the SDK, and between both and the enums the
platform actually runs on -- rather than about either artifact in
isolation. A schema that drifts from `trading.paper.enums` would accept a
strategy the order API then rejects, which is the worst outcome available:
a contract that lies.
"""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator
from jsonschema.exceptions import ValidationError

from trading.agent_contract import platform_sdk as sdk
from trading.agent_contract.schemas import load_schemas
from trading.paper.enums import OrderStatus, OrderType, Product, Side, TimeInForce

SCHEMA_PATH = Path(sdk.__file__).parent / "schema.json"


@pytest.fixture(scope="module")
def schemas() -> dict:
    return load_schemas()


def _definition(schemas: dict, name: str) -> dict:
    return schemas["$defs"][name]


def _validator_for(schemas: dict, name: str) -> Draft202012Validator:
    """A validator for one definition that can still resolve `$ref`s.

    Validating `schemas["$defs"][name]` directly fails on any internal
    reference: the extracted sub-schema has no `$defs` of its own for
    `#/$defs/InstrumentRef` to point at. Wrapping it as a `$ref` against a
    document that still carries `$defs` keeps resolution working.
    """
    return Draft202012Validator({"$ref": f"#/$defs/{name}", "$defs": schemas["$defs"]})


# --- schema.json is a valid, self-consistent JSON Schema ----------------------


def test_schema_file_is_valid_json_schema(schemas: dict) -> None:
    """A malformed schema would fail open: agents would get no validation
    and never learn their output was unchecked."""
    Draft202012Validator.check_schema(schemas)


def test_schema_declares_every_documented_definition(schemas: dict) -> None:
    expected = {"StrategyManifest", "Instrument", "Bar", "Tick", "Order", "Position"}
    assert expected <= set(schemas["$defs"])


# --- the schema's enums match the enums the platform actually enforces --------
#
# These are the tests that matter most. An agent that reads the schema and
# emits `"order_type": "STOP"` should be told so by the validator, not by a
# 422 after upload -- and the only way that stays true is if the schema's
# enumerations are checked against the runtime's.


@pytest.mark.parametrize(
    ("definition", "field", "enum_cls"),
    [
        ("Order", "side", Side),
        ("Order", "order_type", OrderType),
        ("Order", "status", OrderStatus),
        ("Order", "product", Product),
        ("Order", "time_in_force", TimeInForce),
    ],
)
def test_schema_enum_matches_the_runtime_enum(
    schemas: dict, definition: str, field: str, enum_cls: type
) -> None:
    documented = set(_definition(schemas, definition)["properties"][field]["enum"])
    actual = {member.value for member in enum_cls}
    assert documented == actual, (
        f"{definition}.{field} in schema.json has drifted from "
        f"trading.paper.enums.{enum_cls.__name__}"
    )


def test_a_conforming_manifest_validates(schemas: dict) -> None:
    manifest = {
        "name": "sma-crossover",
        "version": "1.0.0",
        "universe": [{"exchange": "NSE", "segment": "CM", "symbol": "RELIANCE"}],
        "data": {"bars": "1m", "ticks": False, "history_bars": 200},
        "capital": "1000000",
        "base_currency": "INR",
        "params": {"fast": {"type": "int", "default": 10, "bounds": [2, 100]}},
    }
    validator = _validator_for(schemas, "StrategyManifest")
    validator.validate(manifest)


def test_a_manifest_missing_its_universe_is_rejected(schemas: dict) -> None:
    manifest = {
        "name": "no-universe",
        "version": "1.0.0",
        "data": {"bars": "1m"},
        "capital": "1000000",
        "base_currency": "INR",
    }
    validator = _validator_for(schemas, "StrategyManifest")
    with pytest.raises(ValidationError):
        validator.validate(manifest)


def test_a_manifest_with_an_unsupported_bar_interval_is_rejected(schemas: dict) -> None:
    """The platform stores 1m/5m/15m/1h/1d. An agent asking for "30s" must
    be told by the validator, not by an empty result set at run time."""
    manifest = {
        "name": "too-fast",
        "version": "1.0.0",
        "universe": [{"exchange": "NSE", "segment": "CM", "symbol": "RELIANCE"}],
        "data": {"bars": "30s"},
        "capital": "1000000",
        "base_currency": "INR",
    }
    validator = _validator_for(schemas, "StrategyManifest")
    with pytest.raises(ValidationError):
        validator.validate(manifest)


# --- platform_sdk.py offers the interface the contract documents --------------


def test_sdk_exposes_every_documented_lifecycle_method() -> None:
    for method in (
        "configure",
        "initialize",
        "on_bar",
        "on_tick",
        "on_order_update",
        "on_expiry",
    ):
        assert hasattr(sdk.Strategy, method), f"Strategy.{method} is documented but missing"


def test_sdk_context_exposes_every_documented_member() -> None:
    ctx = sdk.Context()
    # Checked on the class, not the instance: `now` is a property that
    # raises NotOnThisPlatform by design, and `hasattr` only swallows
    # AttributeError -- so an instance check would propagate that raise
    # instead of reporting presence.
    for member in ("now", "data", "portfolio", "order", "cancel", "log", "state"):
        assert hasattr(type(ctx), member) or hasattr(ctx, member), (
            f"Context.{member} is documented but missing"
        )


def test_sdk_stubs_raise_rather_than_returning_a_plausible_answer() -> None:
    """The SDK is a *stub*: it exists so generated code type-checks and
    imports locally, not so it can be run for results. A stub that quietly
    returned an empty list or a zero would let an agent's local dry run
    look like a passing backtest, which is a worse failure than an
    import error."""
    ctx = sdk.Context()
    with pytest.raises(sdk.NotOnThisPlatform):
        ctx.data.bars(1, interval="1m", count=10)
    with pytest.raises(sdk.NotOnThisPlatform):
        ctx.order(1, side="BUY", quantity=Decimal("1"), rationale="x")
    with pytest.raises(sdk.NotOnThisPlatform):
        _ = ctx.portfolio.cash


def test_sdk_order_rejects_an_empty_rationale_without_reaching_the_platform() -> None:
    """§8's journal rule is a contract requirement, so the stub enforces it
    -- an agent's local dry run should surface a blank rationale, not
    discover it as a 422 after upload."""
    ctx = sdk.Context()
    with pytest.raises(ValueError, match="rationale"):
        ctx.order(1, side="BUY", quantity=Decimal("1"), rationale="   ")


def test_sdk_order_rejects_float_money() -> None:
    """Money is Decimal end to end. A float quantity is the one mistake an
    agent is most likely to make, and the cheapest place to catch it is
    before upload."""
    ctx = sdk.Context()
    with pytest.raises(TypeError, match="Decimal"):
        ctx.order(1, side="BUY", quantity=1.5, rationale="float money")  # type: ignore[arg-type]


def test_sdk_forbids_reading_the_wall_clock() -> None:
    """Determinism rule: the only time is ctx.now. The stub makes the
    documented failure visible locally."""
    ctx = sdk.Context()
    with pytest.raises(sdk.NotOnThisPlatform):
        _ = ctx.now


# --- §2's example must be code, not prose -------------------------------------


def _section_2_example() -> str:
    """The first Python block under §2 of the contract."""
    from pathlib import Path

    import trading.agent_contract as pkg

    text = (
        Path(pkg.__file__).resolve().parents[3] / "docs" / "agent-contract" / "STRATEGY_CONTRACT.md"
    ).read_text()
    section = text.split("## 2. The strategy interface", 1)[1].split("\n## ", 1)[0]
    return section.split("```python", 1)[1].split("```", 1)[0].strip() + "\n"


def test_the_contract_example_passes_the_validator_it_documents() -> None:
    """Found by dogfooding, and the reason this test exists at all.

    §2 said "a class named `Strategy`" and showed `class Strategy:`. Stage
    1 agreed with the document; `sandbox/runner.py` did not, and refused
    that exact shape. A cold agent given only the contract wrote what it
    was told and was rejected three containers later.

    Nothing caught it because every test in this repo was written by
    someone who already knew the real rule and wrote `class
    MyStrategy(Strategy)`. So: the contract's own example is now run
    through the real validator, and the document cannot drift from the
    code again without this failing.
    """
    from trading.agent_contract.validation import validate_strategy

    report = validate_strategy(_section_2_example())

    assert report.ok, report.as_agent_feedback()


def test_the_contract_example_imports_every_name_it_uses() -> None:
    """The example annotates with Context, Bar, Tick and friends. Python
    evaluates annotations at class-definition time, so a name used and not
    imported is a NameError the moment the runner imports the module --
    which an agent copying the example would inherit."""
    import ast

    tree = ast.parse(_section_2_example())
    imported = {
        alias.asname or alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom)
        for alias in node.names
    }
    used = {
        node.id for node in ast.walk(tree) if isinstance(node, ast.Name) and node.id[:1].isupper()
    }
    bases = {
        b.id
        for n in ast.walk(tree)
        if isinstance(n, ast.ClassDef)
        for b in n.bases
        if isinstance(b, ast.Name)
    }

    assert (used | bases) - imported == set(), f"used but not imported: {(used | bases) - imported}"


def _documented_fields(section: str) -> set[str]:
    """Field names from a `| \\`name\\` | type | notes |` table under a §5 heading."""
    import re
    from pathlib import Path

    import trading.agent_contract as pkg

    text = (
        Path(pkg.__file__).resolve().parents[3] / "docs" / "agent-contract" / "STRATEGY_CONTRACT.md"
    ).read_text()
    body = text.split(f"### {section}", 1)[1].split("\n### ", 1)[0]
    names: set[str] = set()
    for line in body.splitlines():
        match = re.match(r"\|\s*`([^`]+)`\s*\|", line)
        if match:
            names.update(part.strip().strip("`") for part in match.group(1).split(","))
    return names


def test_the_documented_OrderUpdate_matches_the_object_strategies_receive() -> None:
    """Second defect found by dogfooding. A cold agent implemented
    `on_order_update` reading `update.order_id`, `update.status` and
    `update.filled_quantity`, and crashed: the runtime hands over a
    wrapper whose only fields are `order` and `previous_status`.

    It was not the agent's mistake. `OrderUpdate` appeared in the contract
    exactly twice -- in an import list and in a method signature -- and §5
    never described it, while §2's prose named `filled_quantity` without
    saying it lives one level down. The one type you must destructure to
    implement the handler was the one type the data model omitted.

    Pins all three together: the table, the SDK stub agents type-check
    against, and the object the loop actually constructs.
    """
    from trading.agent_contract.platform_sdk import OrderUpdate
    from trading.runtime.loop import _Update

    documented = _documented_fields("OrderUpdate")
    assert documented == {"order", "previous_status"}
    assert set(OrderUpdate.__annotations__) == documented

    delivered = _Update(order="sentinel", previous_status="OPEN")  # type: ignore[arg-type]
    assert set(vars(delivered)) == documented


def _section_body(section: str) -> str:
    """The raw prose under a `### <section>` heading, up to the next one."""
    import trading.agent_contract as pkg

    text = (
        Path(pkg.__file__).resolve().parents[3] / "docs" / "agent-contract" / "STRATEGY_CONTRACT.md"
    ).read_text()
    return text.split(f"### {section}", 1)[1].split("\n### ", 1)[0]


def test_the_documented_meaning_of_ts_matches_what_a_daily_run_actually_does() -> None:
    """Third defect of the shape dogfooding keeps surfacing: prose that is
    internally coherent and disagrees with the code.

    §5 documented `ts` as the START of the interval, unqualified. For `1d`
    that is false -- `bars_daily` stamps the row AT the session close (all
    51,081,227 rows are 10:00 UTC / 15:30 IST) and the runtime deliberately
    treats it that way, because an NSE session is 6h15m of market time
    inside a 24-hour calendar interval and no arithmetic on `ts` and
    `interval_sec` can produce the close.

    A reader following the unqualified rule would place every daily bar
    6h15m early and mis-time every decision in a daily strategy. The prose
    is corrected to match the runtime, and this test holds the two together
    so they cannot drift apart silently again.
    """
    from datetime import UTC, datetime

    from trading.runtime.provider import BarRecord

    body = _section_body("Bar")
    assert "session close" in body.lower(), "§5 must state what ts means for a daily bar"
    assert "1d" in body or "86400" in body, "§5 must say which interval the exception applies to"

    # The runtime half: a daily bar built the way `_fetch_daily_bars` builds
    # one reports the session close as its clock, not the next day's.
    session_close = datetime(2026, 3, 2, 10, 0, tzinfo=UTC)
    daily = BarRecord(
        instrument_id=1,
        ts=session_close,
        interval_sec=86400,
        open=Decimal("1"),
        high=Decimal("1"),
        low=Decimal("1"),
        close=Decimal("1"),
        knowable_at=session_close,
    )
    assert daily.close_ts == session_close

    # And the rule the document still states for every other interval.
    intraday = BarRecord(
        instrument_id=1,
        ts=session_close,
        interval_sec=60,
        open=Decimal("1"),
        high=Decimal("1"),
        low=Decimal("1"),
        close=Decimal("1"),
    )
    assert intraday.close_ts == datetime(2026, 3, 2, 10, 1, tzinfo=UTC)
