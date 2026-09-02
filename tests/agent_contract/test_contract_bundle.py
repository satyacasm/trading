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
