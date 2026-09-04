from datetime import UTC, datetime
from decimal import Decimal

import pytest

from trading.paper.enums import ChargeBasis, ChargeType, Product, Rounding
from trading.paper.models import ChargeSchedule
from trading.runtime.payload import SmokePayload, decode_payload, encode_payload
from trading.runtime.provider import BarRecord


def _schedule() -> ChargeSchedule:
    return ChargeSchedule(
        broker="ZERODHA",
        exchange="NSE",
        asset_class="EQUITY",
        product=Product.DELIVERY,
        charge_type=ChargeType.BROKERAGE,
        basis=ChargeBasis.FLAT_PER_ORDER,
        applies_to_side="BOTH",
        rate=Decimal("0"),
        cap=None,
        rounding=Rounding.TWO_DECIMALS,
        gst_base_types=(),
        effective_from=datetime(2020, 1, 1).date(),
        effective_to=None,
        source_note="test",
    )


def _payload() -> SmokePayload:
    return SmokePayload(
        mode="smoke",
        source="class S: pass\n",
        window={
            "start": "2026-08-27T00:00:00+00:00",
            "end": "2026-09-02T00:00:00+00:00",
            "sessions": 5,
        },
        bars={
            1401: (
                BarRecord(
                    instrument_id=1401,
                    ts=datetime(2026, 9, 1, 9, 15, tzinfo=UTC),
                    interval_sec=60,
                    open=Decimal("1300.05"),
                    high=Decimal("1301.00"),
                    low=Decimal("1299.50"),
                    close=Decimal("1300.75"),
                    volume=Decimal("1234"),
                ),
            )
        },
        charge_schedules=(_schedule(),),
        starting_cash=Decimal("1000000.00"),
        slippage_bps=Decimal("5"),
    )


def test_round_trip_preserves_every_field_exactly() -> None:
    restored = decode_payload(encode_payload(_payload()))
    original = _payload()
    assert restored.mode == original.mode
    assert restored.source == original.source
    assert restored.window == original.window
    assert restored.starting_cash == original.starting_cash
    assert restored.slippage_bps == original.slippage_bps
    assert restored.charge_schedules == original.charge_schedules
    assert restored.bars == original.bars


def test_money_survives_as_decimal_not_float() -> None:
    # The whole cost model depends on this. A payload that quietly
    # round-trips prices through binary floating point would corrupt
    # every charge computed in the container.
    restored = decode_payload(encode_payload(_payload()))
    bar = restored.bars[1401][0]
    assert isinstance(bar.close, Decimal)
    assert bar.close == Decimal("1300.75")
    assert str(bar.close) == "1300.75"


def test_encoding_is_compressed() -> None:
    # D-S7 feeds an uncapped universe, so the payload must compress.
    raw = encode_payload(_payload())
    assert raw[:2] == b"\x1f\x8b"  # gzip magic


def test_columnar_encoding_beats_per_object_for_a_realistic_series() -> None:
    bars = tuple(
        BarRecord(
            instrument_id=1401,
            ts=datetime(2026, 9, 1, 9, 15, tzinfo=UTC),
            interval_sec=60,
            open=Decimal("1300.05"),
            high=Decimal("1301.00"),
            low=Decimal("1299.50"),
            close=Decimal("1300.75"),
            volume=Decimal("1234"),
        )
        for _ in range(2000)
    )
    payload = SmokePayload(mode="smoke", source="x", bars={1401: bars})
    # 2000 bars must fit comfortably; a per-object JSON encoding would not.
    assert len(encode_payload(payload)) < 200_000


def test_decode_rejects_a_payload_without_a_mode() -> None:
    import gzip
    import json

    raw = gzip.compress(json.dumps({"source": "x"}).encode())
    with pytest.raises(ValueError, match="mode"):
        decode_payload(raw)


def test_the_payload_default_matches_the_registry_contract_version() -> None:
    # payload.py cannot import the constant -- registry.py imports psycopg
    # and trading.runtime ships into a container with no database -- so the
    # literal is duplicated deliberately. This test is what keeps the
    # duplicate honest; it runs on the host, where psycopg exists.
    from trading.agent_contract.registry import CONTRACT_VERSION

    assert SmokePayload(mode="smoke", source="x").contract_version == CONTRACT_VERSION


def test_knowable_at_survives_the_payload_round_trip() -> None:
    """The strategy runs INSIDE the container, rebuilt by `decode_payload`.

    A `knowable_at` that the codec drops leaves the container deriving
    `ts + 86400` again -- so the daily-clock fix would pass every host-side
    test while being absent from the only process that computes charges and
    dispatches `on_bar`. That is the failure mode this test exists for, and
    it is invisible to any test that does not cross the wire.
    """
    ts = datetime(2026, 3, 2, 10, 0, tzinfo=UTC)
    daily = BarRecord(
        instrument_id=1,
        ts=ts,
        interval_sec=86400,
        open=Decimal("100"),
        high=Decimal("101"),
        low=Decimal("99"),
        close=Decimal("100.5"),
        knowable_at=ts,
    )
    intraday = BarRecord(
        instrument_id=2,
        ts=ts,
        interval_sec=60,
        open=Decimal("10"),
        high=Decimal("10"),
        low=Decimal("10"),
        close=Decimal("10"),
    )

    decoded = decode_payload(
        encode_payload(SmokePayload(mode="smoke", source="x", bars={1: (daily,), 2: (intraday,)}))
    )

    assert decoded.bars[1][0].knowable_at == ts
    assert decoded.bars[1][0].close_ts == ts
    # An intraday bar sets nothing and keeps the arithmetic it always had.
    assert decoded.bars[2][0].knowable_at is None
    assert decoded.bars[2][0].close_ts == datetime(2026, 3, 2, 10, 1, tzinfo=UTC)


def test_dispatch_from_survives_the_payload_round_trip() -> None:
    """Warm-up is decided on the host and executed in the container, so
    `dispatch_from` has to cross the envelope like the bars do.

    Third boundary this sub-project crosses, and the second that is
    hand-written: `asdict` carries new RunOutcome fields OUT of the container
    for free, but everything going IN is encoded field by field. A
    `dispatch_from` that stops at the host leaves the container dispatching
    every warm-up bar as a real event -- the run silently starts earlier than
    the caller asked, with no error anywhere.
    """
    ts = datetime(2026, 3, 5, 10, 0, tzinfo=UTC)
    decoded = decode_payload(
        encode_payload(SmokePayload(mode="smoke", source="x", dispatch_from=ts))
    )
    assert decoded.dispatch_from == ts


def test_dispatch_from_defaults_to_none_for_a_payload_that_omits_it() -> None:
    """Warm-up is opt-in: an envelope without it must dispatch everything,
    exactly as every payload did before this field existed."""
    decoded = decode_payload(encode_payload(SmokePayload(mode="smoke", source="x")))
    assert decoded.dispatch_from is None
