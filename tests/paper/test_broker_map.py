"""One table of which broker prices which asset class.

There were three copies of this map, in `paper.api`, `paper.engine` and
`agent_contract.smoke`. Adding perpetuals to two of them let an order be
accepted by the gateway and then rejected by the engine at fill time with
"no charge schedule covers this fill" -- a rejection that describes the
symptom perfectly and names nothing that would lead you to the cause.
"""

from __future__ import annotations


def test_every_module_reads_the_same_broker_map() -> None:
    from trading.agent_contract import smoke
    from trading.paper import api, charges, engine

    assert api._BROKER_BY_ASSET_CLASS is charges.BROKER_BY_ASSET_CLASS
    assert engine._BROKER_BY_ASSET_CLASS is charges.BROKER_BY_ASSET_CLASS
    assert smoke._BROKER_FOR_ASSET_CLASS is charges.BROKER_BY_ASSET_CLASS


def test_every_tradable_asset_class_has_a_broker() -> None:
    """An asset class absent here has no charge schedule, and an order in
    it is refused at submission rather than filled at a cost of zero.
    That is the right behaviour -- but it must be a decision, not an
    omission."""
    from trading.paper.charges import BROKER_BY_ASSET_CLASS

    assert BROKER_BY_ASSET_CLASS == {
        "EQUITY": "UPSTOX",
        "CRYPTO": "BINANCE",
        "PERP": "BINANCE",
    }
