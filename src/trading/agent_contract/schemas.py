"""Access to the Agent Contract's JSON Schemas.

`schema.json` sits beside this module rather than under `docs/` for one
reason: its enumerations must not drift from the enums the order API
actually enforces. Keeping it inside the package puts it under the same
lint/type gate as the code, and lets
`tests/agent_contract/test_contract_bundle.py` assert field-by-field that
what the schema advertises is what the platform accepts. A schema that
drifts would validate a strategy the API then rejects -- a contract that
lies, which is worse than no contract.

The shipped bundle is assembled from three files:
`docs/agent-contract/STRATEGY_CONTRACT.md`, this package's `schema.json`,
and `platform_sdk.py`.
"""

from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Any

SCHEMA_PATH = Path(__file__).parent / "schema.json"


@lru_cache(maxsize=1)
def load_schemas() -> dict[str, Any]:
    """The full schema document, with every shape under `$defs`."""
    with SCHEMA_PATH.open(encoding="utf-8") as handle:
        loaded: dict[str, Any] = json.load(handle)
    return loaded


def definition(name: str) -> dict[str, Any]:
    """One named schema, e.g. `definition("StrategyManifest")`."""
    defs = load_schemas()["$defs"]
    if name not in defs:
        raise KeyError(f"no schema named {name!r}; available: {sorted(defs)}")
    result: dict[str, Any] = defs[name]
    return result
