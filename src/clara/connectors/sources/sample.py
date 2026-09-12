"""Sample data source.

Generates a small deterministic retail dataset. Exists so that ``clara init``
produces a working lakehouse with real tables, real SQL and a real invoice in
under a minute — with no database, no cloud account and no credentials. It is
also what the test suite syncs, which keeps the ingest path covered without a
network.

Deterministic by seed: the same run produces the same rows, so tests can assert
on values.
"""

from __future__ import annotations

import random
from collections.abc import Iterator
from datetime import timedelta
from typing import Any

from clara.connectors.base import CheckResult, ConnectorSpec, Source
from clara.connectors.protocol import (
    AirbyteMessage,
    ConfiguredStream,
    StreamDescriptor,
    SyncMode,
)
from clara.time_utils import from_iso, to_iso, utcnow

_CUSTOMERS = [
    ("acme", "Acme Corp", "TH"),
    ("globex", "Globex", "SG"),
    ("initech", "Initech", "TH"),
    ("umbrella", "Umbrella", "MY"),
    ("stark", "Stark Industries", "VN"),
]
_PRODUCTS = [
    ("sku-100", "Jasmine rice 5kg", "grocery", 12.50),
    ("sku-200", "Cooking oil 1L", "grocery", 3.20),
    ("sku-300", "Detergent 2kg", "household", 8.75),
    ("sku-400", "Instant noodles x30", "grocery", 6.40),
    ("sku-500", "Dish soap 500ml", "household", 2.10),
]


class SampleSource(Source):
    """Emits ``customers`` and ``orders`` streams."""

    name = "sample"

    @classmethod
    def spec(cls) -> ConnectorSpec:
        return ConnectorSpec(
            name=cls.name,
            title="Sample retail data",
            supports_incremental=True,
            config_schema={
                "type": "object",
                "required": [],
                "properties": {
                    "orders": {"type": "integer", "title": "Number of orders", "default": 500},
                    "seed": {"type": "integer", "title": "Random seed", "default": 42},
                    "days": {"type": "integer", "title": "Days of history", "default": 30},
                },
            },
        )

    # ------------------------------------------------------------------ checks

    def check(self) -> CheckResult:
        return CheckResult(succeeded=True, message="sample source is always available", detected_streams=2)

    def discover(self) -> list[StreamDescriptor]:
        return [
            StreamDescriptor(
                name="customers",
                json_schema={
                    "type": "object",
                    "required": ["customer_id"],
                    "properties": {
                        "customer_id": {"type": "string"},
                        "name": {"type": "string"},
                        "country": {"type": "string"},
                    },
                },
                supported_sync_modes=[SyncMode.FULL_REFRESH],
                source_defined_primary_key=[["customer_id"]],
            ),
            StreamDescriptor(
                name="orders",
                json_schema={
                    "type": "object",
                    "required": ["order_id", "ordered_at"],
                    "properties": {
                        "order_id": {"type": "integer"},
                        "customer_id": {"type": "string"},
                        "sku": {"type": "string"},
                        "product": {"type": "string"},
                        "category": {"type": "string"},
                        "quantity": {"type": "integer"},
                        "unit_price": {"type": "number"},
                        "amount": {"type": "number"},
                        "ordered_at": {"type": "string", "format": "date-time"},
                    },
                },
                supported_sync_modes=[SyncMode.FULL_REFRESH, SyncMode.INCREMENTAL],
                default_cursor_field=["ordered_at"],
                source_defined_primary_key=[["order_id"]],
            ),
        ]

    # ------------------------------------------------------------------- read

    def read(
        self,
        streams: list[ConfiguredStream],
        state: dict[str, Any] | None = None,
    ) -> Iterator[AirbyteMessage]:
        state = dict(state or {})
        selected = {c.stream.name: c for c in streams} or {
            s.name: ConfiguredStream(stream=s) for s in self.discover()
        }

        if "customers" in selected:
            for customer_id, name, country in _CUSTOMERS:
                yield AirbyteMessage.record_message(
                    "customers",
                    {"customer_id": customer_id, "name": name, "country": country},
                )
            yield AirbyteMessage.state_message(dict(state), "customers")

        if "orders" in selected:
            configured = selected["orders"]
            incremental = configured.sync_mode is SyncMode.INCREMENTAL
            since = (state.get("orders") or {}).get("cursor") if incremental else None

            rng = random.Random(self.config.get("seed", 42))
            total = int(self.config.get("orders", 500))
            days = int(self.config.get("days", 30))
            # Anchored to midnight UTC so a given seed always produces the
            # same timestamps: reproducible demos and assertable tests.
            start = (utcnow() - timedelta(days=days)).replace(
                hour=0, minute=0, second=0, microsecond=0
            )

            # Compare parsed datetimes, never ISO strings. Lexicographic
            # comparison is wrong whenever microsecond precision varies:
            # "…:29Z" sorts after "…:29.490439Z" because 'Z' > '.', which
            # silently replays rows on every incremental run.
            since_dt = from_iso(since) if since else None

            emitted = 0
            latest_dt = since_dt
            for order_id in range(1, total + 1):
                customer_id = rng.choice(_CUSTOMERS)[0]
                sku, product, category, unit_price = rng.choice(_PRODUCTS)
                quantity = rng.randint(1, 12)
                ordered_dt = start + timedelta(seconds=rng.randint(0, days * 86_400))
                ordered_at = to_iso(ordered_dt)

                # Incremental mode skips anything at or before the checkpoint,
                # which is what lets the test suite assert that a second sync
                # moves fewer rows than the first.
                if since_dt is not None and ordered_dt <= since_dt:
                    continue

                if latest_dt is None or ordered_dt > latest_dt:
                    latest_dt = ordered_dt
                emitted += 1
                yield AirbyteMessage.record_message(
                    "orders",
                    {
                        "order_id": order_id,
                        "customer_id": customer_id,
                        "sku": sku,
                        "product": product,
                        "category": category,
                        "quantity": quantity,
                        "unit_price": unit_price,
                        "amount": round(quantity * unit_price, 2),
                        "ordered_at": ordered_at,
                    },
                )

            if latest_dt is not None:
                state["orders"] = {"cursor": to_iso(latest_dt)}
            yield AirbyteMessage.state_message(dict(state), "orders")
            yield AirbyteMessage.log_message("INFO", f"generated {emitted:,} orders")
