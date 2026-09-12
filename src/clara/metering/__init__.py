"""Metering, pricing, quotas and billing.

The commercial core of the platform. See ``clara.metering.rates`` for the
pricing model and ``docs/pricing.md`` for worked examples.
"""

from __future__ import annotations

from clara.metering.billing import (
    BillingService,
    CreditGrant,
    CreditLedger,
    Invoice,
    InvoiceStatus,
)
from clara.metering.events import Meter, UsageEvent, UsageSummary
from clara.metering.meter import UsageMeter
from clara.metering.pricing import (
    Bill,
    EfficiencyBaseline,
    LineItem,
    LineKind,
    PricingEngine,
    competitor_comparison,
)
from clara.metering.quotas import QuotaCheck, QuotaEngine
from clara.metering.rates import (
    DEFAULT_RATE_CARD,
    PLANS,
    FeeBand,
    Plan,
    PlanLimits,
    PlanSpec,
    RateCard,
    get_rate_card,
    load_rate_card_file,
    register_rate_card,
)
from clara.metering.store import InMemoryUsageStore, UsageStore

__all__ = [
    "Bill",
    "BillingService",
    "CreditGrant",
    "CreditLedger",
    "DEFAULT_RATE_CARD",
    "EfficiencyBaseline",
    "FeeBand",
    "InMemoryUsageStore",
    "Invoice",
    "InvoiceStatus",
    "LineItem",
    "LineKind",
    "Meter",
    "PLANS",
    "Plan",
    "PlanLimits",
    "PlanSpec",
    "PricingEngine",
    "QuotaCheck",
    "QuotaEngine",
    "RateCard",
    "UsageEvent",
    "UsageMeter",
    "UsageStore",
    "UsageSummary",
    "competitor_comparison",
    "get_rate_card",
    "load_rate_card_file",
    "register_rate_card",
]
