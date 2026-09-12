"""Rate cards and plans — Clara's commercial model in code.

The model is **transparent cost-plus**, and it is a deliberate rejection of how
Databricks and Snowflake price:

* **Layer 1 — infrastructure, at cost, zero markup.** The customer sees the
  actual cloud spend their workloads caused, priced from the provider's own
  rates. They can check it against their cloud bill. If they bring their own
  cloud account, Clara never touches this layer at all.
* **Layer 2 — platform fee, a declining percentage of infrastructure spend.**
  This is Clara's revenue. Expressed as a percentage of a number the customer
  can independently verify, so the value exchange is legible.
* **Layer 3 — add-ons, per unit.** Managed connectors and premium support:
  optional, itemised, and avoidable by self-hosting.

Three mechanisms make it more than a markup:

* **The efficiency dividend.** When Clara's optimiser reduces a workload's
  CCU-minutes against its own trailing baseline, the customer keeps 70% of the
  saving and Clara takes 30% as a success fee — capped so a bill can never
  exceed the unoptimised one. Databricks and Snowflake earn *more* when your
  queries are slow; this inverts that incentive.
* **The community plan is free forever.** Self-host, pay nothing, no feature
  gates. The platform fee only applies to Clara-operated infrastructure.
* **The trial is bounded by the cloud provider's own free tier**, so a trial
  costs the operator nothing to host and can therefore run indefinitely rather
  than expiring in 14 days.

See ``docs/pricing.md`` for worked examples.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from clara.errors import ValidationError
from clara.metering.events import Meter

INF = float("inf")


class Plan(str, Enum):
    """Commercial plans."""

    #: Free, bounded by the cloud provider's free tier. No card required.
    TRIAL = "trial"
    #: Self-hosted open source. No platform fee, no limits, no support.
    COMMUNITY = "community"
    #: Clara-operated, usage-based.
    TEAM = "team"
    #: Lower fee percentage, commitments, SLA.
    BUSINESS = "business"
    #: Negotiated, BYOC, dedicated support.
    ENTERPRISE = "enterprise"


@dataclass(frozen=True)
class FeeBand:
    """A progressive platform-fee band.

    Bands are marginal, like income tax: spend inside each band is charged at
    that band's rate. A customer crossing a threshold never sees their whole
    bill re-rated, which is the usual complaint about volume tiers.
    """

    #: Upper bound of monthly infrastructure spend for this band, in USD.
    up_to_usd: float
    rate: float


@dataclass(frozen=True)
class PlanLimits:
    """Hard limits enforced by the quota engine.

    ``None`` means unlimited. Trial numbers are derived from the cloud
    provider's free tier at runtime, not hard-coded here.
    """

    max_warehouses: int | None = None
    max_warehouse_size: str | None = None
    max_concurrent_queries: int | None = None
    daily_ccu_minutes: float | None = None
    monthly_ccu_minutes: float | None = None
    storage_gb: float | None = None
    monthly_ingest_gb: float | None = None
    daily_orchestration_minutes: float | None = None
    max_users: int | None = None
    #: Query history and usage-event retention.
    retention_days: int = 365
    #: Derive compute/storage limits from the provider's free tier.
    bound_to_provider_free_tier: bool = False

    def describe(self) -> dict[str, Any]:
        return {
            k: v
            for k, v in {
                "max_warehouses": self.max_warehouses,
                "max_warehouse_size": self.max_warehouse_size,
                "max_concurrent_queries": self.max_concurrent_queries,
                "daily_ccu_minutes": self.daily_ccu_minutes,
                "monthly_ccu_minutes": self.monthly_ccu_minutes,
                "storage_gb": self.storage_gb,
                "monthly_ingest_gb": self.monthly_ingest_gb,
                "daily_orchestration_minutes": self.daily_orchestration_minutes,
                "max_users": self.max_users,
                "retention_days": self.retention_days,
                "bound_to_provider_free_tier": self.bound_to_provider_free_tier,
            }.items()
            if v is not None
        }


@dataclass(frozen=True)
class PlanSpec:
    """Everything commercial about one plan."""

    plan: Plan
    display_name: str
    #: Progressive platform-fee bands. Empty means no platform fee.
    fee_bands: tuple[FeeBand, ...] = ()
    #: Monthly minimum for the platform fee, in USD.
    monthly_minimum_usd: float = 0.0
    #: Per-unit add-on prices, on top of infrastructure cost.
    addon_rates: dict[Meter, float] = field(default_factory=dict)
    #: Monthly allowance per meter, free before add-on charges begin.
    included: dict[Meter, float] = field(default_factory=dict)
    limits: PlanLimits = field(default_factory=PlanLimits)
    #: Whether prepaid commitments and their discounts are available.
    commitments_available: bool = False
    support: str = "community forum"
    sla: str | None = None

    def fee_rate_at(self, infra_spend_usd: float) -> float:
        """Marginal fee rate at a given monthly spend. For display only."""
        for band in self.fee_bands:
            if infra_spend_usd <= band.up_to_usd:
                return band.rate
        return self.fee_bands[-1].rate if self.fee_bands else 0.0

    def platform_fee(self, infra_spend_usd: float) -> float:
        """Progressive platform fee on a month's infrastructure spend."""
        if not self.fee_bands:
            return 0.0
        fee = 0.0
        lower = 0.0
        for band in self.fee_bands:
            if infra_spend_usd <= lower:
                break
            taxable = min(infra_spend_usd, band.up_to_usd) - lower
            fee += max(taxable, 0.0) * band.rate
            lower = band.up_to_usd
        return fee

    def effective_rate(self, infra_spend_usd: float) -> float:
        """Blended fee percentage actually paid. The number customers compare."""
        if infra_spend_usd <= 0:
            return 0.0
        return self.platform_fee(infra_spend_usd) / infra_spend_usd

    def describe(self) -> dict[str, Any]:
        return {
            "plan": self.plan.value,
            "display_name": self.display_name,
            "monthly_minimum_usd": self.monthly_minimum_usd,
            "fee_bands": [
                {"up_to_usd": None if b.up_to_usd == INF else b.up_to_usd, "rate": b.rate}
                for b in self.fee_bands
            ],
            "addon_rates": {m.value: r for m, r in self.addon_rates.items()},
            "included": {m.value: q for m, q in self.included.items()},
            "limits": self.limits.describe(),
            "commitments_available": self.commitments_available,
            "support": self.support,
            "sla": self.sla,
        }


# --------------------------------------------------------------------- plans

PLANS: dict[Plan, PlanSpec] = {
    Plan.TRIAL: PlanSpec(
        plan=Plan.TRIAL,
        display_name="Trial",
        fee_bands=(),
        monthly_minimum_usd=0.0,
        included={
            Meter.INGEST: 5.0,
            Meter.ORCHESTRATION: 500.0,
            Meter.API: INF,
        },
        limits=PlanLimits(
            max_warehouses=1,
            max_warehouse_size="xs",
            max_concurrent_queries=2,
            # Replaced at runtime by the provider's actual free-tier allowance.
            daily_ccu_minutes=360.0,
            storage_gb=5.0,
            monthly_ingest_gb=5.0,
            daily_orchestration_minutes=60.0,
            max_users=3,
            retention_days=14,
            bound_to_provider_free_tier=True,
        ),
        support="community forum",
    ),
    Plan.COMMUNITY: PlanSpec(
        plan=Plan.COMMUNITY,
        display_name="Community (self-hosted)",
        # The whole point: run it yourself, pay Clara nothing, forever.
        fee_bands=(),
        monthly_minimum_usd=0.0,
        included=dict.fromkeys(Meter, INF),
        limits=PlanLimits(retention_days=90),
        support="community forum",
    ),
    Plan.TEAM: PlanSpec(
        plan=Plan.TEAM,
        display_name="Team",
        fee_bands=(
            FeeBand(2_000.0, 0.30),
            FeeBand(10_000.0, 0.22),
            FeeBand(50_000.0, 0.15),
            FeeBand(INF, 0.10),
        ),
        monthly_minimum_usd=99.0,
        addon_rates={Meter.INGEST: 0.02},
        included={
            Meter.INGEST: 100.0,
            Meter.ORCHESTRATION: 5_000.0,
            Meter.API: INF,
        },
        limits=PlanLimits(max_concurrent_queries=20, max_users=25, retention_days=180),
        commitments_available=True,
        support="email, next business day",
        sla="99.5% control plane",
    ),
    Plan.BUSINESS: PlanSpec(
        plan=Plan.BUSINESS,
        display_name="Business",
        fee_bands=(
            FeeBand(10_000.0, 0.20),
            FeeBand(50_000.0, 0.13),
            FeeBand(INF, 0.09),
        ),
        monthly_minimum_usd=999.0,
        addon_rates={Meter.INGEST: 0.015},
        included={
            Meter.INGEST: 1_000.0,
            Meter.ORCHESTRATION: 50_000.0,
            Meter.API: INF,
        },
        limits=PlanLimits(max_concurrent_queries=100, retention_days=365),
        commitments_available=True,
        support="priority, 4h response",
        sla="99.9% control plane",
    ),
    Plan.ENTERPRISE: PlanSpec(
        plan=Plan.ENTERPRISE,
        display_name="Enterprise",
        fee_bands=(FeeBand(INF, 0.08),),
        monthly_minimum_usd=4_999.0,
        addon_rates={Meter.INGEST: 0.01},
        included=dict.fromkeys((Meter.INGEST, Meter.ORCHESTRATION, Meter.API), INF),
        limits=PlanLimits(retention_days=1095),
        commitments_available=True,
        support="dedicated engineer, 1h response",
        sla="99.95% control plane, custom data plane",
    ),
}


@dataclass(frozen=True)
class RateCard:
    """A priced configuration: plans, discounts and incentive parameters.

    Named rate cards let an operator run regional or partner pricing without
    forking the code — ``CLARA_RATE_CARD=partner_apac``.
    """

    name: str = "default"
    currency: str = "USD"

    #: Term length in months -> discount on platform fee and add-ons.
    #: Infrastructure is already at cost, so it is never discounted.
    commitment_discounts: dict[int, float] = field(
        default_factory=lambda: {0: 0.0, 12: 0.15, 36: 0.25}
    )

    #: Prepaid credit purchase -> bonus credit fraction.
    credit_bonus_tiers: dict[float, float] = field(
        default_factory=lambda: {10_000.0: 0.05, 50_000.0: 0.10, 100_000.0: 0.15}
    )

    #: Clara's share of verified optimiser savings. Capped at the platform fee,
    #: so the mechanism can only ever reduce a customer's total bill.
    efficiency_share: float = 0.30

    #: Markup on storage. Zero by design — charging for storage on an open table
    #: format would be charging for data the customer already owns.
    storage_markup: float = 0.0

    #: Markup on egress. Zero: passing through a cloud's egress charge at cost
    #: removes any incentive to keep data hostage.
    egress_markup: float = 0.0

    #: Minimum billable duration per query, in seconds. Prevents per-statement
    #: rounding from dominating the bill on thousands of tiny queries.
    minimum_billable_seconds: float = 1.0

    def plan(self, plan: Plan | str) -> PlanSpec:
        resolved = Plan(str(plan).lower()) if not isinstance(plan, Plan) else plan
        spec = PLANS.get(resolved)
        if spec is None:  # pragma: no cover - Plan enum is exhaustive
            raise ValidationError(f"unknown plan: {plan}")
        return spec

    def commitment_discount(self, term_months: int) -> float:
        """Discount for a commitment term, using the best qualifying tier."""
        qualifying = [d for term, d in self.commitment_discounts.items() if term_months >= term]
        return max(qualifying) if qualifying else 0.0

    def credit_bonus(self, purchase_usd: float) -> float:
        """Bonus credit fraction for a prepaid purchase."""
        qualifying = [b for threshold, b in self.credit_bonus_tiers.items() if purchase_usd >= threshold]
        return max(qualifying) if qualifying else 0.0

    def describe(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "currency": self.currency,
            "commitment_discounts": self.commitment_discounts,
            "credit_bonus_tiers": self.credit_bonus_tiers,
            "efficiency_share": self.efficiency_share,
            "storage_markup": self.storage_markup,
            "egress_markup": self.egress_markup,
            "minimum_billable_seconds": self.minimum_billable_seconds,
            "plans": [spec.describe() for spec in PLANS.values()],
        }


DEFAULT_RATE_CARD = RateCard()

_RATE_CARDS: dict[str, RateCard] = {DEFAULT_RATE_CARD.name: DEFAULT_RATE_CARD}


def register_rate_card(card: RateCard) -> None:
    """Register a named rate card, e.g. for regional or partner pricing."""
    _RATE_CARDS[card.name] = card


def get_rate_card(name: str = "default") -> RateCard:
    card = _RATE_CARDS.get(name)
    if card is None:
        raise ValidationError(f"unknown rate card: {name}", available=sorted(_RATE_CARDS))
    return card


def load_rate_card_file(path: str) -> RateCard:
    """Load a rate card from YAML, so operators can price without code changes."""
    import yaml

    with open(path) as handle:
        payload = yaml.safe_load(handle) or {}

    card = RateCard(
        name=payload.get("name", "custom"),
        currency=payload.get("currency", "USD"),
        commitment_discounts={
            int(k): float(v) for k, v in (payload.get("commitment_discounts") or {}).items()
        }
        or DEFAULT_RATE_CARD.commitment_discounts,
        credit_bonus_tiers={
            float(k): float(v) for k, v in (payload.get("credit_bonus_tiers") or {}).items()
        }
        or DEFAULT_RATE_CARD.credit_bonus_tiers,
        efficiency_share=float(payload.get("efficiency_share", 0.30)),
        storage_markup=float(payload.get("storage_markup", 0.0)),
        egress_markup=float(payload.get("egress_markup", 0.0)),
        minimum_billable_seconds=float(payload.get("minimum_billable_seconds", 1.0)),
    )
    register_rate_card(card)
    return card
