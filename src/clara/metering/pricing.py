"""The pricing engine.

Turns aggregated usage into an itemised bill. Every number on that bill is
traceable to an event and a published rate, which is the property that makes
the cost-plus model credible — a customer can reconstruct their own invoice.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any

from clara.metering.events import Meter, UsageSummary
from clara.metering.rates import DEFAULT_RATE_CARD, INF, Plan, PlanSpec, RateCard
from clara.time_utils import to_iso


class LineKind(str, Enum):
    """What kind of charge a line represents. Invoices group by this, so the
    three layers of the pricing model stay visible."""

    #: Layer 1: infrastructure, at cost.
    INFRASTRUCTURE = "infrastructure"
    #: Layer 2: Clara's platform fee.
    PLATFORM_FEE = "platform_fee"
    #: Layer 3: per-unit add-ons.
    ADDON = "addon"
    #: Reductions: commitments, credits, the efficiency dividend.
    DISCOUNT = "discount"
    #: The monthly minimum top-up, when usage falls below it.
    MINIMUM = "minimum"
    #: Clara's share of verified optimiser savings.
    SUCCESS_FEE = "success_fee"


@dataclass
class LineItem:
    """One line on an invoice."""

    kind: LineKind
    description: str
    amount_usd: float
    meter: Meter | None = None
    quantity: float = 0.0
    unit: str = ""
    unit_price_usd: float = 0.0
    #: Free-form explanation, e.g. which fee bands applied.
    detail: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "kind": self.kind.value,
            "description": self.description,
            "meter": self.meter.value if self.meter else None,
            "quantity": round(self.quantity, 6),
            "unit": self.unit,
            "unit_price_usd": round(self.unit_price_usd, 8),
            "amount_usd": round(self.amount_usd, 4),
            "detail": self.detail,
        }


@dataclass
class Bill:
    """A priced period."""

    tenant_id: str
    plan: Plan
    period_start: datetime
    period_end: datetime
    currency: str = "USD"
    rate_card: str = "default"
    line_items: list[LineItem] = field(default_factory=list)

    def add(self, item: LineItem) -> None:
        self.line_items.append(item)

    def _sum(self, *kinds: LineKind) -> float:
        return sum(i.amount_usd for i in self.line_items if i.kind in kinds)

    @property
    def infrastructure_usd(self) -> float:
        return self._sum(LineKind.INFRASTRUCTURE)

    @property
    def platform_fee_usd(self) -> float:
        return self._sum(LineKind.PLATFORM_FEE, LineKind.MINIMUM, LineKind.SUCCESS_FEE)

    @property
    def addons_usd(self) -> float:
        return self._sum(LineKind.ADDON)

    @property
    def discounts_usd(self) -> float:
        """Total reductions, as a negative number."""
        return self._sum(LineKind.DISCOUNT)

    @property
    def subtotal_usd(self) -> float:
        return self.infrastructure_usd + self.platform_fee_usd + self.addons_usd

    @property
    def total_usd(self) -> float:
        return max(0.0, self.subtotal_usd + self.discounts_usd)

    @property
    def effective_markup(self) -> float:
        """Total bill divided by raw infrastructure cost.

        The single most useful number for a buyer: "for every dollar of cloud
        spend, what do I pay in total?"
        """
        if self.infrastructure_usd <= 0:
            return 0.0
        return self.total_usd / self.infrastructure_usd

    def to_dict(self) -> dict[str, Any]:
        return {
            "tenant_id": self.tenant_id,
            "plan": self.plan.value,
            "rate_card": self.rate_card,
            "currency": self.currency,
            "period_start": to_iso(self.period_start),
            "period_end": to_iso(self.period_end),
            "line_items": [i.to_dict() for i in self.line_items],
            "totals": {
                "infrastructure_usd": round(self.infrastructure_usd, 4),
                "platform_fee_usd": round(self.platform_fee_usd, 4),
                "addons_usd": round(self.addons_usd, 4),
                "discounts_usd": round(self.discounts_usd, 4),
                "subtotal_usd": round(self.subtotal_usd, 4),
                "total_usd": round(self.total_usd, 2),
                "effective_markup": round(self.effective_markup, 3),
            },
        }


@dataclass
class EfficiencyBaseline:
    """A workload's trailing efficiency, used to compute the dividend.

    ``ccu_minutes_per_unit`` normalises by work done (bytes scanned, rows
    ingested) so that a customer who simply grows does not look like a
    regression, and Clara does not get paid for a workload that shrank on its
    own.
    """

    ccu_minutes_per_gb_scanned: float
    measured_over_days: int = 30

    def expected_ccu_minutes(self, gb_scanned: float) -> float:
        return self.ccu_minutes_per_gb_scanned * gb_scanned


class PricingEngine:
    """Prices usage summaries against a rate card."""

    def __init__(self, rate_card: RateCard | None = None) -> None:
        self.rate_card = rate_card or DEFAULT_RATE_CARD

    # ------------------------------------------------------------------ pricing

    def price(
        self,
        summary: UsageSummary,
        plan: Plan | str = Plan.TEAM,
        *,
        commitment_term_months: int = 0,
        credits_balance_usd: float = 0.0,
        baseline: EfficiencyBaseline | None = None,
        gb_scanned: float = 0.0,
    ) -> Bill:
        """Price one period.

        Order matters and mirrors the published model: infrastructure at cost,
        then the platform fee on that base, then add-ons above their included
        allowance, then reductions.
        """
        spec = self.rate_card.plan(plan)
        bill = Bill(
            tenant_id=summary.tenant_id,
            plan=spec.plan,
            period_start=summary.period_start,
            period_end=summary.period_end,
            currency=self.rate_card.currency,
            rate_card=self.rate_card.name,
        )

        self._add_infrastructure(bill, summary)
        infra_total = bill.infrastructure_usd

        self._add_platform_fee(bill, spec, infra_total)
        self._add_addons(bill, spec, summary)

        if baseline is not None and gb_scanned > 0:
            self._add_efficiency_dividend(bill, summary, baseline, gb_scanned)

        self._add_commitment_discount(bill, spec, commitment_term_months)
        self._apply_minimum(bill, spec)
        self._apply_credits(bill, credits_balance_usd)
        return bill

    # --------------------------------------------------------------- layer 1

    def _add_infrastructure(self, bill: Bill, summary: UsageSummary) -> None:
        """Infrastructure, at cost. Markups are configurable but default to zero."""
        markups = {
            Meter.STORAGE: self.rate_card.storage_markup,
            Meter.EGRESS: self.rate_card.egress_markup,
        }
        for meter in Meter:
            cost = summary.cost(meter)
            quantity = summary.quantity(meter)
            if cost <= 0 and quantity <= 0:
                continue
            markup = markups.get(meter, 0.0)
            amount = cost * (1.0 + markup)
            label = _meter_label(meter)
            bill.add(
                LineItem(
                    kind=LineKind.INFRASTRUCTURE,
                    description=f"{label} (infrastructure at cost)",
                    meter=meter,
                    quantity=quantity,
                    unit=meter.unit,
                    unit_price_usd=(cost / quantity if quantity else 0.0) * (1.0 + markup),
                    amount_usd=amount,
                    detail=None if markup == 0 else f"includes {markup:.0%} markup",
                )
            )

    # --------------------------------------------------------------- layer 2

    def _add_platform_fee(self, bill: Bill, spec: PlanSpec, infra_total: float) -> None:
        if not spec.fee_bands or infra_total <= 0:
            return
        fee = spec.platform_fee(infra_total)
        blended = spec.effective_rate(infra_total)
        bill.add(
            LineItem(
                kind=LineKind.PLATFORM_FEE,
                description=f"Clara platform fee ({spec.display_name})",
                quantity=infra_total,
                unit="USD infrastructure",
                unit_price_usd=blended,
                amount_usd=fee,
                detail=f"progressive bands, blended {blended:.1%} of infrastructure spend",
            )
        )

    # --------------------------------------------------------------- layer 3

    def _add_addons(self, bill: Bill, spec: PlanSpec, summary: UsageSummary) -> None:
        for meter, rate in spec.addon_rates.items():
            if rate <= 0:
                continue
            included = spec.included.get(meter, 0.0)
            if included == INF:
                continue
            billable = max(0.0, summary.quantity(meter) - included)
            if billable <= 0:
                continue
            bill.add(
                LineItem(
                    kind=LineKind.ADDON,
                    description=f"{_meter_label(meter)} — managed service",
                    meter=meter,
                    quantity=billable,
                    unit=meter.unit,
                    unit_price_usd=rate,
                    amount_usd=billable * rate,
                    detail=f"{included:,.0f} {meter.unit} included in {spec.display_name}",
                )
            )

    # ------------------------------------------------------------- incentives

    def _add_efficiency_dividend(
        self,
        bill: Bill,
        summary: UsageSummary,
        baseline: EfficiencyBaseline,
        gb_scanned: float,
    ) -> None:
        """Share verified optimiser savings with the customer.

        If the workload got *less* efficient, nothing is charged — the mechanism
        is one-directional by design. Clara's share is capped at the platform
        fee, so the total bill can never exceed what it would have been without
        the optimiser.
        """
        expected = baseline.expected_ccu_minutes(gb_scanned)
        actual = summary.quantity(Meter.COMPUTE)
        saved_ccu_minutes = expected - actual
        if saved_ccu_minutes <= 0:
            return

        compute_cost = summary.cost(Meter.COMPUTE)
        cost_per_ccu_minute = compute_cost / actual if actual > 0 else 0.0
        saved_usd = saved_ccu_minutes * cost_per_ccu_minute
        if saved_usd <= 0:
            return

        clara_share = saved_usd * self.rate_card.efficiency_share
        clara_share = min(clara_share, bill.platform_fee_usd)
        customer_share = saved_usd - clara_share

        bill.add(
            LineItem(
                kind=LineKind.SUCCESS_FEE,
                description="Optimiser success fee",
                quantity=saved_ccu_minutes,
                unit="CCU-min saved",
                unit_price_usd=cost_per_ccu_minute * self.rate_card.efficiency_share,
                amount_usd=clara_share,
                detail=(
                    f"${saved_usd:,.2f} saved vs your {baseline.measured_over_days}-day baseline; "
                    f"you keep ${customer_share:,.2f} ({1 - self.rate_card.efficiency_share:.0%}), "
                    f"capped at the platform fee"
                ),
            )
        )

    def _add_commitment_discount(self, bill: Bill, spec: PlanSpec, term_months: int) -> None:
        if term_months <= 0 or not spec.commitments_available:
            return
        rate = self.rate_card.commitment_discount(term_months)
        if rate <= 0:
            return
        # Applied to Clara's own revenue only. Infrastructure is already at cost.
        base = bill.platform_fee_usd + bill.addons_usd
        if base <= 0:
            return
        bill.add(
            LineItem(
                kind=LineKind.DISCOUNT,
                description=f"{term_months}-month commitment discount",
                quantity=base,
                unit="USD platform fee + add-ons",
                unit_price_usd=-rate,
                amount_usd=-base * rate,
                detail="applies to platform fee and add-ons; infrastructure is already at cost",
            )
        )

    def _apply_minimum(self, bill: Bill, spec: PlanSpec) -> None:
        """Top up to the plan's monthly minimum, if usage fell short."""
        if spec.monthly_minimum_usd <= 0:
            return
        clara_revenue = bill.platform_fee_usd + bill.addons_usd + bill.discounts_usd
        shortfall = spec.monthly_minimum_usd - clara_revenue
        if shortfall <= 0:
            return
        bill.add(
            LineItem(
                kind=LineKind.MINIMUM,
                description=f"{spec.display_name} monthly minimum",
                amount_usd=shortfall,
                detail=(
                    f"minimum ${spec.monthly_minimum_usd:,.0f}; "
                    f"usage-based charges were ${clara_revenue:,.2f}"
                ),
            )
        )

    def _apply_credits(self, bill: Bill, credits_balance_usd: float) -> None:
        """Burn prepaid credits against the bill."""
        if credits_balance_usd <= 0:
            return
        applied = min(credits_balance_usd, bill.subtotal_usd + bill.discounts_usd)
        if applied <= 0:
            return
        bill.add(
            LineItem(
                kind=LineKind.DISCOUNT,
                description="Prepaid credits applied",
                amount_usd=-applied,
                detail=f"${credits_balance_usd:,.2f} balance available",
            )
        )

    # -------------------------------------------------------------- estimation

    def estimate_query_cost(
        self,
        *,
        ccu_minutes: float,
        provider: Any,
        plan: Plan | str = Plan.TEAM,
        spot: bool = False,
    ) -> dict[str, Any]:
        """Cost of a single query, for the pre-flight preview.

        Showing this *before* running is how Clara prevents the accidental
        four-figure query that every Snowflake customer has a story about.
        """
        spec = self.rate_card.plan(plan)
        rates = provider.rates()
        from clara.engines.warehouse import CCU_GB_RAM, CCU_VCPUS

        infra_per_ccu_minute = (
            rates.compute_cost_per_hour(CCU_VCPUS, CCU_GB_RAM, spot=spot) / 60.0
        )
        infra = ccu_minutes * infra_per_ccu_minute
        # Marginal fee rate: a single query sits at the top of the month's band.
        fee = infra * spec.fee_rate_at(0.0)
        return {
            "ccu_minutes": round(ccu_minutes, 4),
            "infrastructure_usd": round(infra, 6),
            "platform_fee_usd": round(fee, 6),
            "total_usd": round(infra + fee, 6),
            "provider": provider.name,
            "plan": spec.plan.value,
            "assumes_spot": spot,
        }

    def compare_plans(self, summary: UsageSummary, **kwargs: Any) -> list[dict[str, Any]]:
        """Price the same usage on every plan. Powers "should we upgrade?"."""
        rows = []
        for plan in Plan:
            bill = self.price(summary, plan, **kwargs)
            rows.append(
                {
                    "plan": plan.value,
                    "display_name": self.rate_card.plan(plan).display_name,
                    "total_usd": round(bill.total_usd, 2),
                    "effective_markup": round(bill.effective_markup, 3),
                    "limits_apply": bool(self.rate_card.plan(plan).limits.describe()),
                }
            )
        return sorted(rows, key=lambda r: r["total_usd"])  # type: ignore[arg-type,return-value]


#: Approximate all-in list prices per CCU-hour on competing platforms, for the
#: comparison below. A CCU is 1 vCPU + 4 GB for an hour, so these are derived
#: from each vendor's published unit price and the shape of its smallest
#: warehouse. They are estimates for orientation, not quotes, and exclude
#: negotiated discounts — which is why the comparison always shows its working.
COMPETITOR_CCU_HOUR_USD: dict[str, float] = {
    "snowflake_standard": 0.250,  # 1 credit/hr XS @ $2/credit, ~8 vCPU
    "databricks_sql_serverless": 0.550,
    "databricks_jobs_compute": 0.220,
    "bigquery_on_demand": 0.310,  # $6.25/TB scanned, at ~20 GB/CCU-hour
}


def competitor_comparison(
    provider: Any,
    plan: Plan | str = Plan.TEAM,
    *,
    monthly_ccu_hours: float = 1_000.0,
    rate_card: RateCard | None = None,
) -> dict[str, Any]:
    """Compare Clara's all-in cost per CCU-hour against the incumbents.

    The numbers move with the provider: on a hyperscaler Clara is a few times
    cheaper, and on commodity infrastructure it is an order of magnitude
    cheaper — which is the argument for making the platform cloud-agnostic in
    the first place.
    """
    card = rate_card or DEFAULT_RATE_CARD
    spec = card.plan(plan)
    from clara.engines.warehouse import CCU_GB_RAM, CCU_VCPUS

    infra_per_ccu_hour = provider.rates().compute_cost_per_hour(CCU_VCPUS, CCU_GB_RAM)
    monthly_infra = infra_per_ccu_hour * monthly_ccu_hours
    monthly_fee = spec.platform_fee(monthly_infra)
    clara_per_ccu_hour = (
        (monthly_infra + monthly_fee) / monthly_ccu_hours if monthly_ccu_hours else 0.0
    )

    competitors = []
    for name, price in sorted(COMPETITOR_CCU_HOUR_USD.items(), key=lambda kv: kv[1]):
        # Incumbent pricing excludes the customer's own infrastructure for
        # storage and networking, so compare compute-to-compute.
        competitors.append(
            {
                "platform": name,
                "usd_per_ccu_hour": price,
                "monthly_usd": round(price * monthly_ccu_hours, 2),
                "clara_is_cheaper_by": round(price / clara_per_ccu_hour, 1)
                if clara_per_ccu_hour > 0
                else None,
            }
        )

    return {
        "provider": provider.name,
        "plan": spec.plan.value,
        "monthly_ccu_hours": monthly_ccu_hours,
        "clara": {
            "infrastructure_usd_per_ccu_hour": round(infra_per_ccu_hour, 5),
            "all_in_usd_per_ccu_hour": round(clara_per_ccu_hour, 5),
            "monthly_infrastructure_usd": round(monthly_infra, 2),
            "monthly_platform_fee_usd": round(monthly_fee, 2),
            "monthly_total_usd": round(monthly_infra + monthly_fee, 2),
        },
        "competitors": competitors,
        "assumptions": (
            "1 CCU = 1 vCPU + 4 GB RAM for 1 hour. Competitor figures are "
            "approximate public list prices for compute only, before negotiated "
            "discounts. Clara figures use this provider's on-demand rates."
        ),
    }


def _meter_label(meter: Meter) -> str:
    return {
        Meter.COMPUTE: "Compute",
        Meter.STORAGE: "Storage",
        Meter.INGEST: "Data ingestion",
        Meter.ORCHESTRATION: "Orchestration",
        Meter.EGRESS: "Data egress",
        Meter.API: "API requests",
    }[meter]
