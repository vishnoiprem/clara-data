"""Quota and free-tier enforcement.

Requirement: *the trial version should be limited to what the cloud providers
provide as free limits*. That is implemented literally — trial limits are
**derived at runtime from the configured provider's free tier**, not copied into
a constant. Run Clara's trial on GCP's always-free e2-micro and it never
expires; run it on Tencent, where there is no free compute hour, and the trial
correctly offers storage but no warehouse allowance.

The consequence is that a trial costs the operator nothing to host, so it does
not need an artificial 14-day fuse.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Any

from clara.engines.warehouse import CCU_GB_RAM, CCU_VCPUS, WarehouseSize, spec_for
from clara.errors import BudgetExceededError, QuotaExceededError
from clara.logging_setup import get_logger
from clara.metering.events import Meter
from clara.metering.rates import INF, Plan, PlanLimits, PlanSpec, RateCard, get_rate_card
from clara.metering.store import UsageStore
from clara.providers import CloudProvider
from clara.time_utils import day_bounds, month_bounds

log = get_logger(__name__)


@dataclass
class QuotaCheck:
    """The result of one limit check."""

    allowed: bool
    limit_name: str
    limit: float | None = None
    used: float = 0.0
    unit: str = ""
    message: str = ""

    @property
    def remaining(self) -> float:
        if self.limit is None:
            return INF
        return max(0.0, self.limit - self.used)

    @property
    def utilization(self) -> float:
        if not self.limit or self.limit == INF:
            return 0.0
        return min(1.0, self.used / self.limit)

    def raise_if_denied(self) -> None:
        if not self.allowed:
            raise QuotaExceededError(
                self.message or f"quota exceeded: {self.limit_name}",
                limit_name=self.limit_name,
                limit=self.limit,
                used=round(self.used, 4),
                unit=self.unit,
            )

    def to_dict(self) -> dict[str, Any]:
        return {
            "allowed": self.allowed,
            "limit_name": self.limit_name,
            "limit": None if self.limit == INF else self.limit,
            "used": round(self.used, 4),
            "remaining": None if self.remaining == INF else round(self.remaining, 4),
            "utilization": round(self.utilization, 4),
            "unit": self.unit,
            "message": self.message,
        }


class QuotaEngine:
    """Resolves effective limits and enforces them."""

    def __init__(
        self,
        store: UsageStore,
        provider: CloudProvider,
        *,
        rate_card: RateCard | None = None,
        budget_cap_usd: float | None = None,
    ) -> None:
        self.store = store
        self.provider = provider
        self.rate_card = rate_card or get_rate_card()
        self.budget_cap_usd = budget_cap_usd

    # ------------------------------------------------------- limit resolution

    def effective_limits(self, plan: Plan | str) -> PlanLimits:
        """Plan limits, with trial limits derived from the provider's free tier.

        This is the mechanism behind the free-tier requirement: the trial's
        compute and storage allowances come from whatever the provider actually
        gives away, so Clara never subsidises a trial.
        """
        spec: PlanSpec = self.rate_card.plan(plan)
        limits = spec.limits
        if not limits.bound_to_provider_free_tier:
            return limits

        free = self.provider.free_tier()
        monthly_ccu = free.ccu_minutes_month(CCU_VCPUS, CCU_GB_RAM)

        # An infinite allowance means local development: no limits at all.
        if monthly_ccu == INF:
            return replace(
                limits,
                daily_ccu_minutes=None,
                monthly_ccu_minutes=None,
                storage_gb=None,
                monthly_ingest_gb=None,
            )

        # Spread the monthly allowance evenly, so one bad day cannot exhaust the
        # month and leave the trial dead for three weeks.
        daily_ccu = monthly_ccu / 30.0 if monthly_ccu > 0 else 0.0
        return replace(
            limits,
            monthly_ccu_minutes=monthly_ccu,
            daily_ccu_minutes=daily_ccu,
            storage_gb=free.storage_gb if free.storage_gb > 0 else limits.storage_gb,
            # Ingest is bounded by free storage: you cannot land more than you
            # can keep.
            monthly_ingest_gb=min(
                limits.monthly_ingest_gb or INF,
                free.storage_gb if free.storage_gb > 0 else INF,
            ),
        )

    def describe(self, plan: Plan | str, tenant_id: str) -> dict[str, Any]:
        """Current usage against every resolved limit — the dashboard payload."""
        limits = self.effective_limits(plan)
        checks = [
            self.check_compute(tenant_id, plan, ccu_minutes=0.0),
            self.check_storage(tenant_id, plan, additional_gb=0.0),
            self.check_ingest(tenant_id, plan, additional_gb=0.0),
            self.check_orchestration(tenant_id, plan, additional_minutes=0.0),
        ]
        free = self.provider.free_tier()
        return {
            "plan": str(getattr(plan, "value", plan)),
            "provider": self.provider.name,
            "limits": limits.describe(),
            "derived_from_provider_free_tier": limits.bound_to_provider_free_tier,
            "provider_free_tier_notes": free.notes,
            "checks": [c.to_dict() for c in checks],
        }

    # -------------------------------------------------------------- the checks

    def check_compute(
        self, tenant_id: str, plan: Plan | str, *, ccu_minutes: float
    ) -> QuotaCheck:
        """Daily and monthly compute allowance."""
        limits = self.effective_limits(plan)

        if limits.daily_ccu_minutes is not None:
            start, end = day_bounds()
            used = self.store.total(tenant_id, Meter.COMPUTE, start=start, end=end)
            if used + ccu_minutes > limits.daily_ccu_minutes:
                return QuotaCheck(
                    allowed=False,
                    limit_name="daily_ccu_minutes",
                    limit=limits.daily_ccu_minutes,
                    used=used,
                    unit="CCU-min",
                    message=(
                        f"daily compute allowance exhausted: {used:,.1f} of "
                        f"{limits.daily_ccu_minutes:,.1f} CCU-minutes used. "
                        "Upgrade the plan or wait for the daily reset."
                    ),
                )

        if limits.monthly_ccu_minutes is not None:
            start, end = month_bounds()
            used = self.store.total(tenant_id, Meter.COMPUTE, start=start, end=end)
            if used + ccu_minutes > limits.monthly_ccu_minutes:
                return QuotaCheck(
                    allowed=False,
                    limit_name="monthly_ccu_minutes",
                    limit=limits.monthly_ccu_minutes,
                    used=used,
                    unit="CCU-min",
                    message=(
                        f"monthly compute allowance exhausted: {used:,.1f} of "
                        f"{limits.monthly_ccu_minutes:,.1f} CCU-minutes used."
                    ),
                )

        start, end = day_bounds()
        return QuotaCheck(
            allowed=True,
            limit_name="daily_ccu_minutes",
            limit=limits.daily_ccu_minutes,
            used=self.store.total(tenant_id, Meter.COMPUTE, start=start, end=end),
            unit="CCU-min",
        )

    def check_storage(
        self, tenant_id: str, plan: Plan | str, *, additional_gb: float
    ) -> QuotaCheck:
        limits = self.effective_limits(plan)
        start, end = month_bounds()
        # Storage is metered as GB-months; convert back to an instantaneous GB
        # figure for a limit that is expressed in GB.
        from clara.time_utils import hours_in_month, utcnow

        elapsed_fraction = max(
            (utcnow() - start).total_seconds() / (hours_in_month() * 3600.0), 1e-6
        )
        gb_months = self.store.total(tenant_id, Meter.STORAGE, start=start, end=end)
        used_gb = gb_months / elapsed_fraction

        if limits.storage_gb is not None and used_gb + additional_gb > limits.storage_gb:
            return QuotaCheck(
                allowed=False,
                limit_name="storage_gb",
                limit=limits.storage_gb,
                used=used_gb,
                unit="GB",
                message=(
                    f"storage limit reached: {used_gb:,.2f} of {limits.storage_gb:,.2f} GB. "
                    "Delete tables or upgrade the plan."
                ),
            )
        return QuotaCheck(
            allowed=True, limit_name="storage_gb", limit=limits.storage_gb, used=used_gb, unit="GB"
        )

    def check_ingest(
        self, tenant_id: str, plan: Plan | str, *, additional_gb: float
    ) -> QuotaCheck:
        limits = self.effective_limits(plan)
        start, end = month_bounds()
        used = self.store.total(tenant_id, Meter.INGEST, start=start, end=end)
        limit = limits.monthly_ingest_gb
        if limit is not None and limit != INF and used + additional_gb > limit:
            return QuotaCheck(
                allowed=False,
                limit_name="monthly_ingest_gb",
                limit=limit,
                used=used,
                unit="GB",
                message=f"monthly ingest allowance exhausted: {used:,.2f} of {limit:,.2f} GB.",
            )
        return QuotaCheck(
            allowed=True, limit_name="monthly_ingest_gb", limit=limit, used=used, unit="GB"
        )

    def check_orchestration(
        self, tenant_id: str, plan: Plan | str, *, additional_minutes: float
    ) -> QuotaCheck:
        limits = self.effective_limits(plan)
        start, end = day_bounds()
        used = self.store.total(tenant_id, Meter.ORCHESTRATION, start=start, end=end)
        limit = limits.daily_orchestration_minutes
        if limit is not None and used + additional_minutes > limit:
            return QuotaCheck(
                allowed=False,
                limit_name="daily_orchestration_minutes",
                limit=limit,
                used=used,
                unit="task-min",
                message=f"daily orchestration allowance exhausted: {used:,.1f} of {limit:,.1f} min.",
            )
        return QuotaCheck(
            allowed=True,
            limit_name="daily_orchestration_minutes",
            limit=limit,
            used=used,
            unit="task-min",
        )

    def check_warehouse_size(self, plan: Plan | str, size: WarehouseSize | str) -> QuotaCheck:
        """Whether a plan may run a warehouse of this size."""
        limits = self.effective_limits(plan)
        if limits.max_warehouse_size is None:
            return QuotaCheck(allowed=True, limit_name="max_warehouse_size")

        requested = spec_for(size)
        allowed_spec = spec_for(limits.max_warehouse_size)
        if requested.total_vcpus > allowed_spec.total_vcpus:
            return QuotaCheck(
                allowed=False,
                limit_name="max_warehouse_size",
                limit=float(allowed_spec.total_vcpus),
                used=float(requested.total_vcpus),
                unit="vCPU",
                message=(
                    f"plan allows warehouses up to size "
                    f"{limits.max_warehouse_size.upper()}; "
                    f"{requested.size.value.upper()} was requested."
                ),
            )
        return QuotaCheck(allowed=True, limit_name="max_warehouse_size")

    def check_warehouse_count(self, plan: Plan | str, current_count: int) -> QuotaCheck:
        limits = self.effective_limits(plan)
        if limits.max_warehouses is None:
            return QuotaCheck(allowed=True, limit_name="max_warehouses")
        if current_count >= limits.max_warehouses:
            return QuotaCheck(
                allowed=False,
                limit_name="max_warehouses",
                limit=float(limits.max_warehouses),
                used=float(current_count),
                unit="warehouses",
                message=f"plan allows {limits.max_warehouses} warehouse(s).",
            )
        return QuotaCheck(
            allowed=True,
            limit_name="max_warehouses",
            limit=float(limits.max_warehouses),
            used=float(current_count),
        )

    # ---------------------------------------------------------------- budgets

    def check_budget(self, tenant_id: str, plan: Plan | str, *, additional_usd: float = 0.0) -> None:
        """Enforce a customer-set spend cap.

        A hard cap, not an alert. The most common complaint about usage-based
        data platforms is a surprise invoice; a cap that actually refuses work
        is the only real fix.
        """
        if self.budget_cap_usd is None:
            return

        from clara.metering.pricing import PricingEngine

        summary = self.store.summarize(tenant_id)
        bill = PricingEngine(self.rate_card).price(summary, plan)
        projected = bill.total_usd + additional_usd
        if projected > self.budget_cap_usd:
            raise BudgetExceededError(
                f"monthly budget cap of ${self.budget_cap_usd:,.2f} would be exceeded "
                f"(projected ${projected:,.2f}). Raise the cap to continue.",
                cap_usd=self.budget_cap_usd,
                projected_usd=round(projected, 2),
            )

    # ------------------------------------------------------------- composite

    def authorize_query(
        self,
        tenant_id: str,
        plan: Plan | str,
        *,
        estimated_ccu_minutes: float = 0.0,
        warehouse_size: WarehouseSize | str | None = None,
    ) -> None:
        """Run every check that applies before a query, raising on the first
        failure. One call keeps the query path from drifting out of sync with
        the limit set."""
        if warehouse_size is not None:
            self.check_warehouse_size(plan, warehouse_size).raise_if_denied()
        self.check_compute(tenant_id, plan, ccu_minutes=estimated_ccu_minutes).raise_if_denied()
        self.check_budget(tenant_id, plan)
