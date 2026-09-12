"""Metering, pricing, quota and billing tests.

These cover the commercial model, where a bug is a wrong invoice.
"""

from __future__ import annotations

import pytest

from clara.engines.base import QueryStats
from clara.engines.warehouse import (
    CCU_GB_RAM,
    CCU_VCPUS,
    Warehouse,
    WarehouseSize,
    recommend_size,
    spec_for,
)
from clara.errors import BudgetExceededError, QuotaExceededError
from clara.metering import (
    BillingService,
    CreditLedger,
    EfficiencyBaseline,
    InMemoryUsageStore,
    Invoice,
    LineKind,
    Meter,
    Plan,
    PricingEngine,
    QuotaEngine,
    RateCard,
    UsageMeter,
    competitor_comparison,
    get_rate_card,
)
from clara.providers import build_provider
from clara.settings import Settings


@pytest.fixture
def aws(settings: Settings):  # noqa: ANN201
    return build_provider(settings, "aws")


def _load(meter: UsageMeter, *, queries: int = 100, seconds: float = 10.0) -> Warehouse:
    """Record a representative workload."""
    warehouse = Warehouse(name="wh", size=WarehouseSize.M)
    warehouse.mark_started()
    for index in range(queries):
        meter.record_query(
            QueryStats(engine="trino", wall_seconds=seconds, warehouse_seconds=seconds),
            warehouse,
            query_id=f"q{index}",
        )
    return warehouse


class TestWarehouseSizing:
    def test_ccu_rate_matches_vcpu_count(self) -> None:
        # A CCU is 1 vCPU + 4 GB, and every size keeps a 1:4 ratio, so the CCU
        # rate must equal total vCPUs. This is the anchor of the whole model.
        for size in WarehouseSize:
            spec = spec_for(size)
            assert spec.ccu_per_minute == spec.total_vcpus
            assert spec.total_gb_ram / spec.total_vcpus == CCU_GB_RAM / CCU_VCPUS

    def test_each_size_doubles_capacity(self) -> None:
        sizes = list(WarehouseSize)
        for smaller, larger in zip(sizes, sizes[1:], strict=False):
            assert spec_for(larger).total_vcpus == 2 * spec_for(smaller).total_vcpus

    def test_recommends_larger_size_for_bigger_scans(self) -> None:
        assert recommend_size(0) is WarehouseSize.XS
        small = recommend_size(1024**3)
        large = recommend_size(500 * 1024**3)
        assert spec_for(large).total_vcpus > spec_for(small).total_vcpus

    def test_auto_suspend_respects_idle_timeout(self) -> None:
        from datetime import timedelta

        from clara.time_utils import utcnow

        warehouse = Warehouse(name="wh", auto_suspend_seconds=60)
        warehouse.mark_started()
        assert not warehouse.should_suspend()

        warehouse.last_activity_at = utcnow() - timedelta(seconds=61)
        assert warehouse.should_suspend()

    def test_zero_auto_suspend_never_suspends(self) -> None:
        from datetime import timedelta

        from clara.time_utils import utcnow

        warehouse = Warehouse(name="wh", auto_suspend_seconds=0)
        warehouse.mark_started()
        warehouse.last_activity_at = utcnow() - timedelta(hours=5)
        assert not warehouse.should_suspend(), "0 means 'never suspend', an explicit opt-in"


class TestMetering:
    def test_records_compute_in_ccu_minutes(self, meter: UsageMeter) -> None:
        warehouse = Warehouse(name="wh", size=WarehouseSize.M)  # 8 CCU/min
        event = meter.record_query(
            QueryStats(engine="duckdb", wall_seconds=60.0, warehouse_seconds=60.0), warehouse
        )
        assert event is not None
        assert event.quantity == pytest.approx(8.0), "1 minute on an M warehouse = 8 CCU-min"

    def test_cache_hits_are_free(self, meter: UsageMeter) -> None:
        warehouse = Warehouse(name="wh")
        stats = QueryStats(engine="trino", wall_seconds=30.0, cache_hit=True)
        assert meter.record_query(stats, warehouse) is None

    def test_applies_minimum_billable_duration(self, meter: UsageMeter) -> None:
        meter.minimum_billable_seconds = 1.0
        warehouse = Warehouse(name="wh", size=WarehouseSize.XS)  # 2 CCU/min
        event = meter.record_query(
            QueryStats(engine="duckdb", wall_seconds=0.01, warehouse_seconds=0.01), warehouse
        )
        assert event.quantity == pytest.approx(2.0 / 60.0)

    def test_snapshots_unit_cost_on_the_event(self, meter: UsageMeter, aws) -> None:  # noqa: ANN001
        event = meter.record(Meter.STORAGE, 10.0)
        # Costs are frozen at capture time so re-billing a past month is stable.
        assert event.unit_cost_usd == aws.rates().storage_gb_month
        assert event.infra_cost_usd == pytest.approx(10.0 * aws.rates().storage_gb_month)

    def test_ignores_non_positive_quantities(self, meter: UsageMeter) -> None:
        assert meter.record(Meter.COMPUTE, 0.0) is None
        assert meter.record(Meter.COMPUTE, -5.0) is None

    def test_prorates_storage_by_hours_held(self, meter: UsageMeter) -> None:
        from clara.time_utils import hours_in_month

        event = meter.record_storage_snapshot(1_000_000_000, hours=1.0)  # 1 GB for 1 h
        assert event.quantity == pytest.approx(1.0 / hours_in_month(), rel=1e-6)

    def test_summary_aggregates_by_meter(self, meter: UsageMeter, usage_store) -> None:  # noqa: ANN001
        _load(meter, queries=10, seconds=6.0)
        meter.record_sync(bytes_moved=2e9, rows=1000, connector="postgres")
        summary = usage_store.summarize("ten_test")

        assert summary.quantity(Meter.COMPUTE) == pytest.approx(8.0)  # 10 × 0.1 min × 8
        assert summary.quantity(Meter.INGEST) == pytest.approx(2.0)
        assert summary.total_infra_cost_usd > 0


class TestPricing:
    def test_infrastructure_is_passed_through_at_cost(self, meter: UsageMeter, usage_store) -> None:  # noqa: ANN001
        _load(meter, queries=50, seconds=12.0)
        summary = usage_store.summarize("ten_test")
        bill = PricingEngine().price(summary, Plan.COMMUNITY)

        # Community has no platform fee, so the bill is exactly the infra cost.
        assert bill.platform_fee_usd == 0
        assert bill.total_usd == pytest.approx(summary.total_infra_cost_usd)

    def test_community_plan_is_free_of_platform_fees(self, meter: UsageMeter, usage_store) -> None:  # noqa: ANN001
        _load(meter, queries=500, seconds=60.0)
        bill = PricingEngine().price(usage_store.summarize("ten_test"), Plan.COMMUNITY)
        assert bill.platform_fee_usd == 0, "self-hosting must stay free at any scale"

    def test_fee_bands_are_marginal_not_cliff_edged(self) -> None:
        spec = get_rate_card().plan(Plan.TEAM)
        # Crossing a threshold must not re-rate everything below it.
        assert spec.platform_fee(2_000) == pytest.approx(2_000 * 0.30)
        assert spec.platform_fee(3_000) == pytest.approx(2_000 * 0.30 + 1_000 * 0.22)
        # Blended rate declines with volume.
        assert spec.effective_rate(100_000) < spec.effective_rate(1_000)

    def test_fee_is_monotonic_in_spend(self) -> None:
        spec = get_rate_card().plan(Plan.TEAM)
        fees = [spec.platform_fee(s) for s in (0, 500, 2_000, 9_999, 50_000, 250_000)]
        assert fees == sorted(fees), "a bigger bill can never mean a smaller fee"

    def test_monthly_minimum_tops_up_small_usage(self, meter: UsageMeter, usage_store) -> None:  # noqa: ANN001
        meter.record(Meter.COMPUTE, 1.0)
        bill = PricingEngine().price(usage_store.summarize("ten_test"), Plan.TEAM)
        minimum = get_rate_card().plan(Plan.TEAM).monthly_minimum_usd
        assert any(i.kind is LineKind.MINIMUM for i in bill.line_items)
        assert bill.total_usd >= minimum

    def test_addons_respect_the_included_allowance(self, meter: UsageMeter, usage_store) -> None:  # noqa: ANN001
        # 150 GB ingested, 100 GB included on Team → 50 GB billable at $0.02.
        meter.record(Meter.INGEST, 150.0)
        bill = PricingEngine().price(usage_store.summarize("ten_test"), Plan.TEAM)
        addon = next(i for i in bill.line_items if i.kind is LineKind.ADDON)
        assert addon.quantity == pytest.approx(50.0)
        assert addon.amount_usd == pytest.approx(1.0)

    def test_commitment_discounts_only_clara_revenue(self, meter: UsageMeter, usage_store) -> None:  # noqa: ANN001
        # Large enough that the platform fee clears the plan minimum, otherwise
        # the floor absorbs the discount (see the test below).
        meter.record(Meter.COMPUTE, 8_000_000)
        summary = usage_store.summarize("ten_test")
        engine = PricingEngine()

        plain = engine.price(summary, Plan.BUSINESS)
        committed = engine.price(summary, Plan.BUSINESS, commitment_term_months=36)

        assert plain.platform_fee_usd > get_rate_card().plan(Plan.BUSINESS).monthly_minimum_usd
        assert committed.total_usd < plain.total_usd
        # Infrastructure is already at cost, so it is never discounted.
        assert committed.infrastructure_usd == pytest.approx(plain.infrastructure_usd)

    def test_minimum_absorbs_discounts_below_the_floor(
        self, meter: UsageMeter, usage_store
    ) -> None:  # noqa: ANN001
        """A commitment cannot take a bill below the plan's monthly minimum.

        The minimum is a floor, not a line item that discounts apply after, so a
        small customer on a 3-year commitment still pays the minimum. Asserted
        explicitly because it is a deliberate commercial choice, not a bug.
        """
        _load(meter, queries=10, seconds=5.0)
        summary = usage_store.summarize("ten_test")
        engine = PricingEngine()
        minimum = get_rate_card().plan(Plan.BUSINESS).monthly_minimum_usd

        committed = engine.price(summary, Plan.BUSINESS, commitment_term_months=36)
        assert committed.platform_fee_usd >= minimum
        assert committed.total_usd >= minimum

    def test_credits_reduce_the_total(self, meter: UsageMeter, usage_store) -> None:  # noqa: ANN001
        _load(meter, queries=200, seconds=30.0)
        summary = usage_store.summarize("ten_test")
        engine = PricingEngine()
        without = engine.price(summary, Plan.TEAM)
        with_credit = engine.price(summary, Plan.TEAM, credits_balance_usd=50.0)
        assert with_credit.total_usd == pytest.approx(max(0.0, without.total_usd - 50.0))

    def test_total_never_goes_negative(self, meter: UsageMeter, usage_store) -> None:  # noqa: ANN001
        meter.record(Meter.COMPUTE, 1.0)
        bill = PricingEngine().price(
            usage_store.summarize("ten_test"), Plan.TEAM, credits_balance_usd=1_000_000.0
        )
        assert bill.total_usd == 0.0

    def test_efficiency_dividend_is_capped_at_the_platform_fee(
        self, meter: UsageMeter, usage_store
    ) -> None:  # noqa: ANN001
        _load(meter, queries=200, seconds=20.0)
        summary = usage_store.summarize("ten_test")
        engine = PricingEngine()

        baseline = EfficiencyBaseline(ccu_minutes_per_gb_scanned=100.0)
        bill = engine.price(summary, Plan.TEAM, baseline=baseline, gb_scanned=1_000.0)
        fee = next(i for i in bill.line_items if i.kind is LineKind.SUCCESS_FEE)

        plain = engine.price(summary, Plan.TEAM)
        assert fee.amount_usd <= plain.platform_fee_usd + 1e-9, (
            "the success fee must never make the bill exceed the unoptimised one"
        )

    def test_no_dividend_when_efficiency_got_worse(self, meter: UsageMeter, usage_store) -> None:  # noqa: ANN001
        _load(meter, queries=200, seconds=20.0)
        summary = usage_store.summarize("ten_test")
        # A baseline below actual usage means no saving: charge nothing.
        bill = PricingEngine().price(
            summary,
            Plan.TEAM,
            baseline=EfficiencyBaseline(ccu_minutes_per_gb_scanned=0.001),
            gb_scanned=10.0,
        )
        assert not any(i.kind is LineKind.SUCCESS_FEE for i in bill.line_items)

    def test_storage_and_egress_carry_no_markup_by_default(self) -> None:
        card = RateCard()
        assert card.storage_markup == 0.0
        assert card.egress_markup == 0.0

    def test_plan_comparison_is_ordered_by_cost(self, meter: UsageMeter, usage_store) -> None:  # noqa: ANN001
        _load(meter, queries=100, seconds=15.0)
        rows = PricingEngine().compare_plans(usage_store.summarize("ten_test"))
        assert [r["total_usd"] for r in rows] == sorted(r["total_usd"] for r in rows)

    def test_query_cost_estimate_scales_with_usage(self, aws) -> None:  # noqa: ANN001
        engine = PricingEngine()
        cheap = engine.estimate_query_cost(ccu_minutes=1.0, provider=aws, plan=Plan.TEAM)
        dear = engine.estimate_query_cost(ccu_minutes=100.0, provider=aws, plan=Plan.TEAM)
        assert dear["total_usd"] > cheap["total_usd"]
        assert cheap["infrastructure_usd"] > 0

    def test_spot_capacity_is_cheaper(self, aws) -> None:  # noqa: ANN001
        engine = PricingEngine()
        on_demand = engine.estimate_query_cost(ccu_minutes=100, provider=aws, plan=Plan.TEAM)
        spot = engine.estimate_query_cost(
            ccu_minutes=100, provider=aws, plan=Plan.TEAM, spot=True
        )
        assert spot["total_usd"] < on_demand["total_usd"]


class TestQuotas:
    def test_trial_limits_derive_from_provider_free_tier(self, settings: Settings) -> None:
        store = InMemoryUsageStore()
        aws_limits = QuotaEngine(store, build_provider(settings, "aws")).effective_limits(
            Plan.TRIAL
        )
        tencent_limits = QuotaEngine(store, build_provider(settings, "tencent")).effective_limits(
            Plan.TRIAL
        )

        # AWS gives 750 h/month of a 2 vCPU / 1 GB instance; Tencent gives no
        # free compute hours at all. The trial must reflect each honestly.
        assert aws_limits.daily_ccu_minutes and aws_limits.daily_ccu_minutes > 0
        assert tencent_limits.daily_ccu_minutes == 0.0
        assert tencent_limits.storage_gb == 50.0

    def test_local_provider_is_unlimited(self, settings: Settings) -> None:
        limits = QuotaEngine(
            InMemoryUsageStore(), build_provider(settings, "local")
        ).effective_limits(Plan.TRIAL)
        assert limits.daily_ccu_minutes is None
        assert limits.storage_gb is None

    def test_paid_plans_are_not_free_tier_bound(self, settings: Settings) -> None:
        limits = QuotaEngine(
            InMemoryUsageStore(), build_provider(settings, "aws")
        ).effective_limits(Plan.TEAM)
        assert limits.daily_ccu_minutes is None

    def test_compute_quota_blocks_when_exhausted(self, settings: Settings) -> None:
        store = InMemoryUsageStore()
        provider = build_provider(settings, "aws")
        quotas = QuotaEngine(store, provider)
        limit = quotas.effective_limits(Plan.TRIAL).daily_ccu_minutes

        meter = UsageMeter(provider, store, tenant_id="ten_test")
        meter.record(Meter.COMPUTE, limit + 1)

        check = quotas.check_compute("ten_test", Plan.TRIAL, ccu_minutes=0.0)
        assert not check.allowed
        with pytest.raises(QuotaExceededError):
            check.raise_if_denied()

    def test_warehouse_size_limited_on_trial(self, settings: Settings) -> None:
        quotas = QuotaEngine(InMemoryUsageStore(), build_provider(settings, "aws"))
        assert quotas.check_warehouse_size(Plan.TRIAL, WarehouseSize.XS).allowed
        assert not quotas.check_warehouse_size(Plan.TRIAL, WarehouseSize.XL).allowed
        assert quotas.check_warehouse_size(Plan.TEAM, WarehouseSize.XXXXL).allowed

    def test_warehouse_count_limited_on_trial(self, settings: Settings) -> None:
        quotas = QuotaEngine(InMemoryUsageStore(), build_provider(settings, "aws"))
        assert quotas.check_warehouse_count(Plan.TRIAL, 0).allowed
        assert not quotas.check_warehouse_count(Plan.TRIAL, 1).allowed

    def test_budget_cap_is_enforced(self, settings: Settings) -> None:
        store = InMemoryUsageStore()
        provider = build_provider(settings, "aws")
        meter = UsageMeter(provider, store, tenant_id="ten_test")
        _load(meter, queries=500, seconds=60.0)

        quotas = QuotaEngine(store, provider, budget_cap_usd=1.0)
        with pytest.raises(BudgetExceededError):
            quotas.check_budget("ten_test", Plan.TEAM)

    def test_no_cap_means_no_budget_check(self, settings: Settings) -> None:
        quotas = QuotaEngine(InMemoryUsageStore(), build_provider(settings, "aws"))
        quotas.check_budget("ten_test", Plan.TEAM)  # must not raise


class TestBilling:
    def test_prepayment_earns_a_bonus(self) -> None:
        ledger = CreditLedger()
        small = ledger.purchase("ten_a", 1_000.0)
        large = ledger.purchase("ten_b", 100_000.0)
        assert small.bonus_usd == 0.0
        assert large.bonus_usd == pytest.approx(15_000.0)
        assert ledger.balance("ten_b") == pytest.approx(115_000.0)

    def test_credits_consume_oldest_grant_first(self) -> None:
        ledger = CreditLedger()
        first = ledger.purchase("ten_a", 100.0, expires_in_days=30)
        second = ledger.purchase("ten_a", 100.0, expires_in_days=365)
        ledger.consume("ten_a", 120.0)
        # The sooner-expiring grant must be spent first.
        assert first.remaining_usd == 0.0
        assert second.remaining_usd == pytest.approx(80.0)

    def test_consume_is_bounded_by_balance(self) -> None:
        ledger = CreditLedger()
        ledger.purchase("ten_a", 50.0)
        assert ledger.consume("ten_a", 500.0) == pytest.approx(50.0)
        assert ledger.balance("ten_a") == 0.0

    def test_invoice_renders_the_three_layers(self, meter: UsageMeter, usage_store) -> None:  # noqa: ANN001
        _load(meter, queries=300, seconds=30.0)
        meter.record(Meter.INGEST, 400.0)
        invoice = BillingService(usage_store).generate("ten_test", Plan.TEAM)
        text = invoice.render_text()

        assert "INFRASTRUCTURE (at cost, no markup)" in text
        assert "CLARA PLATFORM FEE" in text
        assert "TOTAL DUE" in text
        assert invoice.number.startswith("CLARA-")

    def test_invoice_lifecycle(self, usage_store) -> None:  # noqa: ANN001
        invoice = BillingService(usage_store).generate("ten_test", Plan.TRIAL)
        assert invoice.status.value == "draft"
        invoice.issue()
        assert invoice.status.value == "issued" and invoice.due_at is not None
        invoice.mark_paid()
        assert invoice.status.value == "paid"

    def test_paid_invoice_cannot_be_voided(self, usage_store) -> None:  # noqa: ANN001
        invoice = BillingService(usage_store).generate("ten_test", Plan.TRIAL)
        invoice.issue().mark_paid()
        with pytest.raises(Exception, match="paid"):
            invoice.void()

    def test_forecast_extrapolates_from_usage(self, meter: UsageMeter, usage_store) -> None:  # noqa: ANN001
        _load(meter, queries=100, seconds=20.0)
        forecast = BillingService(usage_store).forecast("ten_test", Plan.TEAM)
        assert forecast["projected_usd"] >= forecast["to_date_usd"]
        assert 0 < forecast["elapsed_fraction"] <= 1


class TestCompetitorComparison:
    def test_clara_is_cheaper_and_cheapest_on_commodity_hardware(
        self, settings: Settings
    ) -> None:
        rates = {}
        for name in ("aws", "tencent", "generic"):
            provider = build_provider(settings, name)
            data = competitor_comparison(provider, Plan.TEAM, monthly_ccu_hours=1_000)
            rates[name] = data["clara"]["all_in_usd_per_ccu_hour"]
            for row in data["competitors"]:
                assert row["clara_is_cheaper_by"] > 1.0, (
                    f"Clara should undercut {row['platform']} on {name}"
                )

        # The cloud-agnostic argument: commodity providers are far cheaper.
        assert rates["generic"] < rates["tencent"] < rates["aws"]

    def test_states_its_assumptions(self, aws) -> None:  # noqa: ANN001
        data = competitor_comparison(aws, Plan.TEAM)
        assert "approximate" in data["assumptions"].lower()
