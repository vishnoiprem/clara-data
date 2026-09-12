"""Invoicing and prepaid credits.

Turns a priced period into an invoice a finance team can read, and tracks the
prepaid credit balance that commitments buy. Credits expire; grants are consumed
oldest-first so a customer never loses a balance they could have used.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any

from clara.errors import ValidationError
from clara.ids import Prefix, new_id
from clara.logging_setup import get_logger
from clara.metering.pricing import Bill, LineKind, PricingEngine
from clara.metering.rates import Plan, RateCard, get_rate_card
from clara.metering.store import UsageStore
from clara.time_utils import month_bounds, to_iso, utcnow

log = get_logger(__name__)


class InvoiceStatus(str, Enum):
    DRAFT = "draft"
    ISSUED = "issued"
    PAID = "paid"
    VOID = "void"


@dataclass
class CreditGrant:
    """A block of prepaid credit.

    Prepayment earns a bonus (see ``RateCard.credit_bonus_tiers``) — the
    customer gets more credit than cash paid, and Clara gets predictable
    revenue. Bonus credit is tracked separately so it can be reported honestly
    as a discount rather than as revenue.
    """

    tenant_id: str
    purchased_usd: float
    id: str = field(default_factory=lambda: new_id(Prefix.CREDIT_GRANT))
    bonus_usd: float = 0.0
    granted_at: datetime = field(default_factory=utcnow)
    expires_at: datetime | None = None
    term_months: int = 0
    consumed_usd: float = 0.0
    note: str = ""

    @property
    def total_usd(self) -> float:
        return self.purchased_usd + self.bonus_usd

    @property
    def remaining_usd(self) -> float:
        return max(0.0, self.total_usd - self.consumed_usd)

    def is_active(self, now: datetime | None = None) -> bool:
        if self.remaining_usd <= 0:
            return False
        return self.expires_at is None or (now or utcnow()) < self.expires_at

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "tenant_id": self.tenant_id,
            "purchased_usd": round(self.purchased_usd, 2),
            "bonus_usd": round(self.bonus_usd, 2),
            "total_usd": round(self.total_usd, 2),
            "consumed_usd": round(self.consumed_usd, 2),
            "remaining_usd": round(self.remaining_usd, 2),
            "term_months": self.term_months,
            "granted_at": to_iso(self.granted_at),
            "expires_at": to_iso(self.expires_at) if self.expires_at else None,
            "active": self.is_active(),
            "note": self.note,
        }


class CreditLedger:
    """In-process credit ledger.

    The control plane persists grants in its database and uses this class for
    the arithmetic, so the consumption rules live in exactly one place.
    """

    def __init__(self, rate_card: RateCard | None = None) -> None:
        self.rate_card = rate_card or get_rate_card()
        self._grants: dict[str, list[CreditGrant]] = {}

    def purchase(
        self,
        tenant_id: str,
        amount_usd: float,
        *,
        term_months: int = 0,
        expires_in_days: int | None = 365,
        note: str = "",
    ) -> CreditGrant:
        """Buy credit, applying the prepayment bonus for the purchase size."""
        if amount_usd <= 0:
            raise ValidationError("credit purchase must be positive")

        bonus_rate = self.rate_card.credit_bonus(amount_usd)
        grant = CreditGrant(
            tenant_id=tenant_id,
            purchased_usd=amount_usd,
            bonus_usd=amount_usd * bonus_rate,
            term_months=term_months,
            expires_at=utcnow() + timedelta(days=expires_in_days) if expires_in_days else None,
            note=note or (f"{bonus_rate:.0%} prepayment bonus" if bonus_rate else ""),
        )
        self._grants.setdefault(tenant_id, []).append(grant)
        log.info(
            "credit purchased",
            extra={
                "tenant": tenant_id,
                "purchased_usd": amount_usd,
                "bonus_usd": round(grant.bonus_usd, 2),
            },
        )
        return grant

    def grants(self, tenant_id: str, *, active_only: bool = True) -> list[CreditGrant]:
        grants = self._grants.get(tenant_id, [])
        if active_only:
            grants = [g for g in grants if g.is_active()]
        # Oldest first: expiring credit is spent before fresh credit.
        return sorted(grants, key=lambda g: (g.expires_at or datetime.max.replace(tzinfo=g.granted_at.tzinfo), g.granted_at))

    def balance(self, tenant_id: str) -> float:
        return sum(g.remaining_usd for g in self.grants(tenant_id))

    def consume(self, tenant_id: str, amount_usd: float) -> float:
        """Spend credit, oldest grant first. Returns the amount actually consumed."""
        remaining = amount_usd
        consumed = 0.0
        for grant in self.grants(tenant_id):
            if remaining <= 0:
                break
            take = min(grant.remaining_usd, remaining)
            grant.consumed_usd += take
            consumed += take
            remaining -= take
        return consumed


@dataclass
class Invoice:
    """An issued bill."""

    bill: Bill
    id: str = field(default_factory=lambda: new_id(Prefix.INVOICE))
    number: str = ""
    status: InvoiceStatus = InvoiceStatus.DRAFT
    issued_at: datetime | None = None
    due_at: datetime | None = None
    #: Credit consumed against this invoice.
    credits_applied_usd: float = 0.0

    def __post_init__(self) -> None:
        if not self.number:
            stamp = self.bill.period_start.strftime("%Y%m")
            self.number = f"CLARA-{stamp}-{self.id.split('_')[-1][:8]}"

    @property
    def amount_due_usd(self) -> float:
        return max(0.0, self.bill.total_usd)

    def issue(self, *, net_days: int = 30) -> Invoice:
        self.status = InvoiceStatus.ISSUED
        self.issued_at = utcnow()
        self.due_at = self.issued_at + timedelta(days=net_days)
        return self

    def mark_paid(self) -> Invoice:
        if self.status is InvoiceStatus.VOID:
            raise ValidationError("cannot pay a voided invoice")
        self.status = InvoiceStatus.PAID
        return self

    def void(self) -> Invoice:
        if self.status is InvoiceStatus.PAID:
            raise ValidationError("cannot void a paid invoice")
        self.status = InvoiceStatus.VOID
        return self

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "number": self.number,
            "status": self.status.value,
            "issued_at": to_iso(self.issued_at) if self.issued_at else None,
            "due_at": to_iso(self.due_at) if self.due_at else None,
            "credits_applied_usd": round(self.credits_applied_usd, 2),
            "amount_due_usd": round(self.amount_due_usd, 2),
            "bill": self.bill.to_dict(),
        }

    def render_text(self) -> str:
        """Plain-text invoice.

        Grouped by the three pricing layers, because the whole argument for
        cost-plus pricing is lost if the invoice hides which layer a charge
        belongs to.
        """
        bill = self.bill
        width = 72
        lines = [
            "=" * width,
            f"CLARA DATA — INVOICE {self.number}".center(width),
            "=" * width,
            f"Tenant:  {bill.tenant_id}",
            f"Plan:    {bill.plan.value}",
            f"Period:  {bill.period_start:%Y-%m-%d} to {bill.period_end:%Y-%m-%d}",
            f"Status:  {self.status.value}",
            "-" * width,
        ]

        groups = [
            (LineKind.INFRASTRUCTURE, "INFRASTRUCTURE (at cost, no markup)"),
            (LineKind.PLATFORM_FEE, "CLARA PLATFORM FEE"),
            (LineKind.MINIMUM, "PLAN MINIMUM"),
            (LineKind.ADDON, "MANAGED SERVICES"),
            (LineKind.SUCCESS_FEE, "OPTIMISER SUCCESS FEE"),
            (LineKind.DISCOUNT, "DISCOUNTS AND CREDITS"),
        ]
        for kind, heading in groups:
            items = [i for i in bill.line_items if i.kind is kind]
            if not items:
                continue
            lines.append(heading)
            for item in items:
                amount = f"${item.amount_usd:>12,.2f}"
                lines.append(f"  {item.description[:48]:<48}{amount}")
                if item.quantity:
                    lines.append(
                        f"    {item.quantity:,.2f} {item.unit} @ ${item.unit_price_usd:,.6f}"
                    )
                if item.detail:
                    lines.append(f"    ({item.detail})")
            lines.append("")

        lines.extend(
            [
                "-" * width,
                f"  {'Subtotal':<48}${bill.subtotal_usd:>12,.2f}",
                f"  {'Discounts and credits':<48}${bill.discounts_usd:>12,.2f}",
                f"  {'TOTAL DUE':<48}${bill.total_usd:>12,.2f}",
                "=" * width,
                f"For every $1.00 of infrastructure you used, you paid "
                f"${bill.effective_markup:,.2f}.",
                "Infrastructure cost is passed through at the provider's own rates.",
                "=" * width,
            ]
        )
        return "\n".join(lines)


class BillingService:
    """Generates invoices from recorded usage."""

    def __init__(
        self,
        store: UsageStore,
        *,
        rate_card: RateCard | None = None,
        ledger: CreditLedger | None = None,
    ) -> None:
        self.store = store
        self.rate_card = rate_card or get_rate_card()
        self.pricing = PricingEngine(self.rate_card)
        self.ledger = ledger or CreditLedger(self.rate_card)

    def generate(
        self,
        tenant_id: str,
        plan: Plan | str,
        *,
        period: datetime | None = None,
        commitment_term_months: int = 0,
        apply_credits: bool = True,
    ) -> Invoice:
        """Price and invoice one calendar month."""
        start, end = month_bounds(period)
        summary = self.store.summarize(tenant_id, start=start, end=end)
        balance = self.ledger.balance(tenant_id) if apply_credits else 0.0

        bill = self.pricing.price(
            summary,
            plan,
            commitment_term_months=commitment_term_months,
            credits_balance_usd=balance,
        )
        invoice = Invoice(bill=bill)

        if apply_credits and balance > 0:
            # Credit consumed is whatever the pricing engine actually applied.
            applied = -sum(
                i.amount_usd
                for i in bill.line_items
                if i.kind is LineKind.DISCOUNT and "credits" in i.description.lower()
            )
            invoice.credits_applied_usd = self.ledger.consume(tenant_id, applied)
        return invoice

    def forecast(self, tenant_id: str, plan: Plan | str) -> dict[str, Any]:
        """Project the current month's total from usage so far.

        Straight-line extrapolation, which is honest for steady workloads and
        clearly labelled for spiky ones. Shown in the console so the month-end
        number is never a surprise.
        """
        start, end = month_bounds()
        now = utcnow()
        summary = self.store.summarize(tenant_id, start=start, end=end)
        bill = self.pricing.price(summary, plan)

        elapsed = max((now - start).total_seconds(), 1.0)
        total = (end - start).total_seconds()
        ratio = total / elapsed

        return {
            "period_start": to_iso(start),
            "period_end": to_iso(end),
            "elapsed_fraction": round(elapsed / total, 4),
            "to_date_usd": round(bill.total_usd, 2),
            "projected_usd": round(bill.total_usd * ratio, 2),
            "projection_method": "straight-line extrapolation of usage to date",
            "infrastructure_to_date_usd": round(bill.infrastructure_usd, 2),
            "platform_fee_to_date_usd": round(bill.platform_fee_usd, 2),
        }
