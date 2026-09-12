# Pricing, with worked examples

Referenced from `clara.metering.rates`. This document is the commercial model in
prose; the code in that module is the authority.

## The unit

> **1 CCU** (Clara Compute Unit) = 1 vCPU + 4 GB RAM, for 1 minute.

Databricks' DBU and Snowflake's credit are deliberately opaque — you cannot
derive either from hardware, so you cannot check the bill. Clara's unit is
defined against real resources, which means a customer can compute their own
infrastructure cost and verify the platform fee on top of it. That verifiability
is the foundation of everything below.

Warehouse sizes keep a 1:4 vCPU:RAM ratio, so **CCU/minute equals total vCPUs**:

| Size | Nodes | vCPU | RAM | CCU/min | AWS infra $/hr | Hetzner-class $/hr |
|---|---|---|---|---|---|---|
| XS | 1 | 2 | 8 GB | 2 | $0.096 | $0.011 |
| S | 1 | 4 | 16 GB | 4 | $0.192 | $0.023 |
| M | 2 | 8 | 32 GB | 8 | $0.384 | $0.046 |
| L | 4 | 16 | 64 GB | 16 | $0.768 | $0.091 |
| XL | 8 | 32 | 128 GB | 32 | $1.536 | $0.183 |
| 2XL–4XL | 16–64 | 64–256 | 256 GB–1 TB | 64–256 | … | … |

## The three layers

Every invoice is grouped by layer, because the argument for cost-plus pricing is
lost if a customer cannot see which layer a charge belongs to.

### Layer 1 — infrastructure, at cost

Metered quantities × the provider's own unit rates. **Zero markup.** Storage and
egress included: charging rent on data the customer already owns, in an open
format, in their own bucket, is not defensible.

Meters: compute (CCU-min), storage (GB-month, sampled hourly and prorated),
ingest (GB), orchestration (task-min), egress (GB), API calls (free).

### Layer 2 — platform fee

A declining percentage **of layer 1**. Bands are marginal, like income tax, so
crossing a threshold never re-rates the spend below it.

Team plan:

| Monthly infra spend | Marginal rate |
|---|---|
| $0 – $2,000 | 30% |
| $2,000 – $10,000 | 22% |
| $10,000 – $50,000 | 15% |
| above $50,000 | 10% |

A customer at $30,000/month of infrastructure pays
`2,000×0.30 + 8,000×0.22 + 20,000×0.15 = $5,360`, a blended **17.9%** — not 15%
on the whole amount, and not 30%.

### Layer 3 — add-ons

Per-unit, optional, itemised: managed connector ingest above the plan's included
allowance, premium support. All avoidable by self-hosting.

## Worked example

A mid-sized retailer on Team, hosted on Tencent Cloud:

- 3 TB in the lakehouse
- an M warehouse averaging 5 hours/day of real query time
- 400 GB/month ingested from Postgres and a partner CSV feed
- nightly transforms

```
Compute      8 CCU/min × 60 × 5 h × 30 d  = 72,000 CCU-min  × $0.000515  = $37.08
Storage      3,000 GB-month               × $0.0138                     = $41.40
Ingest       400 GB                       × $0.000012                   =  $0.005
Orchestration 3,000 task-min              × $0.00044                    =  $1.32
                                                        infrastructure  = $79.81
Platform fee  30% of $79.81                                             = $23.94
Add-ons       300 GB over the 100 GB included × $0.02                   =  $6.00
                                                        usage subtotal  = $29.94
Plan minimum  $99 floor, so topped up by                                 $69.06
                                                        TOTAL           = $178.87
```

Two honest observations about this bill:

1. **The $99 minimum dominates.** At this size the customer pays the floor, not
   the percentage. That is deliberate — predictable revenue per account — but it
   means Clara is *not* compelling below roughly $350/month of infrastructure.
   Those customers should self-host on Community, and the pricing page should
   say so rather than pretending otherwise.
2. **Storage is half the infrastructure cost**, and storage cannot be
   auto-suspended. Compaction and snapshot expiry are therefore the highest
   leverage cost control on the platform, which is why they are on by default.

The same workload on AWS costs $141 of infrastructure instead of $80, and the
platform fee scales with it. The provider choice is the customer's largest
lever, which is why `clara provider list` exists.

## The efficiency dividend

The mechanism that distinguishes this from a reseller margin.

Clara measures a workload's efficiency as CCU-minutes per GB scanned, over a
trailing 30 days. When the optimiser improves it — better partitioning, engine
routing, compaction, a rewritten plan — the saving is split **70% to the
customer, 30% to Clara**, and Clara's share is **capped at the platform fee**.

Consequences, all intended:

- If Clara makes a workload cheaper, both parties gain.
- If Clara makes it *worse*, nothing is charged: the mechanism is
  one-directional.
- The total bill can never exceed what it would have been unoptimised, because
  of the cap.
- Normalising by GB scanned means a customer who simply grows does not look like
  a regression, and Clara is not paid for a workload that shrank on its own.

Databricks and Snowflake earn more when queries are slow. This inverts that
incentive, and it is the clearest argument a salesperson has.

## Commitments and credits

- **Prepaid credits** earn a bonus: 5% at $10k, 10% at $50k, 15% at $100k.
- **Commitment discounts** — 15% for 12 months, 25% for 36 — apply to the
  platform fee and add-ons only. Infrastructure is already at cost, so there is
  nothing to discount.
- **The plan minimum is a floor, not a line item.** A small customer on a 3-year
  commitment still pays the minimum. Asserted explicitly in the test suite
  because it is a commercial choice that would otherwise look like a bug.

## The trial

Requirement: *the trial should be limited to what the cloud providers give free.*

Implemented literally — trial limits are **derived at runtime from the
configured provider's free tier**, not copied into a constant
(`clara.metering.quotas`):

| Provider | Free compute | Trial daily CCU-min | Trial storage |
|---|---|---|---|
| AWS | 750 h/mo t3.micro (12 mo) | 375 | 5 GB |
| GCP | e2-micro, always free | 372 | 5 GB |
| Azure | 750 h/mo B1S (12 mo) | 375 | 5 GB |
| Tencent | none | 0 | 50 GB |
| Local | unmetered | unlimited | unlimited |

Two consequences worth stating plainly:

- A trial **costs the operator nothing to host**, so it does not need an
  artificial 14-day fuse. On GCP's always-free tier it can run indefinitely.
- On Tencent the trial honestly offers storage but **no warehouse allowance**,
  because Tencent gives no free compute hours. Pretending otherwise would mean
  Clara subsidising trials on that provider.

The monthly allowance is spread evenly across the month rather than offered as
one pool, so a single heavy day cannot exhaust the month and leave the trial
dead for three weeks.

## Budget caps

`CLARA_BUDGET_CAP` is a **hard** cap. Clara refuses work that would breach it,
rather than emailing an alert after the fact. The most common complaint about
usage-based data platforms is a surprise invoice, and a cap that actually stops
is the only fix that works.

## Plans

| Plan | Fee | Minimum | Limits | Support |
|---|---|---|---|---|
| Trial | none | — | Provider free tier; 1 XS warehouse; 2 concurrent queries | Community forum |
| Community | **none, forever** | — | None | Community forum |
| Team | 30% → 10% | $99 | 20 concurrent queries | Email, next business day |
| Business | 20% → 9% | $999 | 100 concurrent queries | Priority, 4 h |
| Enterprise | 8% | $4,999 | Negotiated | Dedicated engineer, 1 h |

**Community is free at any scale, with no feature gates.** The platform fee
applies only to Clara-operated infrastructure. This is what makes the open-source
promise real rather than a trial funnel — and it means the product has to be
worth paying for on operational merit.

## Regional and partner pricing

Rate cards are named and loadable from YAML (`load_rate_card_file`), so an
operator can run regional or partner pricing via `CLARA_RATE_CARD=partner_apac`
without forking the code.
