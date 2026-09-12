# Clara Data

An open, multi-cloud lakehouse platform — Databricks/Snowflake capability on
commodity infrastructure, for companies that cannot justify their pricing.

Clara assembles proven open-source engines (Apache Iceberg, Trino, DuckDB) into
one managed surface: connect a source, land it in open tables, transform it in
SQL, schedule it, and see exactly what it cost. It runs on any S3-compatible
provider — AWS, Azure, GCP, Tencent, Alibaba, Hetzner, or a laptop.

```
Sources ──▶ Ingest ──▶ Iceberg tables ──▶ SQL models ──▶ Tables your BI reads
            (raw)      on your storage    (analytics)
                              │
                    Trino (scale-out) / DuckDB (single-node), routed per query
```

---

## Quickstart

No cloud account, no credentials, no Docker. Python 3.10+.

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -e ".[duckdb]"

clara init          # writes a starter clara.yaml
clara run           # ingest → transform → test → maintain
clara serve         # web console at http://localhost:8080
```

`clara run` prints what it did:

```
┏━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━┳━━━━━━━━━━━┳━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃ task               ┃ kind        ┃ status    ┃ duration ┃ detail                   ┃
┡━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━╇━━━━━━━━━━━╇━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━┩
│ ingest:retail      │ ingest      │ succeeded │ 210ms    │ 2,005 rows → customers,… │
│ model:order_facts  │ transform   │ succeeded │ 251ms    │ 2,000 rows → analytics.… │
│ model:category_mix │ transform   │ succeeded │ 251ms    │                          │
│ model:daily_revenue│ transform   │ succeeded │ 251ms    │ 243 rows → analytics.…   │
│ maintenance        │ maintenance │ succeeded │ 0ms      │                          │
└────────────────────┴─────────────┴───────────┴──────────┴──────────────────────────┘

succeeded — 2,005 rows synced, 3 model(s) built in 253ms
```

Or with `make`:

```bash
make demo     # the run above, end to end
make serve    # API + console
make test     # the test suite
make stack    # full containerised stack: MinIO + Iceberg + Trino + Clara
```

---

## Building a pipeline in the web console

`clara serve` opens a six-step builder. This is the "no data engineer" path —
nothing below requires writing a config file by hand.

**1 · Connect source**
Pick a connector. Its configuration form is generated from the connector's own
JSON schema, so there is nothing to look up. **Test connection** verifies
credentials before you go further.

**2 · Choose data**
Clara discovers the available streams with their columns, primary keys and
usable cursor columns. Per stream, choose:

| Setting | Meaning |
|---|---|
| `full refresh` | Reload everything each run. Simple and always correct. |
| `incremental` | Only rows newer than the last checkpoint. |
| cursor column | What "newer" means — usually `updated_at`. |
| primary key | Enables upserts, so re-runs cannot duplicate rows. |

**Preview rows** shows real data from the source before you commit.

**3 · Raw tables**
Shows the tables Clara will create (`raw.orders`, `raw.customers`), their write
mode and their keys. Schemas come from the source; columns added later are
picked up automatically rather than breaking the sync.

**4 · Transform**
Write SQL against your raw tables:

```sql
SELECT c.country,
       count(*)               AS orders,
       round(sum(o.amount),2) AS revenue
FROM {{ source('raw', 'orders') }} o
JOIN {{ source('raw', 'customers') }} c USING (customer_id)
GROUP BY c.country
```

`{{ source('ns','table') }}` references an ingested table and
`{{ ref('model') }}` another model — dependencies come from the SQL, so you
never declare a DAG. **Preview result** runs it and shows the output rows
*before* the table exists. **Suggest a model** drafts one from your schema.

Choose how it is stored: `table` (rebuilt each run), `view` (computed on read),
or `incremental` (merged on a key).

**5 · Schedule**
Project name, cron schedule, warehouse size. Warehouses auto-suspend after 60 s
idle, so nothing is charged between runs.

**6 · Review & run**
Shows the execution plan — which tasks run, and which run concurrently. **Save
pipeline** writes `clara.yaml`; **Save & run now** also executes it and streams
per-task progress.

The console's other tabs: **Data** browses tables and rows, **SQL** is a query
editor showing which engine ran each query and what it cost, **Runs** is run
history with per-task logs, **Cost** shows usage, a projected invoice, and a
provider cost comparison.

> The console writes the same `clara.yaml` the CLI reads. A pipeline built by
> clicking is a file you can review, diff and commit — there is no export step
> and no hidden state.

---

## The same thing from the CLI

```bash
clara pipeline sources                    # available connectors
clara pipeline test sample --config '{"orders": 500}'
clara validate                            # check clara.yaml
clara plan                                # what would run, in order
clara run                                 # run it
clara run --select model:daily_revenue    # just one model (+ its parents)
clara run --full-refresh                  # rebuild incrementals

clara table list
clara table show analytics.daily_revenue
clara query "SELECT * FROM analytics.daily_revenue LIMIT 5"

clara cost usage                          # usage by meter
clara cost invoice                        # itemised invoice
clara cost compare --ccu-hours 1000       # vs Snowflake/Databricks/BigQuery
clara provider list                       # cost per provider
```

Everything the console does is a REST call, so all of it is scriptable. API docs
are at `/api/docs` when the server is running.

---

## `clara.yaml`

The whole platform as one file:

```yaml
version: 1
project: retail_demo

defaults:
  raw_namespace: raw
  analytics_namespace: analytics

warehouses:
  - name: default
    size: xs                      # xs → 4xl; each step doubles capacity
    auto_suspend_seconds: 60      # idle compute is the biggest avoidable cost

sources:
  - name: retail
    connector: sample             # or postgres, http_file, …
    config:
      orders: 2000
    streams:
      - name: customers
      - name: orders
        sync_mode: incremental
        cursor_field: ordered_at
        primary_key: order_id

models:
  - name: order_facts
    materialization: table
    sql: |
      SELECT o.*, c.country
      FROM {{ source('raw', 'orders') }} o
      LEFT JOIN {{ source('raw', 'customers') }} c USING (customer_id)
    tests:
      - type: not_null
        column: order_id
      - type: unique
        column: order_id

  - name: daily_revenue
    materialization: table
    sql: |
      SELECT order_date, country, round(sum(amount), 2) AS revenue
      FROM {{ ref('order_facts') }}
      GROUP BY order_date, country

maintenance:
  enabled: true                   # compaction + snapshot expiry, nightly

schedule: "0 2 * * *"             # or @daily, "every 15 minutes"
```

Secrets belong in the environment and are referenced as `${VAR}` or
`${VAR:-default}`, so the file is safe to commit.

Data tests (`not_null`, `unique`, `accepted_values`, `sql`) run after each model
builds and fail the run — which is what lets a business trust a table without a
data team reviewing it.

---

## How it works

| Layer | Choice | Why |
|---|---|---|
| Table format | **Apache Iceberg** | Open standard. Your data stays in your bucket as Parquet, readable by Trino, Spark, DuckDB, Flink and ClickHouse. Leaving Clara is not a migration. |
| Storage | **any S3-compatible** | AWS S3, Azure Blob, GCS, Tencent COS, Alibaba OSS, R2, B2, MinIO. Only the endpoint changes. |
| Scale-out SQL | **Trino** | Instant startup, ANSI SQL, small per-query memory footprint, federation. Cheaper than Spark on modest hardware. |
| Single-node SQL | **DuckDB** | Most analytical queries are small. Sub-second start, no shuffle, no cluster. |
| Catalog | **Iceberg REST** | Lakekeeper, Polaris, Nessie, Unity OSS, Glue — or SQLite locally. |
| Ingest | **Airbyte-protocol compatible** | Native Python connectors for speed; existing Airbyte images work through the same runner. |
| Transform | **SQL, dbt-compatible** | `{{ ref }}`/`{{ source }}` authoring without requiring dbt. Point Clara at an existing dbt project if you have one. |
| Orchestration | **built in** | Ingest → transform → maintenance as one DAG, with cron. |

**Query routing is automatic.** Clara estimates the scan from Iceberg's own
statistics (free to read) and sends small queries to DuckDB and large ones to
Trino. Every decision carries a reason, visible in query history:

```
engine: duckdb | estimated scan 1.2GB fits single-node
engine: trino  | estimated scan 340.0GB exceeds single-node limit
```

**Maintenance is not optional.** Nightly compaction and snapshot expiry run by
default. Unbounded snapshots and thousands of tiny Parquet files are why
self-managed lakehouses get slower and more expensive over months; a business
running Clara should never have to know these jobs exist.

---

## Pricing model

Clara's billing unit is public and checkable:

> **1 CCU** (Clara Compute Unit) = 1 vCPU + 4 GB RAM, for 1 minute.

Unlike a DBU or a Snowflake credit, that is anchored to real hardware, so you
can verify Clara's fee against your own cloud bill. Every warehouse size keeps a
1:4 vCPU:RAM ratio, so CCU/minute equals total vCPUs.

The bill has three layers, itemised separately on every invoice:

1. **Infrastructure, at cost — 0% markup.** Priced from your provider's own
   rates. Bring your own cloud account and Clara never touches this layer.
2. **Platform fee — a declining percentage of infrastructure spend.** Clara's
   revenue, expressed as a share of a number you can independently verify.
   Progressive bands, so crossing a threshold never re-rates what is below it.
3. **Add-ons, per unit.** Managed connectors and support: optional, itemised,
   avoidable by self-hosting.

| Plan | Platform fee | Monthly minimum | Notes |
|---|---|---|---|
| **Trial** | none | — | Bounded by your cloud provider's free tier |
| **Community** | **none, forever** | — | Self-hosted, no feature gates, no limits |
| **Team** | 30% → 10% | $99 | 100 GB ingest included |
| **Business** | 20% → 9% | $999 | Commitments, 99.9% SLA |
| **Enterprise** | 8% | $4,999 | BYOC, dedicated support |

Three mechanisms make this more than a markup:

- **The efficiency dividend.** When Clara's optimiser reduces a workload's
  CCU-minutes against its own trailing baseline, you keep 70% of the saving and
  Clara takes 30% — capped at the platform fee, so a bill can never exceed the
  unoptimised one. Databricks and Snowflake earn *more* when your queries are
  slow; this inverts that.
- **Storage and egress at cost.** Charging rent on data you already own, in an
  open format, in your own bucket, would be indefensible.
- **Hard budget caps.** Not an alert — Clara refuses work that would breach the
  cap. Surprise invoices are the most common complaint about usage-based data
  platforms, and a cap that actually stops is the only real fix.

**The trial is bounded by whatever your cloud gives away free** — derived at
runtime from the provider, not hard-coded. On GCP's always-free tier it never
expires. On a provider with no free compute hours it correctly offers storage
but no warehouse allowance. A trial costs the operator nothing to host, so it
does not need an artificial 14-day fuse.

```bash
clara cost compare --ccu-hours 1000 --provider tencent
```

```
Clara on tencent (team plan, 1,000 CCU-hours/month)
  infrastructure  $30.90
  platform fee     $9.27
  total           $40.17   ($0.0402/CCU-hour)

  platform                      $/CCU-hour     $/month   Clara cheaper by
  databricks_jobs_compute            $0.220        $220              5.5×
  snowflake_standard                 $0.250        $250              6.2×
  bigquery_on_demand                 $0.310        $310              7.7×
  databricks_sql_serverless          $0.550        $550             13.7×
```

Competitor figures are approximate public list prices for compute only, before
negotiated discounts. Clara's figures use the provider's on-demand rates.

---

## Running on a cloud

Any S3-compatible storage works. Only the endpoint and credentials change:

```bash
export CLARA_PROVIDER=tencent          # aws | azure | gcp | tencent | alibaba | generic
export CLARA_STORAGE_BUCKET=my-lakehouse
export CLARA_STORAGE_ENDPOINT=https://cos.ap-bangkok.myqcloud.com
export CLARA_STORAGE_ACCESS_KEY=...
export CLARA_STORAGE_SECRET_KEY=...
export CLARA_CATALOG_KIND=rest
export CLARA_CATALOG_URI=https://catalog.internal/catalog
```

`clara provider list` compares cost across all of them:

| provider | medium wh/hour | $/GB-month | free CCU-min/month |
|---|---|---|---|
| generic (Hetzner/OVH class) | $0.0456 | $0.0060 | 0 |
| tencent | $0.2472 | $0.0138 | 0 |
| alibaba | $0.2768 | $0.0148 | 0 |
| aws | $0.3840 | $0.0230 | 11,250 |
| gcp | $0.3872 | $0.0200 | 11,160 |

Commodity providers are roughly **8× cheaper per vCPU-hour** than the
hyperscalers. That spread is the entire argument for refusing to assume a cloud
anywhere in the design.

Adding a provider needs no fork — implement `CloudProvider` and register it, or
publish a `clara.providers` entry point. Connectors work the same way via
`clara.sources`.

### Full containerised stack

```bash
make stack
```

Brings up MinIO (S3), Postgres, Lakekeeper (Iceberg REST catalog), Trino and
Clara. Console at `http://localhost:8000`, Trino at `:8080`, MinIO at `:9001`.
This is the production shape on one machine; swap MinIO's endpoint for real
object storage and the same compose file deploys anywhere.

---

## Layout

```
src/clara/
  spec/           clara.yaml — the declarative platform definition
  catalog/        Iceberg + local catalogs, portable schema/type system
  engines/        DuckDB, Trino, the router, warehouse sizing (CCU)
  connectors/     ingest framework, Airbyte protocol, sources, destinations
  transform/      SQL models, dependency graph, data tests, dbt bridge
  orchestration/  DAG, cron, run/task model, pipeline executor
  metering/       usage events, pricing engine, quotas, invoices
  providers/      pluggable IaaS backends and their unit economics
  control_plane/  REST API + web console
  cli/            the `clara` command
deploy/           Dockerfile, compose stack, Trino config
tests/            hermetic — no network or cloud required
```

Every layer is usable standalone — `clara.catalog` and `clara.metering` are
importable libraries, not framework-internal packages.

## Development

```bash
make install      # venv + all extras
make test         # pytest
make lint         # ruff
make typecheck    # mypy
```

The test suite is hermetic: the local catalog plus DuckDB gives a real lakehouse
in a temp directory, so tests exercise production code paths rather than mocks.
No network, no Docker, no cloud account.

Heavy engines are optional extras behind lazy imports, so `pip install
clara-data` works on a brand-new Python and tells you exactly which extra to add
when you reach for a capability you have not installed.

---

## Status

Working and covered by tests: the declarative spec, local and Iceberg catalogs,
both engines with automatic routing, the ingest framework with three connectors,
SQL transforms with data tests, the DAG executor with cron parsing, metering
through to invoices, quota and free-tier enforcement, the REST API, the web
console, and the CLI.

Known gaps, in rough priority order:

- **Multi-tenancy is modelled but single-tenant in practice.** Usage, quotas and
  billing are keyed by tenant throughout; the control plane serves one tenant
  and the API-key store is in-memory. Real deployments need the Postgres-backed
  key/tenant store the interface already allows for.
- **Run history is in-memory.** Connector checkpoints persist to disk, so
  incremental syncs resume correctly across restarts, but run history does not.
- **The scheduler does not run unattended.** `schedule:` is parsed, validated
  and displayed, and `clara run` executes on demand; a long-running process that
  fires schedules is not wired up. Use cron or a CI job meanwhile.
- **`MERGE` needs Trino.** On the single-node path, keyed upserts are emulated
  by rewriting the affected files — correct and idempotent, but O(table) per
  sync. Fine for development, not for large incremental loads.
- **Provider rate cards are list-price estimates** for one representative
  instance family, refreshed manually. Override them with your negotiated
  pricing before quoting anyone.
- **Iceberg is exercised against the local catalog in tests.** The PyIceberg
  path is implemented and used by the compose stack, but not covered by the
  hermetic suite; it needs integration tests against a real REST catalog.
- **Dagster/Airflow bridges and a BI layer** (Lightdash/Superset) are designed
  for but not implemented.

---

## Appendix — the original brief

Clara was built against these requirements. Where a decision was a judgement
call, the reasoning is in the linked module's docstring.

| # | Requirement | How it is met |
|---|---|---|
| 1 | Function like Databricks/Snowflake | Warehouses, SQL, pipelines, catalog, usage billing — `clara.engines`, `clara.control_plane` |
| 2 | Popular open-source core engines | Iceberg + Trino + DuckDB, all Apache-licensed |
| 3 | Pluggable on any IaaS, including cheap ones | `clara.providers` — 7 providers, plus entry-point plugins; everything addressed as S3 |
| 4 | Mostly open source, community-friendly | Apache 2.0; Community plan free forever; plugin registries for providers and connectors |
| 5 | Serve mid-sized companies, scale to large | DuckDB single-node at the small end, Trino scale-out at the large end, one interface |
| 6 | Usage-based, interesting pricing | Transparent cost-plus + efficiency dividend + hard budget caps — `clara.metering.rates` |
| 7 | Trial limited to cloud free limits | Trial limits derived at runtime from the provider's free tier — `clara.metering.quotas` |
| 8 | Better combination than Airbyte/dbt/Airflow/Lightdash | Airbyte *protocol* without the containers; dbt-compatible SQL without requiring dbt; built-in orchestration instead of Airflow. Reasoning in each module's docstring |
| 9 | Data-engineer-less platform | `clara.yaml` plus a six-step console builder; schemas, dependencies, sizing and maintenance all derived |
| 10 | Consulting and customisation possible | Same spec file underneath the UI; dbt bridge for existing projects; plugin points for private connectors and providers |

## Licence

Apache 2.0. The engines Clara builds on are Apache-licensed too, so there is no
copyleft surprise in a commercial deployment.
