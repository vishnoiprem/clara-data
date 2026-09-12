# Architecture and the decisions behind it

The README says what Clara does. This says why it is built this way, including
where the reasoning is contestable.

## The shape

```
                    ┌──────────────────────────────────────┐
                    │  console (static)  │  CLI  │  REST   │
                    └──────────────────────────────────────┘
                                     │
                    ┌──────────────────────────────────────┐
                    │        control plane (FastAPI)       │
                    │  auth · state · runs · quotas        │
                    └──────────────────────────────────────┘
                                     │
        ┌────────────┬───────────────┼──────────────┬───────────────┐
        ▼            ▼               ▼              ▼               ▼
  ┌──────────┐ ┌──────────┐   ┌────────────┐  ┌──────────┐  ┌──────────────┐
  │   spec   │ │connectors│   │orchestration│  │transform │  │   metering   │
  │clara.yaml│ │  ingest  │   │  DAG + cron │  │SQL models│  │usage→invoice │
  └──────────┘ └──────────┘   └────────────┘  └──────────┘  └──────────────┘
                     │               │              │
                     └───────────────┼──────────────┘
                                     ▼
                    ┌──────────────────────────────────────┐
                    │  engines: router → DuckDB / Trino    │
                    ├──────────────────────────────────────┤
                    │  catalog: Apache Iceberg             │
                    ├──────────────────────────────────────┤
                    │  providers: any S3-compatible store  │
                    └──────────────────────────────────────┘
```

Dependencies point downward only. `clara.catalog` knows nothing about the API;
`clara.metering` knows nothing about engines beyond a stats object. Each layer
is importable on its own, which is what makes the test suite hermetic and the
pieces reusable.

## Decisions

### Iceberg, not Delta or Hudi

All three are credible. Iceberg wins on **engine neutrality**: Trino, Spark,
Flink, DuckDB, ClickHouse and Snowflake all read it as a first-class format,
whereas Delta's best implementation is tied to Databricks' runtime. For a
platform whose main promise is "you are not locked in", the table format must be
the one no vendor controls.

The practical consequence: a customer who leaves Clara keeps a working
lakehouse. Their data is Parquet in their own bucket with an open catalog. That
is the single strongest thing Clara can say against Snowflake, and it only holds
if nothing proprietary touches the storage layer.

### Trino *and* DuckDB, not one engine

The observation this rests on: most analytical queries are small. A distributed
engine pays coordination and shuffle costs on every query; below roughly 20 GB
scanned, a single well-provisioned node wins outright.

Running both and routing per query means small queries cost near-nothing and
large ones still scale. `clara.engines.router` decides from Iceberg's own
snapshot statistics, which are free to read — no planning round trip.

The cost of this choice is real: two engines means two SQL dialects to keep
compatible, and `EngineCapabilities` exists to stop the layers above from
guessing. The main visible seam is `MERGE`, which DuckDB cannot do against
object-storage files; the single-node path emulates it by rewriting affected
files, which is correct and idempotent but O(table).

### Trino, not Spark, as the scale-out engine

Spark is more capable for ML and very large batch. Trino was chosen because the
target customer is running on commodity hardware, where Spark's per-query memory
footprint and JVM startup are a poor fit, and because ANSI SQL is what a
business without a data team can actually use. Spark remains the right answer for
heavy ML — which is why it is a gap, not a rejection.

### Built-in orchestration, not Airflow

Airflow would add a scheduler, a metadata database, a web server and a DAG
authoring model — for a dependency graph that Clara can derive from the SQL. The
built-in executor covers ingest → transform → maintenance with cron, which is
the shape every pipeline here takes.

Teams that already run Dagster or Airflow should drive `PipelineExecutor` from
their own scheduler rather than adopt Clara's. That bridge is designed for and
unimplemented.

### Airbyte's protocol, not Airbyte

The protocol is the valuable part: it is a well-designed contract (spec, check,
discover, read, with resumable state) and it brings an ecosystem of hundreds of
existing connectors. The *runtime* — a container per connector — is heavy for a
small deployment.

So Clara speaks the protocol, ships native in-process Python connectors for
speed, and can drive Airbyte container images through the same runner for the
long tail.

### dbt-compatible authoring, without requiring dbt

dbt needs a project directory, a profile, a Python environment and someone who
understands all three. The target customer has none of those. So Clara
implements `{{ ref }}`, `{{ source }}`, `{{ var }}`, materializations and
`is_incremental()` directly, and a user of the console types SQL into a box.

Teams that already have a dbt project point Clara at it
(`clara.transform.dbt_runner`) and keep their SQL. Both paths produce the same
Iceberg tables.

The deliberate limitation: Clara's templating is regex-based, not Jinja. It
handles the constructs above and nothing else. Full Jinja (loops, macros,
conditionals) would mean a Jinja dependency and a much larger compatibility
surface; if a team needs that, they should use the dbt bridge.

### The declarative spec is the product

`clara.yaml` is not a config file the UI writes as a side effect — it *is* the
platform definition, and both the console and the CLI read and write the same
document through the same validator. That property is what lets a business build
a pipeline by clicking while a consultant reviews it as a diff in a pull
request. It also means the console cannot produce something the CLI would
reject, because there is one `PlatformSpec` and one parse path.

### Providers expose economics, not compute

`CloudProvider` supplies object storage, unit costs and free-tier limits — and
deliberately **not** VM provisioning, Kubernetes or IAM. Abstracting cluster
provisioning across six clouds is how portability projects die. Clara runs its
engines as containers on whatever compute the customer already has.

Including unit costs in the provider interface is what makes cost-plus pricing
possible at all: the pricing engine needs to know what a vCPU-hour actually
costs *here*, and that is provider knowledge.

### Usage events snapshot their unit cost

`UsageEvent.unit_cost_usd` is captured at record time, not looked up at billing
time. Provider prices change; an invoice must not. Re-running billing for March
produces March's number, forever.

Events also record **infrastructure cost, not price**. Price is derived later
from the plan and rate card, so the same event stream answers both "what did
this cost us" and "what do we charge" without double bookkeeping.

## Local vs production

| | Local (`clara init`) | Production (`make stack` / cloud) |
|---|---|---|
| Catalog | `LocalCatalog` — JSON metadata, Parquet files | Iceberg REST (Lakekeeper/Polaris/Nessie) |
| Storage | filesystem | S3-compatible object store |
| Engine | DuckDB | Trino + DuckDB, routed |
| Auth | optional | API key, hashed at rest |
| Merge | file rewrite | Iceberg merge-on-read |

`LocalCatalog` exists so `clara init` works with nothing installed and the test
suite needs no network. It implements the same `Catalog` interface, including
views and keyed upserts, so behaviour matches up the stack. It is explicitly not
production-grade: no snapshot isolation, no concurrency control beyond a process
lock.

## Things that are load-bearing and easy to break

- **`coerce_record` at the write boundary.** Connectors emit JSON; Arrow and
  Iceberg will not implicitly convert `"2026-09-12T00:00:00Z"` into a timestamp.
  Every write goes through coercion, and removing it breaks every typed column.
- **State committed after the write.** `SyncRunner` holds a connector's STATE
  checkpoint until the records it covers have been handed to the destination, so
  a crash replays rows rather than skipping them. Committing state on receipt
  would silently lose data.
- **Cursor comparison on parsed values, never ISO strings.** `"…:29Z"` sorts
  *after* `"…:29.490439Z"` lexicographically, because `'Z' > '.'`. Comparing
  strings replays or skips boundary rows on every incremental run. This was a
  real bug caught by a test.
- **Reserved `LogRecord` attributes.** Passing `message` in `extra=` makes
  `logging` raise, which turns a handled error into a crash inside the error
  handler. Use `detail`.
- **Falsy-object defaults.** `UsageStore` and `Schema` define `__len__`, so an
  empty one is falsy and `store or Default()` silently discards it. Use
  `x if x is not None else Default()`.
- **Per-sync state on reused destinations.** The executor holds one
  `LakehouseDestination` across runs; its overwrite tracking must reset in
  `begin_sync()`, or a second full refresh appends instead of replacing.

## Testing approach

209 tests, no network, no Docker, no cloud account. The local catalog plus
DuckDB provides a real lakehouse in a temp directory, so tests drive production
code paths rather than mocks — `test_pipeline.py::test_runs_end_to_end` syncs a
source, builds three models, runs data tests and asserts actual row counts.

The gap this leaves is honest and stated in the README: the PyIceberg path is
implemented and used by the compose stack but is not covered by the hermetic
suite. It needs integration tests against a real REST catalog.
