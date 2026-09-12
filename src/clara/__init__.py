"""Clara — an open, multi-cloud lakehouse platform.

Clara assembles proven open-source engines (Apache Iceberg, Trino, DuckDB,
Dagster, dbt) into a single managed surface, so a business gets
Databricks/Snowflake-class capability without a data engineering team and
without being locked to one cloud.

The package is layered; each layer is usable standalone:

    clara.spec           declarative platform definition (the "no data engineer" layer)
    clara.catalog        Apache Iceberg table catalog
    clara.engines        query execution (DuckDB single-node, Trino scale-out)
    clara.connectors     ingestion framework (Airbyte-protocol compatible)
    clara.transform      SQL transformation (dbt-core, or built-in runner)
    clara.orchestration  DAG scheduling and job execution
    clara.metering       usage capture, credit accounting, pricing, quotas
    clara.providers      pluggable IaaS backends (AWS/Azure/GCP/Tencent/Alibaba/...)
    clara.control_plane  the REST API that ties it together
    clara.cli            the `clara` command line
"""

from clara.version import __version__

__all__ = ["__version__"]