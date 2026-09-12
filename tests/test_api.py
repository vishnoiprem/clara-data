"""Control-plane API tests.

Exercised through FastAPI's TestClient against a temp-directory lakehouse, so
these cover the same endpoints the web console calls.
"""

from __future__ import annotations

import time
from collections.abc import Iterator
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from clara.control_plane.app import create_app
from clara.control_plane.state import reset_state
from clara.settings import reset_settings


@pytest.fixture
def client(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[TestClient]:
    """A running control plane with auth disabled and an empty lakehouse."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("CLARA_ENV", "local")
    monkeypatch.setenv("CLARA_PROVIDER", "local")
    monkeypatch.setenv("CLARA_CATALOG_KIND", "sqlite")
    monkeypatch.setenv("CLARA_AUTH_DISABLED", "true")
    monkeypatch.setenv("CLARA_DUCKDB_PATH", ":memory:")
    reset_settings()
    reset_state()

    with TestClient(create_app()) as test_client:
        yield test_client

    reset_state()
    reset_settings()


#: The draft the console's wizard posts to /pipeline/build.
DRAFT = {
    "project": "api_test",
    "description": "built by the test suite",
    "raw_namespace": "raw",
    "analytics_namespace": "analytics",
    "warehouse_size": "xs",
    "schedule": "0 2 * * *",
    "sources": [
        {
            "name": "sample",
            "connector": "sample",
            "config": {"orders": 120, "seed": 5},
            "namespace": "raw",
            "streams": [
                {"name": "customers", "sync_mode": "full_refresh"},
                {
                    "name": "orders",
                    "sync_mode": "incremental",
                    "cursor_field": "ordered_at",
                    "primary_key": "order_id",
                },
            ],
        }
    ],
    "models": [
        {
            "name": "revenue_by_country",
            "sql": (
                "SELECT c.country, count(*) AS orders, round(sum(o.amount), 2) AS revenue "
                "FROM {{ source('raw','orders') }} o "
                "JOIN {{ source('raw','customers') }} c USING (customer_id) "
                "GROUP BY c.country"
            ),
            "materialization": "table",
            "tests": [{"type": "not_null", "column": "country"}],
        }
    ],
}


def _run_to_completion(client: TestClient, timeout: float = 60.0) -> dict:
    """Trigger a run and poll until it reaches a terminal state."""
    response = client.post("/api/v1/runs", json={"trigger": "test"})
    assert response.status_code == 202, response.text
    run_id = response.json()["id"]

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        payload = client.get(f"/api/v1/runs/{run_id}").json()
        if payload["status"] in ("succeeded", "failed", "cancelled"):
            return payload
        time.sleep(0.1)
    raise AssertionError(f"run {run_id} did not finish within {timeout}s")


class TestPlatformEndpoints:
    def test_healthz_needs_no_auth(self, client: TestClient) -> None:
        response = client.get("/healthz")
        assert response.status_code == 200
        assert response.json()["status"] == "ok"

    def test_health_reports_configuration(self, client: TestClient) -> None:
        body = client.get("/api/v1/health").json()
        assert body["status"] == "healthy"
        assert body["provider"] == "local"
        assert "duckdb" in body["engines"]

    def test_console_is_served(self, client: TestClient) -> None:
        response = client.get("/")
        assert response.status_code == 200
        assert "Clara Data" in response.text
        assert client.get("/static/app.js").status_code == 200
        assert client.get("/static/styles.css").status_code == 200

    def test_openapi_is_published(self, client: TestClient) -> None:
        schema = client.get("/api/openapi.json").json()
        assert "/api/v1/pipeline/build" in schema["paths"]

    def test_theme_exposes_brand_tokens(self, client: TestClient) -> None:
        tokens = client.get("/api/v1/theme").json()["tokens"]
        # CP AXTRA corporate blue must be the primary colour everywhere.
        assert tokens["color.primary"] == "#306FC7"
        assert tokens["color.success"] == "#43938F"
        assert tokens["color.danger"] == "#DA3832"

    def test_provider_comparison_is_cheapest_first(self, client: TestClient) -> None:
        rows = client.get("/api/v1/providers/compare").json()
        costs = [r["medium_warehouse_hour"] for r in rows]
        assert costs == sorted(costs)

    def test_unknown_route_is_404(self, client: TestClient) -> None:
        assert client.get("/api/v1/nope").status_code == 404


class TestConnectorEndpoints:
    def test_lists_connectors_with_form_schemas(self, client: TestClient) -> None:
        sources = client.get("/api/v1/connectors").json()["sources"]
        names = {s["name"] for s in sources}
        assert {"sample", "postgres", "http_file"} <= names

    def test_check_reports_success(self, client: TestClient) -> None:
        body = client.post(
            "/api/v1/connectors/sample/check", json={"config": {"orders": 10}}
        ).json()
        assert body["succeeded"] is True
        assert body["detected_streams"] == 2

    def test_check_reports_failure_without_raising(self, client: TestClient) -> None:
        # A wrong password must come back as a readable result, not a 500.
        response = client.post(
            "/api/v1/connectors/http_file/check",
            json={"config": {"url": "http://127.0.0.1:9/nothing"}},
        )
        assert response.status_code == 200
        assert response.json()["succeeded"] is False

    def test_invalid_config_is_422(self, client: TestClient) -> None:
        response = client.post(
            "/api/v1/connectors/postgres/check", json={"config": {"host": "x"}}
        )
        assert response.status_code == 422
        assert response.json()["error"]["code"] == "validation_error"

    def test_unknown_connector_is_502(self, client: TestClient) -> None:
        response = client.post("/api/v1/connectors/ghost/check", json={"config": {}})
        assert response.status_code == 502

    def test_discover_returns_streams_and_columns(self, client: TestClient) -> None:
        streams = client.post(
            "/api/v1/connectors/sample/discover", json={"config": {"orders": 10}}
        ).json()["streams"]
        orders = next(s for s in streams if s["name"] == "orders")
        assert orders["supports_incremental"] is True
        assert orders["default_cursor_field"] == ["ordered_at"]
        assert {c["name"] for c in orders["columns"]} >= {"order_id", "amount"}

    def test_preview_returns_sample_rows(self, client: TestClient) -> None:
        body = client.post(
            "/api/v1/connectors/sample/preview",
            json={"config": {"orders": 50}, "stream": "orders", "limit": 5},
        ).json()
        assert body["row_count"] == 5
        assert "amount" in body["columns"]


class TestPipelineEndpoints:
    def test_build_saves_a_spec_and_returns_a_plan(
        self, client: TestClient, tmp_path: Path
    ) -> None:
        body = client.post("/api/v1/pipeline/build", json=DRAFT).json()
        assert body["summary"]["project"] == "api_test"
        assert body["summary"]["schedule"] == "0 2 * * *"
        # The console's output must be the same file the CLI reads.
        assert (tmp_path / "clara.yaml").is_file()
        assert body["plan"]["batches"][0] == ["ingest:sample"]

    def test_invalid_draft_is_rejected(self, client: TestClient) -> None:
        broken = {**DRAFT, "schedule": "not a cron expression"}
        response = client.post("/api/v1/pipeline/build", json=broken)
        assert response.status_code == 422

    def test_validate_reports_errors_without_saving(self, client: TestClient) -> None:
        body = client.post(
            "/api/v1/spec/validate",
            json={"models": [{"name": "m", "sql": "SELECT 1", "depends_on": ["ghost"]}]},
        ).json()
        assert body["valid"] is False
        assert body["errors"]

    def test_get_spec_after_build(self, client: TestClient) -> None:
        client.post("/api/v1/pipeline/build", json=DRAFT)
        body = client.get("/api/v1/spec").json()
        assert body["spec"]["project"] == "api_test"
        assert len(body["spec"]["models"]) == 1


class TestRunEndpoints:
    def test_end_to_end_run_through_the_api(self, client: TestClient) -> None:
        """The console's "Save & run now" path, start to finish."""
        client.post("/api/v1/pipeline/build", json=DRAFT)
        run = _run_to_completion(client)

        assert run["status"] == "succeeded", run.get("error")
        assert run["records_synced"] == 125  # 120 orders + 5 customers
        assert run["models_built"] == 1

        names = {t["name"]: t["status"] for t in run["tasks"]}
        assert names["ingest:sample"] == "succeeded"
        assert names["model:revenue_by_country"] == "succeeded"

        # Tables are now visible and queryable.
        tables = {t["fqn"] for t in client.get("/api/v1/catalog/tables").json()["tables"]}
        assert {"raw.orders", "raw.customers", "analytics.revenue_by_country"} <= tables

    def test_running_an_empty_pipeline_is_rejected(self, client: TestClient) -> None:
        response = client.post("/api/v1/runs", json={})
        assert response.status_code == 422
        assert "nothing to run" in response.json()["error"]["message"]

    def test_second_run_is_incremental(self, client: TestClient) -> None:
        client.post("/api/v1/pipeline/build", json=DRAFT)
        first = _run_to_completion(client)
        second = _run_to_completion(client)
        assert second["status"] == "succeeded"
        assert second["records_synced"] < first["records_synced"]

    def test_run_history_is_listed(self, client: TestClient) -> None:
        client.post("/api/v1/pipeline/build", json=DRAFT)
        _run_to_completion(client)
        runs = client.get("/api/v1/runs").json()["runs"]
        assert len(runs) >= 1
        assert runs[0]["pipeline"] == "api_test"

    def test_unknown_run_is_404(self, client: TestClient) -> None:
        assert client.get("/api/v1/runs/run_missing").status_code == 404


class TestQueryEndpoints:
    @pytest.fixture(autouse=True)
    def _loaded(self, client: TestClient) -> None:
        client.post("/api/v1/pipeline/build", json=DRAFT)
        _run_to_completion(client)

    def test_query_returns_rows_routing_and_cost(self, client: TestClient) -> None:
        body = client.post(
            "/api/v1/query",
            json={"sql": "SELECT * FROM analytics.revenue_by_country ORDER BY revenue DESC"},
        ).json()
        assert body["columns"] == ["country", "orders", "revenue"]
        assert body["row_count"] > 0
        assert body["routing"]["engine"] == "duckdb"
        assert "ccu_minutes" in body["cost"]

    def test_explain_only_does_not_execute(self, client: TestClient) -> None:
        body = client.post(
            "/api/v1/query",
            json={"sql": "SELECT * FROM raw.orders", "explain_only": True},
        ).json()
        assert "plan" in body and "cost" in body
        assert "rows" not in body

    def test_bad_sql_is_a_400(self, client: TestClient) -> None:
        response = client.post("/api/v1/query", json={"sql": "SELECT * FROM nope_xyz"})
        assert response.status_code == 400
        assert response.json()["error"]["code"] == "engine_error"

    def test_sql_preview_compiles_templating(self, client: TestClient) -> None:
        body = client.post(
            "/api/v1/query/preview",
            json={"sql": "SELECT count(*) AS n FROM {{ source('raw','orders') }}", "limit": 5},
        ).json()
        assert '"raw"."orders"' in body["compiled_sql"]
        assert body["rows"][0][0] == 120

    def test_recommends_a_warehouse_size(self, client: TestClient) -> None:
        body = client.post(
            "/api/v1/query/recommend-size", json={"sql": "SELECT * FROM raw.orders"}
        ).json()
        assert body["recommended_size"] in {"xs", "s", "m", "l", "xl", "2xl", "3xl", "4xl"}

    def test_table_preview_and_detail(self, client: TestClient) -> None:
        detail = client.get("/api/v1/catalog/tables/raw/orders").json()
        assert detail["row_count"] == 120
        assert {f["name"] for f in detail["schema"]["fields"]} >= {"order_id", "amount"}

        preview = client.get("/api/v1/catalog/tables/raw/orders/preview?limit=3").json()
        assert len(preview["rows"]) == 3

    def test_unknown_table_is_404(self, client: TestClient) -> None:
        assert client.get("/api/v1/catalog/tables/raw/ghost").status_code == 404


class TestBillingEndpoints:
    def test_dashboard_returns_everything_in_one_call(self, client: TestClient) -> None:
        client.post("/api/v1/pipeline/build", json=DRAFT)
        _run_to_completion(client)
        body = client.get("/api/v1/dashboard").json()

        for key in ("health", "spec", "tables", "runs", "usage", "forecast", "warehouses"):
            assert key in body, f"console home screen needs {key}"
        assert body["table_count"] >= 3

    def test_usage_and_quotas(self, client: TestClient) -> None:
        assert "quantities" in client.get("/api/v1/usage").json()
        quotas = client.get("/api/v1/usage/quotas").json()
        assert quotas["plan"] == "trial"
        assert quotas["checks"]

    def test_invoice_renders_text(self, client: TestClient) -> None:
        body = client.get("/api/v1/billing/invoice").json()
        assert "CLARA DATA" in body["text"]
        assert body["bill"]["plan"] == "trial"

    def test_invoice_can_be_priced_on_another_plan(self, client: TestClient) -> None:
        body = client.get("/api/v1/billing/invoice?plan=team").json()
        assert body["bill"]["plan"] == "team"

    def test_plans_and_comparison(self, client: TestClient) -> None:
        body = client.get("/api/v1/billing/plans").json()
        assert len(body["rate_card"]["plans"]) == 5
        assert body["comparison_on_current_usage"]

    def test_credit_purchase_applies_bonus(self, client: TestClient) -> None:
        body = client.post(
            "/api/v1/billing/credits", json={"amount_usd": 50000, "term_months": 12}
        ).json()
        assert body["grant"]["bonus_usd"] == pytest.approx(5000.0)
        assert body["balance_usd"] == pytest.approx(55000.0)

    def test_estimate_compares_against_incumbents(self, client: TestClient) -> None:
        body = client.post(
            "/api/v1/billing/estimate",
            json={"monthly_ccu_hours": 2000, "plan": "team", "provider": "aws"},
        ).json()
        assert body["provider"] == "aws"
        assert body["competitors"]
        assert all(c["clara_is_cheaper_by"] > 1 for c in body["competitors"])


class TestWarehouseEndpoints:
    def test_lists_with_hourly_cost(self, client: TestClient) -> None:
        warehouses = client.get("/api/v1/warehouses").json()["warehouses"]
        assert warehouses
        assert "cost_per_hour" in warehouses[0]

    def test_suspend_and_resume(self, client: TestClient) -> None:
        name = client.get("/api/v1/warehouses").json()["warehouses"][0]["name"]
        assert client.post(f"/api/v1/warehouses/{name}/resume").json()["state"] == "running"
        assert client.post(f"/api/v1/warehouses/{name}/suspend").json()["state"] == "suspended"

    def test_unknown_warehouse_is_404(self, client: TestClient) -> None:
        assert client.post("/api/v1/warehouses/ghost/resume").status_code == 404


class TestAuth:
    def test_requires_a_key_when_enabled(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.chdir(tmp_path)
        monkeypatch.setenv("CLARA_ENV", "local")
        monkeypatch.setenv("CLARA_PROVIDER", "local")
        monkeypatch.setenv("CLARA_AUTH_DISABLED", "false")
        monkeypatch.setenv("CLARA_BOOTSTRAP_API_KEY", "clara_sk_test_key")
        monkeypatch.setenv("CLARA_DUCKDB_PATH", ":memory:")
        reset_settings()
        reset_state()

        with TestClient(create_app()) as client:
            assert client.get("/api/v1/health").status_code == 401
            assert client.get("/healthz").status_code == 200, "probes stay open"

            ok = client.get(
                "/api/v1/health", headers={"Authorization": "Bearer clara_sk_test_key"}
            )
            assert ok.status_code == 200

            assert client.get(
                "/api/v1/health", headers={"Authorization": "Bearer wrong"}
            ).status_code == 401
            # The alternate header form the console may use.
            assert client.get(
                "/api/v1/health", headers={"X-Clara-Api-Key": "clara_sk_test_key"}
            ).status_code == 200

        reset_state()
        reset_settings()

    def test_auth_cannot_be_disabled_outside_local(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A misconfigured production deployment must fail to start, not serve
        unauthenticated traffic."""
        monkeypatch.setenv("CLARA_ENV", "prod")
        monkeypatch.setenv("CLARA_AUTH_DISABLED", "true")
        reset_settings()
        with pytest.raises(Exception, match="only permitted when CLARA_ENV=local"):
            from clara.settings import Settings

            Settings()
        reset_settings()
