# Clara control plane + web console.
#
# Two stages so the runtime image carries no compiler toolchain. Runs as a
# non-root user, because a data platform holding customer credentials has no
# business running as root.

FROM python:3.12-slim AS builder

ENV PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN apt-get update \
 && apt-get install -y --no-install-recommends build-essential \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY pyproject.toml README.md ./
COPY src ./src

# Install into a virtualenv that the runtime stage copies wholesale.
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
RUN pip install --upgrade pip setuptools wheel \
 && pip install ".[duckdb,trino,iceberg,s3,postgres,server]"


FROM python:3.12-slim AS runtime

ENV PATH="/opt/venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    CLARA_API_HOST=0.0.0.0 \
    CLARA_API_PORT=8080

RUN apt-get update \
 && apt-get install -y --no-install-recommends curl \
 && rm -rf /var/lib/apt/lists/* \
 && useradd --create-home --uid 10001 clara

COPY --from=builder /opt/venv /opt/venv

WORKDIR /workspace
RUN chown clara:clara /workspace
USER clara

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
  CMD curl -fsS http://localhost:8080/healthz || exit 1

# uvicorn directly rather than `clara serve`, so container signals and logs
# behave as an orchestrator expects.
CMD ["uvicorn", "clara.control_plane.app:app", \
     "--host", "0.0.0.0", "--port", "8080", \
     "--no-access-log", "--proxy-headers"]
