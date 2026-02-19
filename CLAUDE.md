# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

YHODA (Yorkshire Health Observatory Data Aggregator) is a **Prefect v3 ETL pipeline** that collects, transforms, and warehouses socioeconomic, health, and environmental indicators for all 22 Yorkshire Local Authority Districts (LADs) into a SQL Server data warehouse.

**Current Status:** Phase 1 complete (scaffolding). Phase 2 (implementation) is pending — all flow and task bodies contain `# TODO: implement` stubs.

## Commands

**Install dependencies:**
```bash
uv sync --extra dev
```

**Lint and type-check:**
```bash
uv run ruff check src/ tests/          # Lint
uv run ruff check --fix src/ tests/    # Auto-fix
uv run ruff format src/ tests/         # Format
uv run mypy src/                       # Type check
```

**Run tests:**
```bash
uv run pytest                                        # All tests
uv run pytest tests/unit/                            # Unit tests only
uv run pytest tests/unit/test_config.py -v          # Single file
uv run pytest --cov=src/yhovi_pipeline               # With coverage
```

Unit tests require env vars (no real DB needed):
```bash
export SQL_SERVER_CONNECTION_STRING="mssql+pyodbc://t:t@h/d"
export DWP_API_KEY="x"
uv run pytest tests/unit/
```

**Database migrations:**
```bash
uv run alembic upgrade head
uv run alembic revision --autogenerate -m "describe change"
uv run alembic downgrade -1
```

**Prefect deployments:**
```bash
uv run prefect deploy --all --no-prompt
```

## Architecture

### Data Flow
```
8 Source APIs → extract/ tasks → transform/ tasks (validate → normalise → geo) → load/ tasks → SQL Server
```

Each source maps to a dedicated extract task in `src/yhovi_pipeline/tasks/extract/`. Flows in `src/yhovi_pipeline/flows/` orchestrate these tasks and are grouped into three domains: `economy/` (4 flows), `society/` (7 flows), `environment/` (2 flows).

### Key Modules

- **`config.py`** — Pydantic Settings singleton. Always use `get_settings()` to access config; never read `os.environ` directly (exception: `db/migrations/env.py`). Secrets are `SecretStr` — call `.get_secret_value()` only at the call site.
- **`db/models.py`** — SQLAlchemy 2.0 ORM with three tables: `Indicator` (fact), `DatasetMetadata` (audit/run history), `GeoLookup` (LSOA→MSOA→LAD→Region dimension).
- **`tasks/transform/`** — `validate.py` (schema & LAD validation), `normalise.py` (canonical Indicator shape), `geo.py` (geography aggregation).
- **`tasks/load/sql_server.py`** — Upserts indicators and writes run metadata.
- **`utils/geo_lookups.py`** — LRU-cached ONS geography lookup; `lsoa_to_lad()` for aggregation.

### Flow/Task Conventions

**Flows:** `@flow(name="<domain>/<slug>", retries=1, task_runner=ThreadPoolTaskRunner(max_workers=4))`. Flow names must match entries in `prefect.yaml`. Flows orchestrate; business logic belongs in tasks.

**Tasks:** `@task(name="<layer>/<source>/<action>", retries=3, retry_delay_seconds=60)`. Tasks are small and single-purpose; return typed DataFrames or ORM instances.

### Database

SQL Server via pyodbc. Upsert key on `Indicator` is `(indicator_id, lad_code, reference_period)`. All 22 Yorkshire LAD codes are defined as `YORKSHIRE_LAD_CODES` in `config.py` and used throughout for filtering/validation. `ExtractionStatus` enum uses `native_enum=False` (SQL Server doesn't support native ENUMs).

### CI/CD

GitHub Actions (`.github/workflows/ci.yml`): lint → test → deploy (deploy only on push to `main`). Alembic migrations are auto-formatted with ruff post-write hooks (configured in `alembic.ini`).

## Environment Variables

See `.env.example`. Required: `SQL_SERVER_CONNECTION_STRING`, `DWP_API_KEY`. Optional: `NOMIS_API_KEY`, `PREFECT_API_URL`, `PREFECT_WORK_POOL`, `LOG_LEVEL`.
