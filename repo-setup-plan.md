## Claude Code Prompt: Yorkshire Vitality Observatory — Prefect Data Pipeline

```
You are initialising a production-grade Python data pipeline repository for the Yorkshire & Humber Office for Data Analytics (YHODA) — specifically the Yorkshire Vitality Observatory (YVO). The pipeline automates ETL (Extract, Transform, Load) workflows for ~60 indicators across three domains (Economy, Society, Environment) at Local Authority District (LAD) level across Yorkshire & Humber.

Read the Prefect v3 documentation thoroughly before writing any code: https://docs.prefect.io/v3/get-started

---

## PROJECT CONTEXT

- **Purpose**: Replace manual data workflows with automated pipelines feeding a SQL Server database that powers the Yorkshire Vitality Suite (PowerBI dashboards).
- **Update cadence**: Primarily monthly, some datasets annual.
- **Geographic scope**: Yorkshire & Humber Local Authority Districts (LADs), requiring standardisation to a common geographic boundary (LSOAs where appropriate).
- **Licence**: MIT
- **Version control**: GitHub (include .gitignore and .github/workflows CI)

---

## DATA SOURCES TO SUPPORT

The pipeline must include connector modules for ALL of the following sources. Implement concrete (not stub) clients where APIs are documented:

1. **Nomis / ONS API** (https://www.nomisweb.co.uk) — primary source for:
   - Annual Population Survey (APS): employment, unemployment, economic inactivity, self-employment, occupations
   - Population Estimates (mid-year)
   - Jobs Density
   - Annual Survey of Hours and Earnings (ASHE) — workplace & residence based
   - UK Business Counts (Business Demography)
   - Subregional Productivity (GVA per hour)
   - Regional GVA by Local Authority

2. **ONS bulk download / datasets API** (https://www.ons.gov.uk) — for datasets not available via Nomis:
   - Business Demography Reference Table
   - GVA by sector per Local Authority (Regional GVA balanced by industry)
   - Household income inequality / ASHE table 8 (earnings percentiles, Gini)
   - Recorded crime by Community Safety Partnership area
   - Access to gardens and public green space

3. **DWP Stat-Xplore** (https://stat-xplore.dwp.gov.uk) — for:
   - Personal Independence Payment (PIP) registrations per Local Authority
   - Children in Low Income Families (Relative Low Income)

4. **Fingertips / Public Health Outcomes Framework** (https://fingertips.phe.org.uk) — for:
   - Life expectancy at birth (male/female)
   - Healthy life expectancy at birth
   - Under-75 mortality from preventable causes

5. **ONS Wellbeing** — Annual personal well-being estimates (life satisfaction, worthwhile)

6. **Sport England Active Lives Survey** (https://activelives.sportengland.org) — physical activity levels, volunteering

7. **Ofcom Connected Nations** — broadband coverage and speeds by LA

8. **DESNZ / BEIS Local Authority Greenhouse Gas Emissions** (https://www.data.gov.uk) — GHG per capita

9. **Defra Local Authority Collected Waste** (https://www.gov.uk/government/statistics/) — recycling rate, waste per head

---

## REPOSITORY STRUCTURE

Initialise the repository with the following structure following Prefect v3 conventions:

```
yhovi-pipeline/
├── prefect.yaml                  # Prefect deployment config (all deployments)
├── .prefectignore
├── .gitignore
├── pyproject.toml                # Project metadata, dependencies (use uv or pip-tools)
├── README.md
├── LICENSE                       # MIT
├── .env.example                  # Template for secrets (never commit .env)
├── .github/
│   └── workflows/
│       └── ci.yml                # Run tests + prefect deploy on push to main
├── src/
│   └── yhovi_pipeline/
│       ├── __init__.py
│       ├── config.py             # Settings via pydantic-settings (reads from env)
│       ├── flows/
│       │   ├── __init__.py
│       │   ├── economy/
│       │   │   ├── __init__.py
│       │   │   ├── employment_jobs.py     # E-EJ-* indicators
│       │   │   ├── earnings_income.py     # E-EI-* indicators
│       │   │   ├── business_economy.py    # E-BE-* indicators
│       │   │   └── sectors.py             # E-S-* indicators
│       │   ├── society/
│       │   │   ├── __init__.py
│       │   │   ├── demographics.py        # S-D-* indicators
│       │   │   ├── housing.py             # S-H-* indicators
│       │   │   ├── health.py              # S-HE-* indicators
│       │   │   ├── wellbeing.py           # S-W-* indicators
│       │   │   ├── connectivity.py        # S-C-* indicators
│       │   │   ├── civic_participation.py # S-CP-* indicators
│       │   │   └── community_safety.py    # S-CS-* indicators
│       │   └── environment/
│       │       ├── __init__.py
│       │       ├── net_zero.py            # E-NZ-* indicators
│       │       ├── waste_recycling.py     # E-WR-* indicators
│       │       └── natural_environment.py # E-NE-* indicators
│       ├── tasks/
│       │   ├── __init__.py
│       │   ├── extract/
│       │   │   ├── __init__.py
│       │   │   ├── nomis.py        # Nomis API client tasks
│       │   │   ├── ons.py          # ONS bulk download tasks
│       │   │   ├── dwp.py          # DWP Stat-Xplore tasks
│       │   │   ├── fingertips.py   # Fingertips API tasks
│       │   │   ├── sport_england.py
│       │   │   ├── ofcom.py
│       │   │   ├── defra.py
│       │   │   └── beis.py
│       │   ├── transform/
│       │   │   ├── __init__.py
│       │   │   ├── geo.py          # Geographic standardisation (LSOA → LAD)
│       │   │   ├── validate.py     # Data quality checks (Great Expectations or pandera)
│       │   │   └── normalise.py    # Common schema normalisation
│       │   └── load/
│       │       ├── __init__.py
│       │       └── sql_server.py   # SQLAlchemy + pyodbc loader tasks
│       ├── db/
│       │   ├── __init__.py
│       │   ├── models.py           # SQLAlchemy ORM models (time series + metadata)
│       │   └── migrations/         # Alembic migrations
│       │       └── env.py
│       └── utils/
│           ├── __init__.py
│           ├── logging.py          # Structured logging helpers
│           ├── metadata.py         # Dataset metadata tracking
│           └── geo_lookups.py      # ONS geography lookup tables/helpers
└── tests/
    ├── conftest.py
    ├── unit/
    │   ├── test_nomis.py
    │   ├── test_transform.py
    │   └── test_validate.py
    └── integration/
        └── test_flows.py           # Prefect test runner
```

---

## IMPLEMENTATION REQUIREMENTS

### 1. Prefect v3 Patterns

- Use `@flow` and `@task` decorators throughout. All flows must have `name`, `description`, `retries`, and `retry_delay_seconds` set.
- Tasks should have `retries=3`, `retry_delay_seconds=60`, and `log_prints=True`.
- Use Prefect's built-in logger (`get_run_logger()`) for all pipeline logging — NOT plain `print()`.
- Register all deployments in `prefect.yaml` using `prefect deploy`. Include a `work_pool` named `yhovi-default`.
- Use `prefect.blocks` (Secret blocks) for all credentials — never hardcode secrets. Credentials needed:
  - `NOMIS_API_KEY` (optional, increases rate limits)
  - `DWP_API_KEY` (Stat-Xplore API key)
  - `SQL_SERVER_CONNECTION_STRING`
- Use `ConcurrentTaskRunner` where tasks can be parallelised (e.g. fetching data for multiple LAs simultaneously).
- Schedule all flows monthly via `CronSchedule("0 6 1 * *")` (1st of each month at 06:00 UTC) in `prefect.yaml`.

### 2. Extract Tasks

For each source, implement a concrete HTTP client using `httpx` (async-compatible). Each extract task must:
- Accept `dataset_id`, `geography_codes` (list of ONS LAD codes for Yorkshire), and `reference_date` parameters.
- Return a `pandas.DataFrame` with raw data.
- Handle pagination where APIs return paginated responses.
- Implement exponential back-off on rate-limit responses (HTTP 429).
- Log the URL, response status, and number of records retrieved.

**Yorkshire LAD codes** to hardcode as a constant in `config.py`:
```python
YORKSHIRE_LAD_CODES = [
    "E06000014",  # York
    "E07000163",  # Craven
    "E07000164",  # Hambleton
    "E07000165",  # Harrogate
    "E07000166",  # Richmondshire
    "E07000167",  # Ryedale
    "E07000168",  # Scarborough
    "E07000169",  # Selby
    "E08000016",  # Barnsley
    "E08000017",  # Doncaster
    "E08000018",  # Rotherham
    "E08000019",  # Sheffield
    "E08000032",  # Bradford
    "E08000033",  # Calderdale
    "E08000034",  # Kirklees
    "E08000035",  # Leeds
    "E08000036",  # Wakefield
    "E06000010",  # Kingston upon Hull
    "E06000011",  # East Riding of Yorkshire
    "E06000012",  # North East Lincolnshire
    "E06000013",  # North Lincolnshire
    "E06000061",  # North Yorkshire (post-2023 merger)
]
```

**Nomis API**: Use `https://www.nomisweb.co.uk/api/v01/dataset/{dataset_id}.data.csv` with parameters `geography`, `date`, `measures`. Refer to https://www.nomisweb.co.uk/api/v01/help for full parameter reference.

**DWP Stat-Xplore**: Use the REST API at `https://stat-xplore.dwp.gov.uk/webapi/rest/v1/`. Implement `get_schema()` and `get_table()` methods.

**Fingertips**: Use the public Fingertips API `https://fingertips.phe.org.uk/api/`. Key endpoints: `/indicator_metadata/csv/by_indicator_id` and `/data/csv/by_indicator_id`.

### 3. Transform Tasks

- `geo.py`: Implement `standardise_geography(df, source_geo_col, target="LAD")` that maps from LSOA/MSOA/CSP/Region to LAD using ONS geography lookup tables (download from https://geoportal.statistics.gov.uk/).
- `validate.py`: Use `pandera` to define a `IndicatorSchema` with required columns: `indicator_id`, `lad_code`, `lad_name`, `reference_period`, `value`, `unit`, `source`, `extracted_at`. All columns non-null except `value` (flag missing values rather than drop).
- `normalise.py`: Apply the common schema, calculate z-scores for composite index construction (for future use), and produce a tidy long-format DataFrame.

### 4. Load Tasks

- Use `SQLAlchemy` with `pyodbc` for SQL Server connectivity.
- Implement `upsert_indicators(df, table_name)` using `MERGE` SQL logic (upsert by `indicator_id + lad_code + reference_period`).
- Implement a separate `metadata` table tracking: `dataset_id`, `source_name`, `source_url`, `last_extracted_at`, `record_count`, `pipeline_run_id`.

### 5. Database Models

Define SQLAlchemy ORM models for:
- `indicators` table: `id`, `indicator_id` (VARCHAR), `dataset_id`, `domain`, `subdomain`, `lad_code`, `lad_name`, `reference_period` (DATE), `value` (FLOAT), `unit`, `source`, `extracted_at`, `pipeline_run_id`
- `dataset_metadata` table: `dataset_id`, `source_name`, `api_endpoint`, `last_extracted_at`, `record_count`, `status` (ENUM: success/failure/partial)
- `geo_lookup` table: ONS geography crosswalk (LSOA → MSOA → LAD → Region)

Use Alembic for migration management. Run `alembic init db/migrations` and create an initial migration.

### 6. Error Handling & Alerting

- Wrap all extract tasks in try/except. On failure, log the error with `logger.error()` and emit a Prefect event (`emit_event()`) with event name `"yhovi.pipeline.extract.failed"`.
- Implement a parent orchestrator flow in `flows/orchestrator.py` that runs all domain flows and sends a summary notification on completion (use Prefect's notification blocks or a simple email via SMTP Secret block).
- Failed indicator extracts should NOT stop the whole pipeline — use `allow_failure()` wrappers where appropriate.

### 7. Configuration

Use `pydantic-settings` in `config.py` to manage all configuration. The `Settings` class should include:
- `sql_server_connection_string: SecretStr`
- `nomis_api_key: SecretStr | None = None`
- `dwp_api_key: SecretStr`
- `prefect_work_pool: str = "yhovi-default"`
- `log_level: str = "INFO"`
- `yorkshire_lad_codes: list[str]` (default to the constant above)

### 8. Testing

- Write unit tests for each extract client using `pytest` with `respx` (mock HTTP for `httpx`).
- Write transform tests using `pandera` schema validation against sample DataFrames.
- Write a Prefect flow test using `prefect.testing.utilities` to run flows in test mode.
- Achieve >80% coverage on extract and transform modules.

### 9. Documentation

- `README.md` must include: project overview, quickstart (how to run locally), environment variable reference, how to add a new data source, architecture diagram (Mermaid).
- Docstrings on all flows, tasks, and public functions (Google style).
- `CONTRIBUTING.md` with branching strategy (feature branches → main), PR process, and how to run tests.

### 10. CI/CD

Create `.github/workflows/ci.yml` that:
- Runs on push to `main` and pull requests.
- Installs dependencies, runs `pytest`, and checks coverage.
- On push to `main` only: runs `prefect deploy --all` to register deployments.
- Uses GitHub Actions secrets: `PREFECT_API_KEY`, `PREFECT_API_URL`.

---

## DEPENDENCIES (pyproject.toml)

```toml
[project]
name = "yhovi-pipeline"
version = "0.1.0"
requires-python = ">=3.11"
dependencies = [
    "prefect>=3.0",
    "httpx>=0.27",
    "pandas>=2.0",
    "pandera>=0.19",
    "sqlalchemy>=2.0",
    "pyodbc>=5.0",
    "pydantic-settings>=2.0",
    "alembic>=1.13",
    "tenacity>=8.0",       # retry/backoff utilities
]

[project.optional-dependencies]
dev = [
    "pytest>=8.0",
    "pytest-cov",
    "pytest-asyncio",
    "respx",               # httpx mock
    "ruff",                # linting
    "mypy",
]
```

---

## STARTING POINT — IMPLEMENT FULLY

After scaffolding the full directory structure, implement these files completely (not as stubs):

1. `src/yhovi_pipeline/config.py` — full Settings class
2. `src/yhovi_pipeline/tasks/extract/nomis.py` — full Nomis API client with `fetch_nomis_dataset()` task
3. `src/yhovi_pipeline/tasks/transform/validate.py` — full pandera schema + validation task
4. `src/yhovi_pipeline/tasks/load/sql_server.py` — full upsert task
5. `src/yhovi_pipeline/db/models.py` — full ORM models
6. `src/yhovi_pipeline/flows/economy/employment_jobs.py` — full flow for E-EJ-* indicators as a working end-to-end example
7. `src/yhovi_pipeline/flows/orchestrator.py` — top-level orchestrator flow
8. `prefect.yaml` — full deployment config for all flows
9. `tests/unit/test_nomis.py` — full unit tests
10. `README.md` — full documentation

For all other flow files and extract modules, create the file with the correct structure, imports, and docstrings but indicate clearly with a `# TODO: implement` comment where source-specific logic is needed.

Do not truncate any of the 10 files listed above — they must be complete and runnable.
```
