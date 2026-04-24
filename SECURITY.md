# Security Policy

## Supported Versions

YHODA is under active development. Only the latest commit on the `main` branch
receives security fixes. No versioned releases have been published yet.

| Branch / Version | Supported          |
| ---------------- | ------------------ |
| `main` (latest)  | :white_check_mark: |
| Any older commit | :x:                |

## Reporting a Vulnerability

**Please do not report security vulnerabilities through public GitHub issues.**

Report vulnerabilities privately by emailing:

**security@yhoda.example.ac.uk**

Include as much of the following as possible:

- A description of the vulnerability and its potential impact
- The component or file affected (e.g. a specific module, API endpoint, or
  configuration file)
- Steps to reproduce or a proof-of-concept (if safe to share)
- Any suggested mitigations you have identified

You can expect an acknowledgement within **3 business days**. We will aim to
provide a status update — confirmed, investigating, or declined with reasoning
— within **10 business days**. Critical vulnerabilities affecting live data
infrastructure will be prioritised for immediate remediation.

We will not pursue legal action against researchers who report vulnerabilities
in good faith and follow this policy.

## Scope

This policy covers the YHODA ETL pipeline and its associated infrastructure:

- Pipeline source code in this repository (`src/yhovi_pipeline/`)
- Database schema and migration scripts (`src/yhovi_pipeline/db/`)
- CI/CD configuration (`.github/workflows/`)
- Secrets handling and environment variable management

Out of scope:

- Third-party services this pipeline consumes (Nomis, ONS, NHS Fingertips,
  DWP Stat-Xplore) — report those directly to their respective owners
- The Yorkshire Engagement Portal front-end — contact the portal team
  separately

## Security Design

### Secrets management

- All credentials (database URL, API keys) are loaded exclusively via
  `pydantic-settings` from environment variables — never hardcoded or committed
- `SecretStr` is used for all secret values; `.get_secret_value()` is only
  called at the call site immediately before use
- `.env` files are listed in `.gitignore` and must never be committed
- The only permitted exception is `db/migrations/env.py`, which reads
  `DATABASE_URL` from `os.environ` directly for Alembic compatibility

### Database access

- The pipeline connects to PostgreSQL using a least-privilege service account
  with `INSERT`, `UPDATE`, and `SELECT` only — no `DROP` or `TRUNCATE`
- All data mutations use parameterised queries via SQLAlchemy; raw string
  interpolation into SQL is prohibited
- Alembic migration scripts are the only permitted mechanism for DDL changes

### CI/CD

- GitHub Actions workflows run in isolated environments; secrets are injected
  via GitHub encrypted secrets and are never echoed to logs
- The deploy step runs only on push to `main` after lint, type-check, and test
  stages all pass

### Dependency management

- Dependencies are pinned via `uv.lock` and reviewed on each update
- `uv sync` is used for reproducible installs; `pip install` is not used in
  CI or production
- Pre-commit hooks enforce linting (`ruff`) and type-checking (`mypy`) before
  any commit reaches the remote

### Data handling

- The pipeline processes publicly available statistical data; no personal data
  (PII) is ingested or stored
- Disclosure-controlled suppressed values are stored as `NULL`, not imputed
- Access to the staging and production databases is restricted to the YHODA
  team via SSH tunnelling through the university VPN

## Known Limitations

- API keys for DWP Stat-Xplore and Nomis are stored as plain environment
  variables on the VM. Rotation procedures should be documented in the
  operational runbook before handover (June 2026).
- The shared drive mount (`/mnt/yhoda_drive`) relies on university network
  access controls rather than application-level authorisation.
