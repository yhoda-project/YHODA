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

**yhoda@sheffield.ac.uk**

Include as much of the following as possible:

- A description of the vulnerability and its potential impact
- The component or file affected (e.g. a specific module, API endpoint, or
  configuration file)
- Steps to reproduce or a proof-of-concept (if safe to share)
- Any suggested mitigations you have identified

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

### Data handling

- The pipeline processes publicly available statistical data; no personal data
  (PII) is ingested or stored
- Disclosure-controlled suppressed values are stored as `NULL`, not imputed
- Access to databases is restricted to the YHODA
  team via SSH tunnelling through the University VPN
