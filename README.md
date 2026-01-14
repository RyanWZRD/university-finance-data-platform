![CI](https://github.com/RyanWZRD/university-finance-data-platform/actions/workflows/ci.yml/badge.svg)

# University Finance Data Platform

## Overview
This project demonstrates an end-to-end data engineering pipeline designed to support **reliable, auditable financial reporting** in a university context.

The platform ingests raw financial data, applies robust validation and data quality controls, and produces analytics-ready warehouse tables suitable for reporting, dashboards, and downstream analysis.

The project is intentionally designed to reflect **real-world data engineering practices**, including data validation, quarantine handling, orchestration, and warehouse modelling.

---

## Problem Statement
University finance teams often rely on manual processes, spreadsheets, and disconnected systems to produce financial reports. These approaches can lead to:

- Inconsistent figures
- Limited auditability
- Slow reporting cycles
- Weak data quality controls

This project shows how a modern data engineering pipeline can improve **accuracy, transparency, and scalability** in financial reporting.

---

## High-Level Architecture
The platform follows a standard **ELT (Extract, Load, Transform)** pattern:

1. Raw financial data is ingested from source systems
2. Data is stored in a raw and staging layer
3. Finance-aware validation rules are applied
4. Clean, structured warehouse tables are built
5. Analytics queries operate only on validated data

---

## Technology Stack
- **Python** – ingestion, validation, orchestration
- **SQL** – warehouse transformations and analytics
- **DuckDB** – local analytical warehouse (Parquet-native)
- **Parquet** – columnar storage format
- **Git & GitHub** – version control

*(Cloud tooling such as AWS, dbt, and Airflow are planned extensions.)*

---

## Project Structure
```text
architecture/        # Architecture notes and data quality rules
data/
  ├── raw/            # Raw source data
  └── staging/        # Validated, quarantined, and warehouse-ready data
ingestion/           # Extraction and validation logic
transformations/     # Warehouse build SQL and scripts
orchestration/       # Pipeline orchestration logic
warehouse/           # Warehouse schema documentation
analytics_examples/  # Example analytics queries
infra/               # Infrastructure notes (future)
