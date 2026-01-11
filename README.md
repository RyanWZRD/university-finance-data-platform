# University Finance Data Platform

## Overview
This project demonstrates an end-to-end data engineering pipeline designed to support reliable, auditable financial reporting in a university context.

The pipeline ingests raw financial data, applies structured transformations, and loads analytics-ready data into a warehouse for reporting and analysis.

## Problem Statement
University finance teams often rely on manual processes, spreadsheets, and disconnected systems to produce financial reports.  
This can lead to:
- Inconsistent figures
- Limited auditability
- Slow reporting cycles

This project shows how a modern data engineering approach can improve accuracy, transparency, and scalability.

## High-Level Architecture
The platform follows a standard ELT (Extract, Load, Transform) pattern:

1. Raw financial data is ingested from source systems
2. Data is stored in a raw layer
3. Transformations are applied using analytics engineering best practices
4. Clean, structured tables are produced for reporting

## Technology Stack (Planned)
- SQL
- Python
- AWS (S3, Redshift, Athena)
- dbt
- Apache Airflow
- Git & GitHub

## Project Structure
- `architecture/` – Architecture diagrams and design notes
- `data/` – Raw and staged datasets
- `ingestion/` – Data extraction and validation scripts
- `transformations/` – dbt models and tests
- `orchestration/` – Airflow DAGs
- `warehouse/` – Warehouse schemas and performance notes
- `analytics_examples/` – Example analytical queries
- `infra/` – Infrastructure and IAM documentation
## Data Quality & Validation
This project applies finance-aware data quality rules to ensure only auditable, reporting-safe data proceeds downstream.

Validation logic:
- Enforces required finance fields
- Applies transaction-type sign rules
- Separates CRITICAL failures from WARNING conditions
- Produces an auditable validation report
- Fails the pipeline on critical finance control breaches

Detailed rules and rationale are documented here:  
📄 `architecture/data_quality_rules.md`

## Current Status
🚧 Project in active development.  
Initial repository structure and documentation completed.

## Future Improvements
- Add automated data quality checks
- Implement incremental loading
- Extend to near–real-time ingestion
- Add role-based access controls

## Author
Finance professional transitioning into data engineering, with a focus on building robust and auditable data platforms for financial reporting.
