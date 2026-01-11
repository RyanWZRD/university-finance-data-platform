# Data Pipeline DAG — University Finance Platform

## Overview
This pipeline ingests, validates, and transforms university finance data into an analytics-ready warehouse.

The pipeline is designed to be:
- Deterministic
- Idempotent
- Fail-fast on data quality breaches
- Safe to rerun

## DAG Structure

extract_csv_data
↓
validate_raw_data
↓
build_fact_transactions
↓
build_dim_dates
↓
build_dim_departments
↓
analytics queries

## Task Descriptions

### extract_csv_data
- Reads raw CSV data
- Writes raw Parquet to staging
- No transformations applied

### validate_raw_data
- Applies finance-aware data quality rules
- Quarantines invalid records
- Enforces rejection thresholds
- Fails pipeline if thresholds exceeded

### build_fact_transactions
- Reads validated staging data
- Produces analytics-ready fact table
- Excludes rejected records

### build_dim_dates
- Builds calendar dimension
- Enables time-based aggregation

### build_dim_departments
- Builds department dimension
- Prepares data for organisational reporting

## Failure Behaviour
- Any task failure stops downstream execution
- Validation failure prevents warehouse builds
- All outputs are reproducible via rerun

## Scheduling (Future)
- Intended to run daily (e.g. overnight batch)
- Can be extended to event-driven ingestion
