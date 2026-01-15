![CI](https://github.com/RyanWZRD/university-finance-data-platform/actions/workflows/ci.yml/badge.svg)

# University Finance Data Platform

A small, config-driven data pipeline that ingests transactions from CSV, quarantines bad rows, validates schema + business rules, produces analytics outputs, and writes per-run metrics as JSON.

## Project structure

- `data/raw/`  
  Raw input data (e.g., `transactions_sample.csv`)

- `data/processed/`  
  Cleaned dataset + quarantined rows
  - `transactions_clean.csv`
  - `transactions_quarantine.csv`

- `data/gold/`  
  Analytics outputs
  - `transactions_analytics.csv`
  - `department_monthly_summary.csv`

- `metrics/` *(ignored by git)*  
  Run metrics JSON files (e.g., `run_YYYYMMDD_HHMMSS.json`)

- `config/`  
  YAML configuration (paths + pipeline options)

- `src/`  
  Pipeline source code
  - `src/pipeline/run_pipeline.py` (main entrypoint)
  - `src/ingestion/load_csv.py` (ingestion + quarantine + validation + processed outputs)
  - `src/transforms/transform_transactions.py` (gold outputs)
  - `src/metrics/summarize_runs.py` (summarise last N run metrics)

## Setup

### 1) Create a virtual environment (recommended)

**Windows (PowerShell):**
```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m venv .venv
source .venv/bin/activate
