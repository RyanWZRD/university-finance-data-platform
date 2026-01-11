# Data Quality Rules (University Finance Pipeline)

## Purpose
This project ingests and validates university finance transaction data for reporting.  
These rules act as a **data quality gate** to prevent invalid or unauditable records from entering downstream analytics tables.

The validation step produces:
- `data/staging/transactions_clean.parquet` (cleaned/typed output)
- `data/staging/validation_report.txt` (audit trail)
- A non-zero exit code if CRITICAL issues exist (so orchestration can fail the run)

## Severity Levels
- **CRITICAL**: the pipeline should fail. Data cannot be trusted for reporting/audit purposes.
- **WARNING**: pipeline may continue, but issues must be reviewed.

## Current Rule Set

### Rule 1 — Required columns must exist (CRITICAL)
**Fields:** `transaction_id`, `transaction_date`, `department_id`, `transaction_type`, `amount`  
**Reason:** Without these fields, the dataset cannot be reconciled, modelled, or reliably reported.

### Rule 2 — Required fields must not be missing (CRITICAL)
**Checks:** null/blank values in required fields  
**Reason:** Missing finance fields break allocations, approvals, reporting rollups, and audit trails.

### Rule 3 — `transaction_date` must be parseable (CRITICAL)
**Checks:** invalid date values (e.g., "INVALID_DATE")  
**Reason:** Financial reporting depends on correct periods (month-end, quarter-end, year-end). Invalid dates compromise reporting integrity.

### Rule 4 — `amount` must be numeric (CRITICAL)
**Checks:** non-numeric amounts or missing values  
**Reason:** Aggregations, budget vs actual analysis, and reconciliation require numeric values.

### Rule 5 — `transaction_type` must be allowed (CRITICAL)
**Allowed values:** `EXPENSE`, `INCOME`, `REFUND`  
**Reason:** Prevents uncontrolled categories entering the warehouse (supports consistent reporting).

### Rule 6 — Sign rules by `transaction_type` (CRITICAL)
**Rules:**
- `EXPENSE` → amount must be **> 0**
- `INCOME` → amount must be **> 0**
- `REFUND` → amount must be **< 0**

**Reason:** Sign consistency prevents incorrect totals and supports accurate rollups and financial controls.

### Rule 7 — Duplicate `transaction_id` detection (WARNING)
**Checks:** duplicated transaction IDs  
**Reason:** Duplicate postings may indicate reprocessing or upstream duplication. This is often reviewable without blocking the entire pipeline.

## Notes on Design Choices
- The validation step writes an audit report even when CRITICAL issues exist.
- The pipeline currently fails on any CRITICAL issue (strict mode), which is appropriate for finance reporting.
- Future improvement: quarantine invalid rows into a rejected dataset while allowing valid rows to proceed (with thresholds).
