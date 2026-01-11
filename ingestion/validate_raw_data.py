from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import sys
import pandas as pd

from ingestion.logging_config import setup_logging

logger = setup_logging("validate")

IN_PATH = Path("data/staging/transactions_raw.parquet")
OUT_PATH = Path("data/staging/transactions_clean.parquet")
REPORT_PATH = Path("data/staging/validation_report.txt")

REQUIRED_COLS = [
    "transaction_id",
    "transaction_date",
    "department_id",
    "transaction_type",
    "amount",
]

ALLOWED_TRANSACTION_TYPES = {"EXPENSE", "INCOME", "REFUND"}

@dataclass(frozen=True)
class Issue:
    severity: str  # "CRITICAL" or "WARNING"
    message: str
    affected_rows: int | None = None

def _count_nulls(df: pd.DataFrame, col: str) -> int:
    return int(df[col].isna().sum())

def _count_invalid_dates(series: pd.Series) -> tuple[pd.Series, int]:
    parsed = pd.to_datetime(series, errors="coerce")
    bad = int(parsed.isna().sum())
    return parsed, bad

def _count_non_numeric(series: pd.Series) -> tuple[pd.Series, int]:
    numeric = pd.to_numeric(series, errors="coerce")
    bad = int(numeric.isna().sum())
    return numeric, bad

def main() -> None:
    logger.info("Starting validation step")

    if not IN_PATH.exists():
        logger.error("Missing input parquet: %s (run extract first)", IN_PATH)
        raise FileNotFoundError(f"Missing input file: {IN_PATH}. Run extract_csv_data.py first.")

    df = pd.read_parquet(IN_PATH)
    logger.info("Loaded %d rows from %s", len(df), IN_PATH.resolve())

    issues: list[Issue] = []

    # 1) Required columns present (CRITICAL)
    missing_cols = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing_cols:
        issues.append(Issue("CRITICAL", f"Missing required columns: {missing_cols}"))
        # If required columns are missing, continuing checks may error; still write report below.

    # 2) Missing required field values (CRITICAL)
    for col in REQUIRED_COLS:
        if col in df.columns:
            missing_count = _count_nulls(df, col)
            if missing_count:
                issues.append(Issue("CRITICAL", f"Missing values in required column '{col}'", missing_count))

    # 3) Date parsing (CRITICAL if bad dates exist)
    if "transaction_date" in df.columns:
        df["transaction_date"], bad_dates = _count_invalid_dates(df["transaction_date"])
        if bad_dates:
            issues.append(Issue("CRITICAL", "Invalid 'transaction_date' values (could not parse)", bad_dates))

    # 4) Amount parsing (CRITICAL if non-numeric)
    if "amount" in df.columns:
        df["amount"], bad_amounts = _count_non_numeric(df["amount"])
        if bad_amounts:
            issues.append(Issue("CRITICAL", "Non-numeric 'amount' values (could not parse)", bad_amounts))

    # 5) Transaction type allowed values (CRITICAL)
    if "transaction_type" in df.columns:
        invalid_type_mask = ~df["transaction_type"].isin(ALLOWED_TRANSACTION_TYPES)
        invalid_type_count = int(invalid_type_mask.sum())
        if invalid_type_count:
            issues.append(Issue("CRITICAL", f"Invalid transaction_type (allowed: {sorted(ALLOWED_TRANSACTION_TYPES)})", invalid_type_count))

    # 6) Finance sign rules (CRITICAL)
    # EXPENSE: amount > 0
    # INCOME:  amount > 0
    # REFUND:  amount < 0
    if {"transaction_type", "amount"}.issubset(df.columns):
        # Only evaluate where both fields are present
        valid_mask = df["transaction_type"].notna() & df["amount"].notna()

        expense_bad = valid_mask & (df["transaction_type"] == "EXPENSE") & (df["amount"] <= 0)
        income_bad = valid_mask & (df["transaction_type"] == "INCOME") & (df["amount"] <= 0)
        refund_bad = valid_mask & (df["transaction_type"] == "REFUND") & (df["amount"] >= 0)

        expense_bad_count = int(expense_bad.sum())
        income_bad_count = int(income_bad.sum())
        refund_bad_count = int(refund_bad.sum())

        if expense_bad_count:
            issues.append(Issue("CRITICAL", "EXPENSE rows must have amount > 0", expense_bad_count))
        if income_bad_count:
            issues.append(Issue("CRITICAL", "INCOME rows must have amount > 0", income_bad_count))
        if refund_bad_count:
            issues.append(Issue("CRITICAL", "REFUND rows must have amount < 0", refund_bad_count))

    # 7) Potential duplicates (WARNING)
    if "transaction_id" in df.columns:
        dup_count = int(df["transaction_id"].duplicated().sum())
        if dup_count:
            issues.append(Issue("WARNING", "Duplicate transaction_id values found", dup_count))

    # Write outputs (we still write cleaned parquet and report for audit trail)
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(OUT_PATH, index=False)

    # Build report
    critical = [i for i in issues if i.severity == "CRITICAL"]
    warning = [i for i in issues if i.severity == "WARNING"]

    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with REPORT_PATH.open("w", encoding="utf-8") as f:
        f.write("Validation Report\n")
        f.write("=================\n\n")
        f.write(f"Rows checked: {len(df)}\n")
        f.write(f"Critical issues: {len(critical)}\n")
        f.write(f"Warnings: {len(warning)}\n\n")

        if critical:
            f.write("CRITICAL ISSUES (pipeline should fail)\n")
            f.write("------------------------------------\n")
            for item in critical:
                if item.affected_rows is None:
                    f.write(f"- {item.message}\n")
                else:
                    f.write(f"- {item.message} | affected_rows={item.affected_rows}\n")
            f.write("\n")

        if warning:
            f.write("WARNINGS (pipeline may continue)\n")
            f.write("-------------------------------\n")
            for item in warning:
                if item.affected_rows is None:
                    f.write(f"- {item.message}\n")
                else:
                    f.write(f"- {item.message} | affected_rows={item.affected_rows}\n")
            f.write("\n")

        if not issues:
            f.write("No issues found.\n")

    logger.info("Wrote cleaned parquet to %s", OUT_PATH.resolve())
    logger.info("Wrote validation report to %s", REPORT_PATH.resolve())

    if critical:
        logger.error("Validation FAILED with %d critical issue(s)", len(critical))
        # Exit non-zero so orchestration tools can fail the run
        sys.exit(1)

    if warning:
        logger.warning("Validation PASSED with %d warning(s)", len(warning))
    else:
        logger.info("Validation PASSED with no issues")

if __name__ == "__main__":
    main()
