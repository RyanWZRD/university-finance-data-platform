from __future__ import annotations

import argparse
from pathlib import Path
import sys
import pandas as pd

from ingestion.logging_config import setup_logging

logger = setup_logging("validate")

IN_PATH = Path("data/staging/transactions_raw.parquet")
VALID_OUT_PATH = Path("data/staging/transactions_valid.parquet")
REJECTED_OUT_PATH = Path("data/staging/transactions_rejected.parquet")
REPORT_PATH = Path("data/staging/validation_report.txt")

REQUIRED_COLS = [
    "transaction_id",
    "transaction_date",
    "department_id",
    "transaction_type",
    "amount",
]

ALLOWED_TRANSACTION_TYPES = {"EXPENSE", "INCOME", "REFUND"}

MAX_REJECT_RATE = 0.05   # 5%
MAX_REJECT_ROWS = 0      # disabled


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate finance data")
    parser.add_argument(
        "--mode",
        choices=["strict", "lenient"],
        default="strict",
        help="Validation mode: strict fails on threshold breach, lenient logs warnings only",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    logger.info("Starting validation step (quarantine + threshold mode)")

    if not IN_PATH.exists():
        raise FileNotFoundError(f"Missing input file: {IN_PATH}")

    # Load data FIRST
    df = pd.read_parquet(IN_PATH)
    logger.info("Loaded %d rows from %s", len(df), IN_PATH.resolve())

    # Ensure required columns exist
    missing_cols = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")

    df = df.copy()
    df["rejection_reasons"] = ""

    def add_reason(mask: pd.Series, reason: str) -> None:
        df.loc[mask, "rejection_reasons"] = df.loc[mask, "rejection_reasons"].apply(
            lambda existing: (existing + ("; " if existing else "") + reason)
        )

    # 1) Missing required fields
    for col in REQUIRED_COLS:
        missing_mask = df[col].isna() | df[col].astype(str).str.strip().eq("")
        if missing_mask.any():
            add_reason(missing_mask, f"missing_{col}")

    # 2) Parse transaction_date
    parsed_dates = pd.to_datetime(df["transaction_date"], errors="coerce")
    bad_date_mask = parsed_dates.isna()
    if bad_date_mask.any():
        add_reason(bad_date_mask, "invalid_transaction_date")
    df["transaction_date"] = parsed_dates

    # 3) Parse amount (no double-counting)
    raw_amount = df["amount"]
    is_amount_missing = raw_amount.isna() | raw_amount.astype(str).str.strip().eq("")
    numeric_amount = pd.to_numeric(raw_amount, errors="coerce")

    is_amount_non_numeric = (~is_amount_missing) & (numeric_amount.isna())
    if is_amount_non_numeric.any():
        add_reason(is_amount_non_numeric, "non_numeric_amount")

    df["amount"] = numeric_amount

    # 4) Transaction type allowed values
    invalid_type_mask = ~df["transaction_type"].isin(ALLOWED_TRANSACTION_TYPES)
    if invalid_type_mask.any():
        add_reason(invalid_type_mask, "invalid_transaction_type")

    # 5) Sign rules
    valid_mask = df["transaction_type"].notna() & df["amount"].notna()

    expense_bad = valid_mask & (df["transaction_type"] == "EXPENSE") & (df["amount"] <= 0)
    income_bad = valid_mask & (df["transaction_type"] == "INCOME") & (df["amount"] <= 0)
    refund_bad = valid_mask & (df["transaction_type"] == "REFUND") & (df["amount"] >= 0)

    if expense_bad.any():
        add_reason(expense_bad, "expense_amount_must_be_positive")
    if income_bad.any():
        add_reason(income_bad, "income_amount_must_be_positive")
    if refund_bad.any():
        add_reason(refund_bad, "refund_amount_must_be_negative")

    # Split valid vs rejected
    rejected_mask = df["rejection_reasons"] != ""
    rejected_df = df.loc[rejected_mask].copy()
    valid_df = df.loc[~rejected_mask].copy()

    VALID_OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    valid_df.to_parquet(VALID_OUT_PATH, index=False)
    rejected_df.to_parquet(REJECTED_OUT_PATH, index=False)

    total = len(df)
    rejected_n = len(rejected_df)
    reject_rate = rejected_n / total if total else 0.0

    # Write validation report
    with REPORT_PATH.open("w", encoding="utf-8") as f:
        f.write("Validation Report (Quarantine + Threshold Mode)\n")
        f.write("==============================================\n\n")
        f.write(f"Rows checked: {total}\n")
        f.write(f"Valid rows: {len(valid_df)}\n")
        f.write(f"Rejected rows: {rejected_n}\n")
        f.write(f"Rejection rate: {reject_rate:.2%}\n\n")

        if rejected_n:
            f.write("Rejection reasons (count)\n")
            f.write("-------------------------\n")
            reasons = (
                rejected_df["rejection_reasons"]
                .str.split("; ")
                .explode()
                .value_counts()
            )
            for reason, count in reasons.items():
                f.write(f"- {reason}: {count}\n")
        else:
            f.write("No rejected rows.\n")

    logger.info("Wrote valid parquet to %s (rows=%d)", VALID_OUT_PATH, len(valid_df))
    logger.info("Wrote rejected parquet to %s (rows=%d)", REJECTED_OUT_PATH, rejected_n)
    logger.info("Wrote validation report to %s", REPORT_PATH)

    # Threshold-based fail decision
    fail_by_rate = reject_rate > MAX_REJECT_RATE
    fail_by_rows = (MAX_REJECT_ROWS > 0) and (rejected_n > MAX_REJECT_ROWS)

    if fail_by_rate or fail_by_rows:
        if args.mode == "strict":
            logger.error(
                "Validation FAILED (strict mode): rejected=%d (%.2f%%)",
                rejected_n,
                reject_rate * 100,
            )
            sys.exit(1)
        else:
            logger.warning(
                "Validation threshold breached (lenient mode): rejected=%d (%.2f%%)",
                rejected_n,
                reject_rate * 100,
            )

    if rejected_n > 0:
        logger.warning(
            "Validation PASSED with rejects: rejected=%d (%.2f%%)",
            rejected_n,
            reject_rate * 100,
        )
    else:
        logger.info("Validation PASSED with no rejected rows")


if __name__ == "__main__":
    main()
