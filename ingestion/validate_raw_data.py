from pathlib import Path
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

def main() -> None:
    logger.info("Starting validation step")

    if not IN_PATH.exists():
        logger.error("Missing input parquet: %s (run extract first)", IN_PATH)
        raise FileNotFoundError(f"Missing input file: {IN_PATH}. Run extract_csv_data.py first.")

    df = pd.read_parquet(IN_PATH)
    logger.info("Loaded %d rows from %s", len(df), IN_PATH.resolve())

    issues: list[str] = []

    # 1) Required columns present
    missing_cols = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing_cols:
        issues.append(f"Missing required columns: {missing_cols}")

    # 2) Missing values in required fields
    for col in REQUIRED_COLS:
        if col in df.columns:
            missing_count = int(df[col].isna().sum())
            if missing_count:
                issues.append(f"Missing values in {col}: {missing_count}")

    # 3) Parse dates
    if "transaction_date" in df.columns:
        parsed = pd.to_datetime(df["transaction_date"], errors="coerce")
        bad_dates = int(parsed.isna().sum())
        if bad_dates:
            issues.append(f"Invalid transaction_date values: {bad_dates}")
        df["transaction_date"] = parsed

    # 4) Amount numeric + detect negatives
    if "amount" in df.columns:
        df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
        negative_amounts = int((df["amount"] < 0).sum())
        if negative_amounts:
            issues.append(f"Negative amount rows: {negative_amounts}")

    # Write outputs
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(OUT_PATH, index=False)

    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with REPORT_PATH.open("w", encoding="utf-8") as f:
        f.write("Validation Report\n")
        f.write("=================\n\n")
        if issues:
            f.write("Issues found:\n")
            for item in issues:
                f.write(f"- {item}\n")
        else:
            f.write("No issues found.\n")

    logger.info("Wrote cleaned parquet to %s", OUT_PATH.resolve())
    logger.info("Wrote validation report to %s", REPORT_PATH.resolve())

    if issues:
        logger.warning("Validation completed with %d issue(s)", len(issues))
    else:
        logger.info("Validation completed with no issues")

if __name__ == "__main__":
    main()
