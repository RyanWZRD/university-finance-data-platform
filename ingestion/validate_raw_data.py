from pathlib import Path
import pandas as pd

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
    if not IN_PATH.exists():
        raise FileNotFoundError(f"Missing input file: {IN_PATH}. Run extract_csv_data.py first.")

    df = pd.read_parquet(IN_PATH)

    issues = []

    # 1) Required columns present
    missing_cols = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing_cols:
        issues.append(f"Missing required columns: {missing_cols}")

    # 2) Missing values in required fields
    for col in REQUIRED_COLS:
        if col in df.columns:
            missing_count = df[col].isna().sum()
            if missing_count:
                issues.append(f"Missing values in {col}: {missing_count}")

    # 3) Parse dates
    if "transaction_date" in df.columns:
        parsed = pd.to_datetime(df["transaction_date"], errors="coerce")
        bad_dates = parsed.isna().sum()
        if bad_dates:
            issues.append(f"Invalid transaction_date values: {bad_dates}")
        df["transaction_date"] = parsed

    # 4) Amount must be non-negative for this example (you can later allow refunds)
    if "amount" in df.columns:
        df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
        negative_amounts = (df["amount"] < 0).sum()
        if negative_amounts:
            issues.append(f"Negative amount rows: {negative_amounts}")

    # Save cleaned output (even if issues exist — real pipelines often do)
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(OUT_PATH, index=False)

    # Write report
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

    print(f"Wrote cleaned parquet to {OUT_PATH}")
    print(f"Wrote validation report to {REPORT_PATH}")

if __name__ == "__main__":
    main()
