import pandas as pd
from pathlib import Path

from src.validation.validate_schema import (
    SchemaValidationError,
    transactions_schema_spec,
    validate_schema,
    validate_transaction_dates,
    validate_transaction_types,
)


def load_transactions_csv():
    """
    Load, quarantine, validate, and write transactions data.
    """

    file_path = Path("data/raw/transactions_sample.csv")

    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    df = pd.read_csv(file_path)

    # --- Quarantine rules (Week 8 best practice) ---
    required_cols = [
        "transaction_id",
        "transaction_date",
        "department_id",
        "transaction_type",
        "amount",
    ]

    missing_required_mask = df[required_cols].isna().any(axis=1)
    parsed_dates = pd.to_datetime(df["transaction_date"], errors="coerce")
    invalid_date_mask = parsed_dates.isna()

    quarantine_mask = missing_required_mask | invalid_date_mask

    quarantine_df = df[quarantine_mask].copy()
    clean_df = df[~quarantine_mask].copy()

    if not quarantine_df.empty:
        print(f"🧯 Quarantining {len(quarantine_df)} bad rows")
        print(
            {
                "missing_required": int(missing_required_mask.sum()),
                "invalid_date": int(invalid_date_mask.sum()),
            }
        )

    df = clean_df

    # --- Schema & business validation ---
    spec = transactions_schema_spec()

    try:
        validate_schema(df, spec)
        validate_transaction_types(df)
        validate_transaction_dates(df)
        print("✅ Schema & business validation passed")
    except SchemaValidationError as e:
        print("❌ Validation failed")
        print(f"Reason: {e}")
        raise

    # --- Output ---
    processed_dir = Path("data/processed")
    processed_dir.mkdir(parents=True, exist_ok=True)

    clean_path = processed_dir / "transactions_clean.csv"
    df.to_csv(clean_path, index=False)
    print(f"💾 Wrote cleaned data to: {clean_path}")

    if not quarantine_df.empty:
        quarantine_path = processed_dir / "transactions_quarantine.csv"
        quarantine_df.to_csv(quarantine_path, index=False)
        print(f"🧯 Wrote quarantined rows to: {quarantine_path}")

    print(f"\nNumber of rows: {len(df)}")
    print(f"Columns: {list(df.columns)}")
    print("\nFirst 5 rows:")
    print(df.head())

    return df


if __name__ == "__main__":
    load_transactions_csv()
