import logging
from pathlib import Path

import pandas as pd

from src.validation.validate_schema import (
    SchemaValidationError,
    transactions_schema_spec,
    validate_schema,
    validate_transaction_dates,
    validate_transaction_types,
)


def load_transactions_csv(raw_path: Path | None = None) -> pd.DataFrame:
    """
    Load transactions CSV, quarantine bad rows, validate clean rows,
    and write processed outputs.

    Args:
        raw_path: Optional path to the raw CSV. If None, defaults to
                  data/raw/transactions_sample.csv

    Returns:
        Clean (validated) DataFrame.
    """
    file_path = raw_path or Path("data/raw/transactions_sample.csv")

    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    df = pd.read_csv(file_path)

    # --- Quarantine rules (Week 8/10 best practice) ---
    required_cols = [
        "transaction_id",
        "transaction_date",
        "department_id",
        "transaction_type",
        "amount",
    ]

    # Rows missing any required field
    missing_required_mask = df[required_cols].isna().any(axis=1)

    # Rows with invalid dates
    parsed_dates = pd.to_datetime(df["transaction_date"], errors="coerce")
    invalid_date_mask = parsed_dates.isna()

    # Anything bad goes to quarantine
    quarantine_mask = missing_required_mask | invalid_date_mask

    quarantine_df = df[quarantine_mask].copy()
    clean_df = df[~quarantine_mask].copy()

    if not quarantine_df.empty:
        logging.warning(f"Quarantining {len(quarantine_df)} bad rows")
        logging.warning(
            f"Quarantine reasons: missing_required={int(missing_required_mask.sum())}, "
            f"invalid_date={int(invalid_date_mask.sum())}"
        )

    df = clean_df

    # --- Schema & business validation ---
    spec = transactions_schema_spec()

    try:
        validate_schema(df, spec)
        validate_transaction_types(df)
        validate_transaction_dates(df)
        logging.info("Schema & business validation passed")
    except SchemaValidationError as e:
        logging.error(f"Validation failed: {e}")
        raise

    # --- Output ---
    processed_dir = Path("data/processed")
    processed_dir.mkdir(parents=True, exist_ok=True)

    clean_path = processed_dir / "transactions_clean.csv"
    df.to_csv(clean_path, index=False)
    logging.info(f"Wrote cleaned data to: {clean_path}")

    if not quarantine_df.empty:
        quarantine_path = processed_dir / "transactions_quarantine.csv"
        quarantine_df.to_csv(quarantine_path, index=False)
        logging.info(f"Wrote quarantined rows to: {quarantine_path}")

    logging.info(f"Clean rows: {len(df)} | Columns: {list(df.columns)}")

    return df


if __name__ == "__main__":
    # Basic standalone logging if you run this module directly:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    load_transactions_csv()
