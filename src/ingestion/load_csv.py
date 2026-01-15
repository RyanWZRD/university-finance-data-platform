import logging
from pathlib import Path
from typing import Any, Dict, Tuple, Union

import pandas as pd

from src.validation.validate_schema import (
    SchemaValidationError,
    transactions_schema_spec,
    validate_schema,
    validate_transaction_dates,
    validate_transaction_types,
)


def load_transactions_csv(
    raw_path: Path | None = None,
    processed_dir: Path | None = None,
    quarantine_enabled: bool = True,
    return_metrics: bool = False,
) -> Union[pd.DataFrame, Tuple[pd.DataFrame, Dict[str, Any]]]:
    """
    Load transactions CSV, optionally quarantine bad rows, validate clean rows,
    and write processed outputs.
    """
    file_path = raw_path or Path("data/raw/transactions_sample.csv")
    processed_dir = processed_dir or Path("data/processed")

    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    processed_dir.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(file_path)

    metrics: Dict[str, Any] = {
        "input_rows": int(len(df)),
        "clean_rows": None,
        "quarantined_rows": 0,
        "quarantine_missing_required": 0,
        "quarantine_invalid_date": 0,
        "quarantine_enabled": bool(quarantine_enabled),
    }

    quarantine_df = pd.DataFrame(columns=df.columns)

    if quarantine_enabled:
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
        df = df[~quarantine_mask].copy()

        metrics["quarantined_rows"] = int(len(quarantine_df))
        metrics["quarantine_missing_required"] = int(missing_required_mask.sum())
        metrics["quarantine_invalid_date"] = int(invalid_date_mask.sum())

        if len(quarantine_df) > 0:
            logging.warning(f"Quarantining {len(quarantine_df)} bad rows")
            logging.warning(
                "Quarantine reasons: "
                f"missing_required={metrics['quarantine_missing_required']}, "
                f"invalid_date={metrics['quarantine_invalid_date']}"
            )

        quarantine_path = processed_dir / "transactions_quarantine.csv"
        quarantine_df.to_csv(quarantine_path, index=False)
        logging.info(f"Wrote quarantined rows to: {quarantine_path}")

    # Validate clean data
    try:
        spec = transactions_schema_spec()
        validate_schema(df, spec)
        validate_transaction_types(df)
        validate_transaction_dates(df)
        logging.info("Schema & business validation passed")
    except SchemaValidationError as e:
        logging.error(f"Validation failed: {e}")
        raise

    clean_path = processed_dir / "transactions_clean.csv"
    df.to_csv(clean_path, index=False)
    logging.info(f"Wrote cleaned data to: {clean_path}")

    metrics["clean_rows"] = int(len(df))

    # Trend-friendly metric
    if metrics["input_rows"] > 0:
        metrics["quarantine_rate"] = round(
            metrics["quarantined_rows"] / metrics["input_rows"], 4
        )
    else:
        metrics["quarantine_rate"] = 0.0

    logging.info(f"Clean rows: {len(df)} | Columns: {list(df.columns)}")

    if return_metrics:
        return df, metrics

    return df


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )
    load_transactions_csv(return_metrics=False)
