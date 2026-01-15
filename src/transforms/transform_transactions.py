import logging
from pathlib import Path

import pandas as pd


def transform_transactions(
    processed_dir: Path | None = None,
    gold_dir: Path | None = None,
) -> pd.DataFrame:
    """
    Transform cleaned transactions into analytics and gold aggregates.

    Args:
        processed_dir: Directory containing cleaned input data.
        gold_dir: Directory for gold outputs.

    Returns:
        Transformed analytics DataFrame.
    """
    processed_dir = processed_dir or Path("data/processed")
    gold_dir = gold_dir or Path("data/gold")

    processed_path = processed_dir / "transactions_clean.csv"

    if not processed_path.exists():
        raise FileNotFoundError(f"Missing cleaned input file: {processed_path}")

    gold_dir.mkdir(parents=True, exist_ok=True)

    # --- Load ---
    df = pd.read_csv(processed_path)

    # --- Transform 1: parse dates ---
    df["transaction_date"] = pd.to_datetime(df["transaction_date"], errors="raise")

    # --- Transform 2: add date parts ---
    df["year"] = df["transaction_date"].dt.year
    df["month"] = df["transaction_date"].dt.month
    df["year_month"] = df["transaction_date"].dt.to_period("M").astype(str)

    # --- Transform 3: normalize amounts ---
    sign_map = {"INCOME": 1, "EXPENSE": -1, "REFUND": -1}
    df["amount_normalized"] = df["amount"].abs() * df["transaction_type"].map(sign_map)
    df["amount_normalized"] = df["amount_normalized"].round(2)

    # --- Output: analytics dataset ---
    analytics_path = gold_dir / "transactions_analytics.csv"
    df.to_csv(analytics_path, index=False)
    logging.info(f"Wrote analytics dataset to: {analytics_path}")

    # --- Aggregate: department x month ---
    summary = (
        df.pivot_table(
            index=["department_id", "year_month"],
            columns="transaction_type",
            values="amount_normalized",
            aggfunc="sum",
            fill_value=0.0,
        )
        .reset_index()
    )

    for col in ["INCOME", "EXPENSE", "REFUND"]:
        if col not in summary.columns:
            summary[col] = 0.0

    summary = summary.rename(
        columns={
            "INCOME": "total_income",
            "EXPENSE": "total_expense",
            "REFUND": "total_refund",
        }
    )

    summary["net"] = (
        summary["total_income"]
        + summary["total_expense"]
        + summary["total_refund"]
    )

    money_cols = ["total_income", "total_expense", "total_refund", "net"]
    summary[money_cols] = summary[money_cols].round(2)

    summary_path = gold_dir / "department_monthly_summary.csv"
    summary.to_csv(summary_path, index=False)
    logging.info(f"Wrote department summary to: {summary_path}")

    logging.info(
        f"Transform rows: {len(df)} | "
        f"Departments: {df['department_id'].nunique()} | "
        f"Months: {df['year_month'].nunique()}"
    )

    return df


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )
    transform_transactions()
