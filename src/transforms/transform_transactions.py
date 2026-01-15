import logging
from pathlib import Path

import pandas as pd


def transform_transactions() -> pd.DataFrame:
    processed_path = Path("data/processed/transactions_clean.csv")
    if not processed_path.exists():
        raise FileNotFoundError(f"Missing cleaned input file: {processed_path}")

    df = pd.read_csv(processed_path)

    # --- Transform 1: parse dates ---
    df["transaction_date"] = pd.to_datetime(df["transaction_date"], errors="raise")

    # --- Transform 2: add date parts (analytics-friendly) ---
    df["year"] = df["transaction_date"].dt.year
    df["month"] = df["transaction_date"].dt.month
    df["year_month"] = df["transaction_date"].dt.to_period("M").astype(str)

    # --- Transform 3: normalize sign conventions ---
    # INCOME positive, EXPENSE negative, REFUND negative
    sign_map = {"INCOME": 1, "EXPENSE": -1, "REFUND": -1}
    df["amount_normalized"] = df["amount"].abs() * df["transaction_type"].map(sign_map)

    # Avoid float weirdness for currency
    df["amount_normalized"] = df["amount_normalized"].round(2)

    # --- Output: analytics dataset ---
    gold_dir = Path("data/gold")
    gold_dir.mkdir(parents=True, exist_ok=True)

    out_path = gold_dir / "transactions_analytics.csv"
    df.to_csv(out_path, index=False)
    logging.info(f"Wrote analytics dataset to: {out_path}")

    # --- Aggregate: department x month summary ---
    summary_by_type = (
        df.pivot_table(
            index=["department_id", "year_month"],
            columns="transaction_type",
            values="amount_normalized",
            aggfunc="sum",
            fill_value=0.0,
        )
        .reset_index()
    )

    # Ensure columns always exist
    for col in ["INCOME", "EXPENSE", "REFUND"]:
        if col not in summary_by_type.columns:
            summary_by_type[col] = 0.0

    summary_by_type = summary_by_type.rename(
        columns={
            "INCOME": "total_income",
            "EXPENSE": "total_expense",
            "REFUND": "total_refund",
        }
    )

    summary_by_type["net"] = (
        summary_by_type["total_income"]
        + summary_by_type["total_expense"]
        + summary_by_type["total_refund"]
    )

    money_cols = ["total_income", "total_expense", "total_refund", "net"]
    summary_by_type[money_cols] = summary_by_type[money_cols].round(2)

    summary_path = gold_dir / "department_monthly_summary.csv"
    summary_by_type.to_csv(summary_path, index=False)
    logging.info(f"Wrote department summary to: {summary_path}")
    logging.info("\n" + summary_by_type.sort_values(["department_id", "year_month"]).to_string(index=False))


    # Helpful compact log (no huge tables unless you want them)
    logging.info(f"Transform rows: {len(df)} | Departments: {df['department_id'].nunique()} | Months: {df['year_month'].nunique()}")

    return df


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    transform_transactions()
