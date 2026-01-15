import pandas as pd
from pathlib import Path


def transform_transactions():
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

    # --- Transform 3: OPTIONAL normalization ---
    # Many finance models prefer:
    # income = positive, expense/refund = negative (or refund positive depending on your definition)
    # Your sample already has refund as negative, so we'll enforce:
    # INCOME positive, EXPENSE negative, REFUND negative
    sign_map = {"INCOME": 1, "EXPENSE": -1, "REFUND": -1}
    df["amount_normalized"] = df["amount"].abs() * df["transaction_type"].map(sign_map)

    # --- Output ---
    gold_dir = Path("data/gold")
    gold_dir.mkdir(parents=True, exist_ok=True)

    out_path = gold_dir / "transactions_analytics.csv"
    df.to_csv(out_path, index=False)
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

    summary_path = gold_dir / "department_monthly_summary.csv"
    summary_by_type.to_csv(summary_path, index=False)

    print(f"📊 Wrote department summary to: {summary_path}")
    print(summary_by_type.sort_values(['department_id', 'year_month']).head(10))


    print("✅ Transform complete")
    print(f"💾 Wrote analytics dataset to: {out_path}")
    print(f"Rows: {len(df)}")
    print(df.head())

    return df


if __name__ == "__main__":
    transform_transactions()
