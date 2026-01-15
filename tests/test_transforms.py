import pandas as pd
from pathlib import Path

from src.transforms.transform_transactions import transform_transactions


def test_transform_creates_gold_outputs(tmp_path, monkeypatch):
    base = tmp_path

    # Create expected directory structure
    (base / "data/processed").mkdir(parents=True)
    (base / "data/gold").mkdir(parents=True)

    # Minimal clean input
    df = pd.DataFrame(
        {
            "transaction_id": ["T001"],
            "transaction_date": ["2025-10-01"],
            "department_id": ["D001"],
            "transaction_type": ["INCOME"],
            "amount": [100.0],
            "description": ["Test"],
        }
    )
    df.to_csv(base / "data/processed/transactions_clean.csv", index=False)

    # Run transform inside temp directory
    monkeypatch.chdir(base)

    out_df = transform_transactions()

    # Column sanity check
    assert "amount_normalized" in out_df.columns
    assert "year_month" in out_df.columns

    # Output files exist
    assert (base / "data/gold/transactions_analytics.csv").exists()
    assert (base / "data/gold/department_monthly_summary.csv").exists()
