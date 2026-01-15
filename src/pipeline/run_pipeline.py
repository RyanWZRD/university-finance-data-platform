import argparse
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

import yaml

from src.ingestion.load_csv import load_transactions_csv
from src.transforms.transform_transactions import transform_transactions


def load_config(path: Path) -> dict:
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def setup_logging(level: str):
    logging.basicConfig(
        level=getattr(logging, level),
        format="%(asctime)s | %(levelname)s | %(message)s",
    )


def parse_args():
    parser = argparse.ArgumentParser(description="Run the finance data pipeline.")
    parser.add_argument(
        "--config",
        default="config/dev.yml",
        help="Path to YAML config file",
    )
    return parser.parse_args()


def write_run_metrics(metrics_dir: Path, payload: Dict[str, Any]) -> Path:
    metrics_dir.mkdir(parents=True, exist_ok=True)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = metrics_dir / f"run_{ts}.json"

    with out_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, default=str)

    return out_path


def run():
    args = parse_args()
    config_path = Path(args.config)

    config = load_config(config_path)
    setup_logging(config["logging"]["level"])

    logging.info("Starting pipeline")
    logging.info(f"Using config: {config_path}")

    raw_path = Path(config["paths"]["raw"])
    processed_dir = Path(config["paths"]["processed_dir"])
    gold_dir = Path(config["paths"]["gold_dir"])
    metrics_dir = Path(config["paths"].get("metrics_dir", "metrics"))

    quarantine_enabled = bool(config.get("pipeline", {}).get("quarantine_enabled", True))

    # --- Ingestion (returns df + metrics) ---
    df_clean, ingest_metrics = load_transactions_csv(
        raw_path=raw_path,
        processed_dir=processed_dir,
        quarantine_enabled=quarantine_enabled,
        return_metrics=True,
    )
    logging.info("Ingestion complete")

    # --- Transform (returns df) ---
    df_analytics = transform_transactions(
        processed_dir=processed_dir,
        gold_dir=gold_dir,
    )
    logging.info("Transform complete")
        # --- Net totals (trend-friendly metrics) ---
    df_calc = df_clean.copy()

if "transaction_type" in df_calc.columns and "amount" in df_calc.columns:
    income_total = float(df_calc.loc[df_calc["transaction_type"] == "income", "amount"].sum())
    expense_total = float(df_calc.loc[df_calc["transaction_type"] == "expense", "amount"].sum())
else:
    income_total = 0.0
    expense_total = 0.0

net_total = income_total - expense_total



    # --- Net totals (trend-friendly metrics) ---
    df_calc = df_analytics.copy()

    if "transaction_type" in df_calc.columns and "amount" in df_calc.columns:
        income_total = float(
            df_calc.loc[df_calc["transaction_type"] == "income", "amount"].sum()
        )
        expense_total = float(
            df_calc.loc[df_calc["transaction_type"] == "expense", "amount"].sum()
        )
    else:
        income_total = 0.0
        expense_total = 0.0

    net_total = income_total - expense_total

    # --- Build run metrics payload ---
    run_metrics: Dict[str, Any] = {
        "run": {
            "timestamp": datetime.now().isoformat(timespec="seconds"),
            "config_path": str(config_path),
        },
        "paths": {
            "raw": str(raw_path),
            "processed_dir": str(processed_dir),
            "gold_dir": str(gold_dir),
            "metrics_dir": str(metrics_dir),
        },
        "ingestion": ingest_metrics,
        "transform": {
            "analytics_rows": int(len(df_analytics)),
            "analytics_columns": list(df_analytics.columns),
            "income_total": round(income_total, 2),
            "expense_total": round(expense_total, 2),
            "net_total": round(net_total, 2),
        },
    }

    metrics_path = write_run_metrics(metrics_dir, run_metrics)
    logging.info(f"Wrote run metrics to: {metrics_path}")

    logging.info("Pipeline finished successfully")



if __name__ == "__main__":
    run()
    # Net totals (positive for income, negative for expense)


