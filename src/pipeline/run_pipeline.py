import argparse
import logging
from pathlib import Path

from src.ingestion.load_csv import load_transactions_csv
from src.transforms.transform_transactions import transform_transactions


def setup_logging(log_level: str):
    logging.basicConfig(
        level=getattr(logging, log_level),
        format="%(asctime)s | %(levelname)s | %(message)s",
    )


def parse_args():
    p = argparse.ArgumentParser(description="Run the finance data pipeline.")
    p.add_argument(
        "--raw",
        default="data/raw/transactions_sample.csv",
        help="Path to raw transactions CSV",
    )
    p.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level",
    )
    return p.parse_args()


def run():
    args = parse_args()
    setup_logging(args.log_level)

    raw_path = Path(args.raw)

    logging.info("Starting pipeline")
    logging.info(f"Raw input: {raw_path}")

    load_transactions_csv(raw_path=raw_path)
    logging.info("Ingestion complete")

    transform_transactions()
    logging.info("Transform complete")

    logging.info("Pipeline finished successfully")


if __name__ == "__main__":
    run()
