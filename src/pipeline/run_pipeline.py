import argparse
import logging
from pathlib import Path
import yaml

from src.ingestion.load_csv import load_transactions_csv
from src.transforms.transform_transactions import transform_transactions


def load_config(path: Path) -> dict:
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")
    with path.open("r") as f:
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

    load_transactions_csv(
        raw_path=raw_path,
        processed_dir=processed_dir,
    )
    logging.info("Ingestion complete")

    transform_transactions(
        processed_dir=processed_dir,
        gold_dir=gold_dir,
    )
    logging.info("Transform complete")

    logging.info("Pipeline finished successfully")


if __name__ == "__main__":
    run()
