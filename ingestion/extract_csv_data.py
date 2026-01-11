from pathlib import Path
import pandas as pd

from ingestion.logging_config import setup_logging

logger = setup_logging("extract")

RAW_PATH = Path("data/raw/transactions_sample.csv")
OUT_PATH = Path("data/staging/transactions_raw.parquet")

def main() -> None:
    logger.info("Starting extract step")

    if not RAW_PATH.exists():
        logger.error("Missing input file: %s", RAW_PATH)
        raise FileNotFoundError(f"Missing input file: {RAW_PATH}")

    if RAW_PATH.stat().st_size == 0:
        logger.error("Input file is empty: %s", RAW_PATH)
        raise ValueError(f"Input file is empty: {RAW_PATH}")

    logger.info("Reading CSV from %s", RAW_PATH.resolve())
    df = pd.read_csv(RAW_PATH)

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(OUT_PATH, index=False)

    logger.info("Loaded %d rows", len(df))
    logger.info("Wrote raw parquet to %s", OUT_PATH.resolve())

if __name__ == "__main__":
    main()
