import os
print("RUNNING FILE:", os.path.abspath(__file__))

from pathlib import Path
import pandas as pd

print(">>> starting extract script")


from pathlib import Path
import pandas as pd
print(">>> starting extract script")
RAW_PATH = Path("data/raw/transactions_sample.csv")
OUT_PATH = Path("data/staging/transactions_raw.parquet")

def main() -> None:
    if not RAW_PATH.exists():
        raise FileNotFoundError(f"Missing input file: {RAW_PATH}")

    df = pd.read_csv(RAW_PATH)

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(OUT_PATH, index=False)

    print(f"Loaded {len(df)} rows from {RAW_PATH}")
    print(f"Wrote raw parquet to {OUT_PATH}")

if __name__ == "__main__":
    main()
