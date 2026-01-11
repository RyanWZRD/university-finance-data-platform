from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path
from datetime import datetime


PROJECT_ROOT = Path(__file__).resolve().parents[1]
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the full data pipeline")
    parser.add_argument(
        "--mode",
        choices=["strict", "lenient"],
        default="strict",
        help="Pipeline mode: strict stops on validation threshold breach; lenient continues",
    )
    return parser.parse_args()



def run_step(name: str, cmd: list[str]) -> None:
    """
    Run a pipeline step as a subprocess.
    Fails fast if the step returns a non-zero exit code.
    """
    print("\n" + "=" * 70)
    print(f"STEP: {name}")
    print(f"CMD : {' '.join(cmd)}")
    print(f"TIME: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

    result = subprocess.run(
        cmd,
        cwd=str(PROJECT_ROOT),
        text=True,
    )

    if result.returncode != 0:
        print("\n" + "-" * 70)
        print(f"❌ FAILED: {name} (exit code {result.returncode})")
        print("Stopping pipeline.")
        print("-" * 70)
        sys.exit(result.returncode)

    print(f"✅ SUCCESS: {name}")


def main() -> None:
    args = parse_args()

    # Use the same Python executable that is running this script
    py = sys.executable

    run_step("Extract CSV -> staging parquet", [py, "-m", "ingestion.extract_csv_data"])
    run_step(
    f"Validate + quarantine ({args.mode} mode)",
    [py, "-m", "ingestion.validate_raw_data", "--mode", args.mode],
)

    run_step("Build warehouse (facts + dims)", [py, "transformations/run_build.py"])

    print("\n🎉 Pipeline completed successfully.")


if __name__ == "__main__":
    main()
