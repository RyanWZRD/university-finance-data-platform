import argparse
import json
from pathlib import Path
from typing import Any, Dict, List


def parse_args():
    p = argparse.ArgumentParser(description="Summarize recent pipeline run metrics JSON files.")
    p.add_argument("--metrics-dir", default="metrics", help="Directory containing run_*.json files")
    p.add_argument("--n", type=int, default=10, help="How many recent runs to summarize")
    return p.parse_args()


def load_json(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def main():
    args = parse_args()
    metrics_dir = Path(args.metrics_dir)

    files = sorted(metrics_dir.glob("run_*.json"), reverse=True)[: args.n]

    if not files:
        print(f"No run_*.json files found in: {metrics_dir.resolve()}")
        return

    rows: List[Dict[str, Any]] = []
    for fp in files:
        d = load_json(fp)
        run_ts = d.get("run", {}).get("timestamp")
        ingest = d.get("ingestion", {})
        transform = d.get("transform", {})

        rows.append(
            {
                "file": fp.name,
                "timestamp": run_ts,
                "input_rows": ingest.get("input_rows"),
                "clean_rows": ingest.get("clean_rows"),
                "quarantine_rate": ingest.get("quarantine_rate", 0.0),
"income_total": transform.get("income_total", 0.0),
"expense_total": transform.get("expense_total", 0.0),
"net_total": transform.get("net_total", 0.0),


            }
        )

    headers = list(rows[0].keys())
    col_widths = {h: max(len(h), max(len(str(r.get(h))) for r in rows)) for h in headers}

    def fmt_row(r: Dict[str, Any]) -> str:
        return " | ".join(str(r.get(h, "")).ljust(col_widths[h]) for h in headers)

    print(fmt_row({h: h for h in headers}))
    print("-+-".join("-" * col_widths[h] for h in headers))
    for r in rows:
        print(fmt_row(r))


if __name__ == "__main__":
    main()
