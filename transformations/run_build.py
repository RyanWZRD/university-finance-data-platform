from __future__ import annotations

from pathlib import Path
import duckdb


DB_PATH = Path("warehouse.duckdb")

SQL_FILES = [
    Path("transformations/build_fact_transactions.sql"),
    Path("transformations/build_dim_dates.sql"),
    Path("transformations/build_dim_departments.sql"),
]


def main() -> None:
    con = duckdb.connect(str(DB_PATH))

    for sql_path in SQL_FILES:
        if not sql_path.exists():
            raise FileNotFoundError(f"Missing SQL file: {sql_path}")

        sql = sql_path.read_text(encoding="utf-8")
        con.execute(sql)
        print(f"✅ ran {sql_path}")

    # Quick row-count checks
    fact_count = con.execute("SELECT COUNT(*) FROM fact_transactions").fetchone()[0]
    dates_count = con.execute("SELECT COUNT(*) FROM dim_dates").fetchone()[0]
    dept_count = con.execute("SELECT COUNT(*) FROM dim_departments").fetchone()[0]

    print("\nBuild complete:")
    print(f"- fact_transactions: {fact_count}")
    print(f"- dim_dates: {dates_count}")
    print(f"- dim_departments: {dept_count}")


if __name__ == "__main__":
    main()
