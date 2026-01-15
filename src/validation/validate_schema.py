from __future__ import annotations

from dataclasses import dataclass
import pandas as pd


@dataclass(frozen=True)
class SchemaSpec:
    required_columns: tuple[str, ...]
    not_null_columns: tuple[str, ...] = ()
    numeric_columns: tuple[str, ...] = ()


class SchemaValidationError(ValueError):
    """Raised when incoming data fails schema or business validation."""


def transactions_schema_spec() -> SchemaSpec:
    return SchemaSpec(
        required_columns=(
            "transaction_id",
            "transaction_date",
            "department_id",
            "transaction_type",
            "amount",
            "description",  # keep present (can be nullable)
        ),
        not_null_columns=(
            "transaction_id",
            "transaction_date",
            "department_id",
            "transaction_type",
            "amount",
        ),
        numeric_columns=("amount",),
    )


def validate_schema(df: pd.DataFrame, spec: SchemaSpec) -> None:
    # 1) Duplicate column names
    if df.columns.duplicated().any():
        dupes = df.columns[df.columns.duplicated()].tolist()
        raise SchemaValidationError(f"Duplicate column names found: {dupes}")

    # 2) Required columns present
    missing = [c for c in spec.required_columns if c not in df.columns]
    if missing:
        raise SchemaValidationError(f"Missing required columns: {missing}")

    # 3) Not-null checks
    null_violations = {}
    for c in spec.not_null_columns:
        nulls = int(df[c].isna().sum())
        if nulls > 0:
            null_violations[c] = nulls
    if null_violations:
        raise SchemaValidationError(f"Nulls found in required fields: {null_violations}")

    # 4) Numeric checks (coercion test, does NOT mutate df)
    numeric_issues = {}
    for c in spec.numeric_columns:
        coerced = pd.to_numeric(df[c], errors="coerce")
        bad = int(coerced.isna().sum() - df[c].isna().sum())
        if bad > 0:
            numeric_issues[c] = bad
    if numeric_issues:
        raise SchemaValidationError(f"Non-numeric values found in numeric fields: {numeric_issues}")


def validate_transaction_types(df: pd.DataFrame) -> None:
    allowed = {"INCOME", "EXPENSE", "REFUND"}
    invalid = df.loc[~df["transaction_type"].isin(allowed), "transaction_type"].unique()
    if len(invalid) > 0:
        raise SchemaValidationError(
            f"Invalid transaction_type values: {invalid.tolist()}"
        )


def validate_transaction_dates(df: pd.DataFrame) -> None:
    parsed = pd.to_datetime(df["transaction_date"], errors="coerce")
    bad = parsed.isna()
    if bad.any():
        bad_values = df.loc[bad, "transaction_date"].unique().tolist()
        raise SchemaValidationError(f"Invalid transaction_date values: {bad_values}")
