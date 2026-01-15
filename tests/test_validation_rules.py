import pandas as pd
import pytest

from ingestion.validate_raw_data import ALLOWED_TRANSACTION_TYPES


def test_allowed_transaction_types():
    assert "EXPENSE" in ALLOWED_TRANSACTION_TYPES
    assert "INCOME" in ALLOWED_TRANSACTION_TYPES
    assert "REFUND" in ALLOWED_TRANSACTION_TYPES
    assert "TRANSFER" not in ALLOWED_TRANSACTION_TYPES
import pandas as pd
import pytest

from src.validation.validate_schema import (
    SchemaValidationError,
    transactions_schema_spec,
    validate_schema,
)


def test_schema_missing_required_columns_raises():
    df = pd.DataFrame({"transaction_id": ["T001"]})
    spec = transactions_schema_spec()

    with pytest.raises(SchemaValidationError):
        validate_schema(df, spec)


def test_schema_passes_with_required_columns():
    df = pd.DataFrame(
        {
            "transaction_id": ["T001"],
            "transaction_date": ["2025-10-01"],
            "department_id": ["D001"],
            "transaction_type": ["EXPENSE"],
            "amount": [10.0],
            "description": ["Test"],
        }
    )
    spec = transactions_schema_spec()

    validate_schema(df, spec)
