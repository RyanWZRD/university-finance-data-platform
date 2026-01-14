import pandas as pd
import pytest

from ingestion.validate_raw_data import ALLOWED_TRANSACTION_TYPES


def test_allowed_transaction_types():
    assert "EXPENSE" in ALLOWED_TRANSACTION_TYPES
    assert "INCOME" in ALLOWED_TRANSACTION_TYPES
    assert "REFUND" in ALLOWED_TRANSACTION_TYPES
    assert "TRANSFER" not in ALLOWED_TRANSACTION_TYPES
