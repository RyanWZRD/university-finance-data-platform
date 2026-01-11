-- Build fact_transactions from validated data

CREATE OR REPLACE TABLE fact_transactions AS
SELECT
    transaction_id,
    transaction_date,
    department_id,
    transaction_type,
    amount
FROM read_parquet('data/staging/transactions_valid.parquet');
