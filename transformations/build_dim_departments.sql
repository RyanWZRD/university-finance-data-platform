-- Build dim_departments from fact_transactions

CREATE OR REPLACE TABLE dim_departments AS
SELECT DISTINCT
    department_id
FROM fact_transactions
WHERE department_id IS NOT NULL;
