-- Build dim_dates from fact_transactions

CREATE OR REPLACE TABLE dim_dates AS
SELECT DISTINCT
    transaction_date AS date,
    EXTRACT(year FROM transaction_date) AS year,
    EXTRACT(month FROM transaction_date) AS month,
    EXTRACT(quarter FROM transaction_date) AS quarter
FROM fact_transactions;

