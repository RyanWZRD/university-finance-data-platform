/*
Week 1 SQL Practice
Focus: Core SQL concepts applied to finance-style data
*/

-- Total number of transactions
SELECT
    COUNT(*) AS total_transactions
FROM transactions;

-- Total spend by transaction type
SELECT
    transaction_type,
    SUM(amount) AS total_amount
FROM transactions
GROUP BY transaction_type
ORDER BY total_amount DESC;

-- Spend by department
SELECT
    department_id,
    SUM(amount) AS department_spend
FROM transactions
GROUP BY department_id
ORDER BY department_spend DESC;

-- Monthly spend trend
SELECT
    DATE_TRUNC('month', transaction_date) AS month,
    SUM(amount) AS monthly_spend
FROM transactions
GROUP BY month
ORDER BY month;

-- Average transaction value
SELECT
    AVG(amount) AS avg_transaction_value
FROM transactions;
