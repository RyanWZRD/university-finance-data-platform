/*
Week 2 SQL Practice
Focus: CTEs + window functions (hireable SQL patterns)
*/

-- Running total of spend by month
WITH monthly_spend AS (
    SELECT
        DATE_TRUNC('month', transaction_date) AS month,
        SUM(amount) AS monthly_spend
    FROM transactions
    GROUP BY month
)
SELECT
    month,
    monthly_spend,
    SUM(monthly_spend) OVER (ORDER BY month) AS running_total
FROM monthly_spend
ORDER BY month;

-- Rank departments by spend
SELECT
    department_id,
    SUM(amount) AS department_spend,
    RANK() OVER (ORDER BY SUM(amount) DESC) AS spend_rank
FROM transactions
GROUP BY department_id;

-- De-duplicate records (keep latest)
WITH ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY transaction_id
            ORDER BY updated_at DESC
        ) AS rn
    FROM transactions
)
SELECT *
FROM ranked
WHERE rn = 1;
