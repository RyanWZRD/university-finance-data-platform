-- Monthly income vs expense vs net position

SELECT
    d.year,
    d.month,
    SUM(CASE WHEN f.transaction_type = 'INCOME'  THEN f.amount ELSE 0 END) AS total_income,
    SUM(CASE WHEN f.transaction_type = 'EXPENSE' THEN f.amount ELSE 0 END) AS total_expense,
    SUM(
        CASE
            WHEN f.transaction_type = 'INCOME'  THEN f.amount
            WHEN f.transaction_type = 'EXPENSE' THEN -f.amount
            WHEN f.transaction_type = 'REFUND'  THEN f.amount
            ELSE 0
        END
    ) AS net_position
FROM fact_transactions f
JOIN dim_dates d
  ON f.transaction_date = d.date
GROUP BY d.year, d.month
ORDER BY d.year, d.month;
