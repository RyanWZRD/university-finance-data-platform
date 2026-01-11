# Warehouse Schema — University Finance Platform

## Design Principles
- Star schema for analytics
- Clear separation between facts and dimensions
- Finance-friendly grain and naming
- Optimised for reporting, not ingestion

## Fact Table

### fact_transactions
**Grain:** one row per financial transaction

**Columns**
- transaction_id (PK)
- transaction_date (FK → dim_dates.date)
- department_id (FK → dim_departments.department_id)
- transaction_type (EXPENSE | INCOME | REFUND)
- amount (signed numeric)

**Notes**
- Only validated (clean) transactions are included
- Rejected records never enter the warehouse

## Dimension Tables

### dim_departments
**Grain:** one row per department

**Columns**
- department_id (PK)

**Notes**
- Enriched later with names and hierarchies

### dim_dates
**Grain:** one row per calendar date

**Columns**
- date (PK)
- year
- month
- quarter

**Notes**
- Enables time-based reporting and period rollups
