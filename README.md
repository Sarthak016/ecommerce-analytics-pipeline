# Ecommerce Analytics Pipeline

An end-to-end data engineering pipeline built with PySpark, Apache Airflow, dbt, and PostgreSQL — processing raw ecommerce data into analytics-ready insights automatically on a daily schedule.

## Architecture

## Tech Stack

- **PySpark** — Data ingestion, cleaning, and transformation
- **Apache Airflow** — Pipeline orchestration and scheduling
- **dbt Core** — Analytics modeling and data quality testing
- **PostgreSQL** — Data warehouse
- **Docker** — Containerized infrastructure

## Pipeline Flow

1. Raw CSV files (orders, customers, products) are ingested daily
2. PySpark ETL jobs clean and validate the data — handling nulls, deduplication, type casting, and business logic
3. Clean data is loaded into PostgreSQL staging schema
4. dbt staging models standardize and document the data
5. dbt mart models generate analytics-ready tables with business insights
6. Airflow orchestrates all tasks with the ETL jobs running in parallel

## Project Structure

## Data Models

### Staging Layer
- `stg_orders` — Cleaned and validated orders
- `stg_customers` — Deduplicated customer records
- `stg_products` — Standardized product catalog

### Analytics Layer
- `mart_customer_orders` — Customer revenue summary and order history
- `mart_revenue_by_category` — Revenue and profit by product category
- `mart_daily_sales` — Daily sales trends and KPIs

## Data Quality

dbt tests are run automatically after every pipeline execution:
- `not_null` checks on all primary keys
- `unique` checks on all identifiers
- `accepted_values` checks on status fields
- Referential integrity checks between models

## Setup Instructions

### Prerequisites
- Docker Desktop
- Git

### Run Locally

```bash
# Clone the repo
git clone https://github.com/Sarthak016/ecommerce-analytics-pipeline.git
cd ecommerce-analytics-pipeline

# Start the stack
docker-compose up airflow-init
docker-compose up -d airflow-webserver airflow-scheduler postgres

# Open Airflow UI
# Go to http://localhost:8080
# Username: admin | Password: admin

# Trigger the pipeline manually from the UI
```

## Key Results

- Processes 3 source tables daily with full data quality validation
- PySpark ETL runs in parallel reducing execution time
- dbt generates 3 analytics marts powering business insights
- Full pipeline completes in under 5 minutes