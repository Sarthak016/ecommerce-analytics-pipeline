
  create view "airflow"."analytics_staging"."stg_products__dbt_tmp"
    
    
  as (
    -- Staging model for products
-- Reads from staging schema loaded by PySpark ETL

with source as (
    select * from staging.products
),

renamed as (
    select
        product_id,
        product_name,
        category,
        brand,
        cost_price,
        selling_price,
        stock_quantity,
        profit_margin_pct,
        ingested_at
    from source
    where product_id is not null
      and selling_price > 0
)

select * from renamed
  );