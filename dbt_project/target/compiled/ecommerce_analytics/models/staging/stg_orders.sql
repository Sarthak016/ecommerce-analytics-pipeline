-- Staging model for orders
-- Reads from staging schema loaded by PySpark ETL

with source as (
    select * from staging.orders
),

renamed as (
    select
        order_id,
        customer_id,
        product_id,
        order_date,
        quantity,
        unit_price,
        total_amount,
        lower(status) as status,
        ingested_at
    from source
    where order_id is not null
      and customer_id is not null
      and product_id is not null
)

select * from renamed