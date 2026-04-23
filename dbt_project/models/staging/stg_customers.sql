-- Staging model for customers
-- Reads from staging schema loaded by PySpark ETL

with source as (
    select * from staging.customers
),

renamed as (
    select
        customer_id,
        name         as customer_name,
        email,
        city,
        state,
        signup_date,
        segment,
        ingested_at
    from source
    where customer_id is not null
      and email is not null
)

select * from renamed