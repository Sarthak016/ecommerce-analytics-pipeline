-- Mart: Daily Sales Summary
-- Aggregates orders by date for trend analysis

with orders as (
    select * from {{ ref('stg_orders') }}
),

daily_sales as (
    select
        order_date,
        count(distinct order_id)             as total_orders,
        count(distinct customer_id)          as unique_customers,
        sum(total_amount)                    as daily_revenue,
        avg(total_amount)                    as avg_order_value,
        sum(quantity)                        as total_units_sold,
        sum(case when status = 'completed'
            then total_amount else 0 end)    as completed_revenue,
        sum(case when status = 'cancelled'
            then 1 else 0 end)               as cancelled_orders,
        sum(case when status = 'pending'
            then 1 else 0 end)               as pending_orders
    from orders
    group by order_date
)

select * from daily_sales
order by order_date