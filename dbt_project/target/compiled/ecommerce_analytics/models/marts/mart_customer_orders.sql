-- Mart: Customer Orders Summary
-- Joins orders with customers to give a full picture per customer

with orders as (
    select * from "airflow"."analytics_staging"."stg_orders"
),

customers as (
    select * from "airflow"."analytics_staging"."stg_customers"
),

customer_order_summary as (
    select
        c.customer_id,
        c.customer_name,
        c.email,
        c.city,
        c.state,
        c.segment,
        count(o.order_id)                    as total_orders,
        sum(o.total_amount)                  as total_revenue,
        avg(o.total_amount)                  as avg_order_value,
        min(o.order_date)                    as first_order_date,
        max(o.order_date)                    as last_order_date,
        sum(case when o.status = 'completed'
            then o.total_amount else 0 end)  as completed_revenue,
        sum(case when o.status = 'cancelled'
            then 1 else 0 end)               as cancelled_orders
    from customers c
    left join orders o on c.customer_id = o.customer_id
    group by
        c.customer_id, c.customer_name, c.email,
        c.city, c.state, c.segment
)

select * from customer_order_summary