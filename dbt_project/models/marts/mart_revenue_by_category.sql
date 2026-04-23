-- Mart: Revenue by Product Category
-- Joins orders with products to analyze revenue by category

with orders as (
    select * from {{ ref('stg_orders') }}
),

products as (
    select * from {{ ref('stg_products') }}
),

category_revenue as (
    select
        p.category,
        p.brand,
        count(distinct o.order_id)           as total_orders,
        sum(o.quantity)                      as total_units_sold,
        sum(o.total_amount)                  as total_revenue,
        avg(o.total_amount)                  as avg_order_value,
        sum(o.quantity * p.cost_price)       as total_cost,
        sum(o.total_amount) -
            sum(o.quantity * p.cost_price)   as total_profit,
        avg(p.profit_margin_pct)             as avg_profit_margin
    from orders o
    left join products p on o.product_id = p.product_id
    where o.status = 'completed'
    group by p.category, p.brand
)

select * from category_revenue