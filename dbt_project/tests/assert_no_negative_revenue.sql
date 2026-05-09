-- Singular test: assert no negative or zero revenue in orders
-- This test passes if it returns zero rows
-- Fails if any completed order has a zero or negative total amount

select
    order_id,
    total_amount
from {{ ref('stg_orders') }}
where total_amount <= 0
  and status = 'completed'
