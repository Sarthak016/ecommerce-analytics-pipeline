
    
    

select
    order_date as unique_field,
    count(*) as n_records

from "airflow"."analytics_analytics"."mart_daily_sales"
where order_date is not null
group by order_date
having count(*) > 1


