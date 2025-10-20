with sales as (
    select * from {{ ref('int_local_bike__sales_enriched') }}
)

select
    customer_id,
    customer_name,
    customer_city,
    customer_state,
    count(distinct order_id) as total_orders,
    sum(total_price) as total_spent,
    avg(total_price) as avg_order_value
from sales
group by customer_id, customer_name, customer_city, customer_state

