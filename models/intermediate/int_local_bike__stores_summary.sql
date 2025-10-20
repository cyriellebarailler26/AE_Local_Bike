with sales as (
    select * from {{ ref('int_local_bike__sales_enriched') }}
)

select
    store_id,
    store_name,
    store_city,
    store_state,
    count(distinct order_id) as total_orders,
    sum(total_price) as total_revenue,
    count(distinct staff_id) as total_staffs,
    count(distinct customer_id) as total_customers
from sales
group by store_id, store_name, store_city, store_state

