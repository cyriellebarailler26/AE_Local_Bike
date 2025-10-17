with orders as (
    select * from {{ ref('stg_local_bike__orders') }}
),
order_items as (
    select * from {{ ref('stg_local_bike__order_items') }}
),
customers as (
    select * from {{ ref('stg_local_bike__customers') }}
),
staffs as (
    select * from {{ ref('stg_local_bike__staffs') }}
),
stores as (
    select * from {{ ref('stg_local_bike__stores') }}
),
products as (
    select * from {{ ref('stg_local_bike__products') }}
)

select
    o.order_id,
    o.order_date,
    o.required_date,
    o.shipped_date,
    o.store_id,
    s.store_name,
    s.city as store_city,
    s.state as store_state,
    o.staff_id,
    sf.first_name || ' ' || sf.last_name as staff_name,
    o.customer_id,
    c.first_name || ' ' || c.last_name as customer_name,
    c.city as customer_city,
    c.state as customer_state,
    oi.product_id,
    p.product_name,
    p.list_price,
    oi.quantity,
    oi.quantity * p.list_price as total_price
from orders o
join order_items oi on o.order_id = oi.order_id
left join customers c on o.customer_id = c.customer_id
left join staffs sf on o.staff_id = sf.staff_id
left join stores s on o.store_id = s.store_id
left join products p on oi.product_id = p.product_id

