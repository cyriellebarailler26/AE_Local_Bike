with products as (
    select * from {{ ref('stg_local_bike__products') }}
),
brands as (
    select * from {{ ref('stg_local_bike__brands') }}
),
categories as (
    select * from {{ ref('stg_local_bike__categories') }}
),
stocks as (
    select * from {{ ref('stg_local_bike__stocks') }}
)

select
    p.product_id,
    p.product_name,
    b.brand_name,
    c.category_name,
    SUM(s.quantity) as stock_quantity,
    p.model_year,
    p.list_price
from products p
left join brands b on p.brand_id = b.brand_id
left join categories c on p.category_id = c.category_id
left join stocks s on p.product_id = s.product_id
GROUP BY 
    p.product_id,
    p.product_name,
    b.brand_name,
    c.category_name,
    p.model_year,
    p.list_price
