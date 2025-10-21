with products as (
    select distinct
        product_id,
        product_name,
        brand_name,
        category_name,
        model_year,
        list_price,
        stock_quantity
    from {{ ref('int_local_bike__products_enriched') }}
)

select * from products
