with customers as (
    select distinct
        customer_id,
        customer_name,
        customer_city,
        customer_state
    from {{ ref('int_local_bike__customers_summary') }}
)

select * from customers
