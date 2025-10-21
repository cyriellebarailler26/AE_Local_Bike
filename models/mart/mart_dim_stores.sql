with stores as (
    select distinct
        store_id,
        store_name,
        store_city,
        store_state
    from {{ ref('int_local_bike__stores_summary') }}
)

select * from stores
