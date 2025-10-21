with sales as (
    select
        s.order_id,
        s.order_date,
        s.customer_id,
        s.product_id,
        s.store_id,
        s.staff_id,
        s.quantity,
        s.list_price,
        s.total_price,
        extract(year from s.order_date) as year,
        extract(month from s.order_date) as month
    from {{ ref('int_local_bike__sales_enriched') }} s
),

-- KPI par client
customer_kpis as (
    select
        customer_id,
        sum(total_price) as customer_total_spent,
        -- customer_avg_order_value = total_spent / nb_orders_distinct
        sum(total_price) / nullif(count(distinct order_id), 0) as customer_avg_order_value,
        count(distinct order_id) as customer_total_orders
    from {{ ref('int_local_bike__sales_enriched') }}
    group by customer_id
),

enriched as (
    select
        s.*,
        sum(s.total_price) over (partition by s.store_id) as store_revenue,
        sum(s.total_price) over (partition by s.product_id) as product_revenue,
        sum(s.total_price) over (partition by s.store_id,s.year, s.month) as store_monthly_revenue,
        count(distinct s.order_id) over (partition by s.store_id) as store_orders_count,
        -- jointure des KPIs par client 
        ck.customer_total_spent,
        ck.customer_avg_order_value,
        ck.customer_total_orders

    from sales s
    left join customer_kpis ck
      on s.customer_id = ck.customer_id
)

select * from enriched
