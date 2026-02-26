with order_items_enriched as (
    select
        oi.order_id,
        oi.product_id,
        oi.quantity,
        oi.line_total,
        oi.discounted_total,
        o.user_id,
        o._extracted_date
    from {{ ref('stg_order_items') }} oi
    inner join {{ ref('stg_orders') }} o
        on oi.order_id = o.order_id
        and oi._extracted_date = o._extracted_date
)

select
    date_trunc('month', cast(_extracted_date as date)) as sales_month,
    product_id,
    user_id,
    count(distinct order_id) as num_orders,
    sum(quantity) as total_quantity,
    sum(line_total) as gross_sales,
    sum(discounted_total) as net_sales
from order_items_enriched
group by 1, 2, 3
