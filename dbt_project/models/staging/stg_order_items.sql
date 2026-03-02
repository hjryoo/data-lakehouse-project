select
    order_id,
    product_id,
    product_name,
    unit_price,
    quantity,
    line_total,
    discount_pct,
    discounted_total,
    _extracted_date
from {{ source('staging', 'stg_order_items') }}
