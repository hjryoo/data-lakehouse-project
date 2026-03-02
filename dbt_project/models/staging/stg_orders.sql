select
    order_id,
    user_id,
    order_total,
    total_products,
    total_quantity,
    _extracted_date
from {{ source('staging', 'stg_orders') }}
