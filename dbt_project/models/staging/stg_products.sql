select
    product_id,
    product_name,
    category,
    brand,
    price,
    discount_pct,
    rating,
    stock,
    _extracted_date
from {{ source('staging', 'stg_products') }}
