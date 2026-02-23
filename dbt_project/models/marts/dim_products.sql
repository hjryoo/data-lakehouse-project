with ranked as (
    select
        *,
        row_number() over (
            partition by product_id
            order by _extracted_date desc
        ) as rn
    from {{ ref('stg_products') }}
)

select
    product_id,
    product_name,
    category,
    brand,
    price,
    discount_pct,
    rating,
    stock,
    _extracted_date as last_updated
from ranked
where rn = 1
