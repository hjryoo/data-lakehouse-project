with ranked as (
    select
        *,
        row_number() over (
            partition by user_id
            order by _extracted_date desc
        ) as rn
    from {{ ref('stg_users') }}
)

select
    user_id,
    first_name,
    last_name,
    full_name,
    email,
    age,
    gender,
    phone,
    city,
    state,
    country,
    company_name,
    department,
    job_title,
    _extracted_date as last_updated
from ranked
where rn = 1
