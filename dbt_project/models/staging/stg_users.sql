select
    user_id,
    first_name,
    last_name,
    first_name || ' ' || last_name as full_name,
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
    _extracted_date
from {{ source('staging', 'stg_users') }}
