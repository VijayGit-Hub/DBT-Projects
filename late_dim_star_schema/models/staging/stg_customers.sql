with source as (
    select * from {{ ref('raw_customers') }}
),

deduped as (
    select
        trim(customer_id) as customer_id,
        trim(full_name) as full_name,
        lower(trim(email)) as email,
        trim(country) as country,
        trim(segment) as segment,
        cast(updated_at as timestamp_ntz) as updated_at,
        row_number() over (
            partition by trim(customer_id)
            order by cast(updated_at as timestamp_ntz) desc
        ) as rn
    from source
    where customer_id is not null
)

select
    customer_id,
    full_name,
    email,
    country,
    segment,
    updated_at
from deduped
where rn = 1