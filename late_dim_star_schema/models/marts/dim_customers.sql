with known_customers as (
    select
        customer_id,
        full_name,
        email,
        country,
        segment,
        false as is_inferred,
        updated_at as source_updated_at
    from {{ ref('stg_customers') }}
),

late_fact_customers as (
    select distinct
        o.customer_id
    from {{ ref('stg_orders') }} o
    left join known_customers c
        on o.customer_id = c.customer_id
    where o.customer_id is not null
      and c.customer_id is null
),

inferred_customers as (
    select
        customer_id,
        'Unknown Customer (' || customer_id || ')' as full_name,
        null::varchar as email,
        'Unknown' as country,
        'Unassigned' as segment,
        true as is_inferred,
        current_timestamp()::timestamp_ntz as source_updated_at
    from late_fact_customers
),

unioned as (
    select * from known_customers
    union all
    select * from inferred_customers
)

select
    md5(customer_id) as customer_key,
    customer_id,
    full_name,
    email,
    country,
    segment,
    is_inferred,
    source_updated_at,
    current_timestamp()::timestamp_ntz as dbt_updated_at
from unioned