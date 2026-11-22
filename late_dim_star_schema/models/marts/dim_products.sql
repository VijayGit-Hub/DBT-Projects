select
    md5(product_id) as product_key,
    product_id,
    product_name,
    category,
    unit_price,
    current_timestamp()::timestamp_ntz as dbt_updated_at
from {{ ref('stg_products') }}