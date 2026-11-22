select
    trim(product_id) as product_id,
    trim(product_name) as product_name,
    trim(category) as category,
    cast(unit_price as number(12, 2)) as unit_price
from {{ ref('raw_products') }}
where product_id is not null