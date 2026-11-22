select
    trim(order_id) as order_id,
    trim(customer_id) as customer_id,
    trim(product_id) as product_id,
    cast(quantity as number(10, 0)) as quantity,
    cast(order_amount as number(12, 2)) as order_amount,
    cast(order_timestamp as timestamp_ntz) as order_timestamp
from {{ ref('raw_orders') }}
where order_id is not null