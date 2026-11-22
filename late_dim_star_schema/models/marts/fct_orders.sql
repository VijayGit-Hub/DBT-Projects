with orders as (
    select * from {{ ref('stg_orders') }}
),

customers as (
    select customer_key, customer_id, is_inferred
    from {{ ref('dim_customers') }}
),

products as (
    select product_key, product_id
    from {{ ref('dim_products') }}
)

select
    o.order_id,
    c.customer_key,
    p.product_key,
    o.customer_id as customer_natural_id,
    o.product_id as product_natural_id,
    o.order_timestamp,
    o.quantity,
    o.order_amount,
    coalesce(c.is_inferred, true) as has_inferred_customer,
    current_timestamp()::timestamp_ntz as dbt_updated_at
from orders o
left join customers c
    on o.customer_id = c.customer_id
left join products p
    on o.product_id = p.product_id