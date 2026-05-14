with orders as (
    select * from {{ source('raw_woocommerce', 'orders') }}
),

line_items as (
    select
        _dlt_parent_id,
        name                                as product_name,
        product_id,
        quantity,
        cast(total as float64)              as line_total
    from {{ source('raw_woocommerce', 'orders__line_items') }}
),

orders_cleaned as (
    select
        orders.id                                           as order_id,
        lower(trim(orders.billing__email))                  as email,
        orders.status                                       as order_status,
        cast(orders.total as float64)                       as order_total,
        lower(replace(orders.currency_symbol, '$', 'usd'))  as currency,
        orders.payment_method                               as payment_method,
        orders.payment_method_title                         as payment_method_title,
        orders.date_created                                 as created_at,
        orders.date_paid                                    as paid_at,
        orders.transaction_id                               as stripe_charge_id,
        orders._dlt_id
    from orders
    where orders.status in ('completed', 'processing')
),

joined as (
    select
        o.order_id,
        o.email,
        o.order_status,
        o.order_total,
        o.currency,
        o.payment_method,
        o.payment_method_title,
        o.created_at,
        o.paid_at,
        o.stripe_charge_id,
        li.product_name,
        li.product_id,
        li.quantity,
        li.line_total
    from orders_cleaned o
    left join line_items li
        on li._dlt_parent_id = o._dlt_id
)

select * from joined