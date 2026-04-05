select
    charge_id                                           as transaction_id,
    lower(email)                                        as email,
    amount,
    currency,
    created_at,
    payment_method_type,
    case
        when wc_order_id is not null        then 'woocommerce'
        when lower(vendor) = 'learnworlds'  then 'learnworlds'
        when invoice is not null            then 'learnworlds'
        else                                     'unknown'
    end                                             as platform

from {{ ref('stg_stripe_charges') }}

union all

select
    cast(date as string)                                as transaction_id,
    null                                                as email,
    sum(estimated_revenue)                              as amount,
    'usd'                                               as currency,
    cast(date as timestamp)                             as created_at,
    null                                                as payment_method_type,
    'youtube'                                           as platform

from {{ ref('stg_youtube') }}
where estimated_revenue > 0
group by date

union all

select
    cast(order_id as string)                            as transaction_id,
    email,
    order_total                                         as amount,
    currency,
    created_at,
    payment_method                                      as payment_method_type,
    'woocommerce'                                       as platform

from {{ ref('stg_woocommerce_orders') }}
where payment_method in ('ppcp-gateway', 'woocommerce_payments')