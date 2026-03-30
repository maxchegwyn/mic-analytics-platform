select
    charge_id,
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