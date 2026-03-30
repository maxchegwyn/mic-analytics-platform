with charges as (
    select * from {{ source('raw_stripe', 'charges') }}
),

cleaned as (
    select
        id                                              as charge_id,
        payment_intent                                  as payment_intent_id,
        payment_method                                  as payment_method_id,
        customer__id                                    as customer_id,
        lower(trim(customer__email))                    as email,
        amount / 100.0                                  as amount,
        amount_captured / 100.0                         as amount_captured,
        amount_refunded / 100.0                         as amount_refunded,
        currency,
        status,
        paid,
        refunded,
        disputed,
        captured,
        timestamp_seconds(created)                      as created_at,
        description,
        payment_method_details__type                    as payment_method_type,
        payment_method_details__card__brand             as card_brand,
        payment_method_details__card__country           as card_country,
        payment_method_details__card__last4             as card_last4,
        metadata__order_id                              as wc_order_id,
        metadata__vendor                                as vendor,
        invoice
    from charges
    where paid = true
        and refunded = false
)

select * from cleaned