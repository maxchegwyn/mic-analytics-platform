with

members as (
    select
        email,
        personality_type,
        pda_url,
        status                          as mailchimp_status,
        subscribed_at,
        avg_open_rate,
        avg_click_rate
    from {{ ref('stg_mailchimp_members') }}
),

jotform_agg as (
    select
        email,
        min(submitted_at)               as first_submission_date,
        count(submission_id)            as submission_count
    from {{ ref('stg_jotform') }}
    group by email
),

jotform_latest as (
    select
        email,
        first_name,
        clarity_e,
        clarity_s,
        clarity_t,
        clarity_j
    from {{ ref('stg_jotform') }}
    qualify row_number() over (partition by email order by submitted_at desc) = 1
),

wc_agg as (
    select
        lower(trim(email))              as email,
        sum(line_total)                 as total_wc_spend,
        min(created_at)                 as first_purchase_date,
        count(distinct order_id)        as purchase_count
    from {{ ref('stg_woocommerce_orders') }}
    group by lower(trim(email))
),

stripe_agg as (
    select
        email,
        sum(amount)                     as total_stripe_spend
    from {{ ref('stg_stripe_charges') }}
    group by email
)

select
    m.email,
    j_latest.first_name,
    m.personality_type,
    m.pda_url,
    m.mailchimp_status,
    m.subscribed_at,
    m.avg_open_rate,
    m.avg_click_rate,
    j_agg.first_submission_date,
    j_agg.submission_count,
    j_latest.clarity_e                  as latest_clarity_e,
    j_latest.clarity_s                  as latest_clarity_s,
    j_latest.clarity_t                  as latest_clarity_t,
    j_latest.clarity_j                  as latest_clarity_j,
    wc.total_wc_spend,
    wc.first_purchase_date,
    wc.purchase_count,
    s.total_stripe_spend
from members as m
left join jotform_agg as j_agg
    on m.email = j_agg.email
left join jotform_latest as j_latest
    on m.email = j_latest.email
left join wc_agg as wc
    on m.email = wc.email
left join stripe_agg as s
    on m.email = s.email