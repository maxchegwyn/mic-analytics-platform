with source as (
    select * from {{ source('raw_mailchimp', 'members') }}
),

cleaned as (
    select
        id                                          as member_id,
        lower(trim(email_address))                  as email,
        status,
        timestamp_signup                            as subscribed_at,
        timestamp_opt                               as opted_in_at,
        last_changed                                as last_changed_at,
        merge_fields__fname                         as first_name,
        merge_fields__ptype                         as personality_type,
        merge_fields__pdaurl                        as pda_url,
        stats__avg_open_rate                        as avg_open_rate,
        stats__avg_click_rate                       as avg_click_rate
    from source
    qualify row_number() over (
        partition by lower(trim(email_address))
        order by last_changed desc
    ) = 1
)

select * from cleaned