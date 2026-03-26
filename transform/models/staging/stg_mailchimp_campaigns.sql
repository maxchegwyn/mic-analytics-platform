with source as (
    select * from {{ source('raw_mailchimp', 'campaigns') }}
),

cleaned as (
    select
        id                                              as campaign_id,
        status,
        emails_sent,
        cast(send_time as timestamp)                    as sent_at,
        settings__title                                 as title,
        settings__subject_line                          as subject_line,
        report_summary__opens                           as opens,
        report_summary__unique_opens                    as unique_opens,
        report_summary__clicks                          as clicks,
        report_summary__subscriber_clicks               as subscriber_clicks,
        report_summary__open_rate__v_double             as open_rate,
        report_summary__click_rate__v_double            as click_rate
    from source
    where status = 'sent'
)

select * from cleaned