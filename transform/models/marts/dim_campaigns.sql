select
    campaign_id,
    title,
    subject_line,
    sent_at,
    emails_sent,
    open_rate,
    click_rate
from {{ ref('stg_mailchimp_campaigns') }}