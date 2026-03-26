with parent as (
    select * from {{ source('raw_mailchimp', 'email_activity') }}
),

child as (
    select * from {{ source('raw_mailchimp', 'email_activity__activity') }}
),

joined as (
    select
        lower(trim(parent.email_address))   as email,
        parent.campaign_id,
        child.action,
        child.timestamp                     as actioned_at,
        child.url
    from child
    left join parent
        on child._dlt_parent_id = parent._dlt_id
)

select * from joined