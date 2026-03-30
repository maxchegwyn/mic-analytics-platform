with source as (
    select * from {{ source('raw_youtube', 'raw_channel_daily_metrics') }}
),

cleaned as (
    select
        cast(date as date)                                    as date,
        coalesce(views, 0)                                    as views,
        coalesce(estimated_minutes_watched, 0) / 60.0         as watch_hours,
        coalesce(subscribers_gained, 0)                       as subscribers_gained,
        coalesce(subscribers_lost, 0)                         as subscribers_lost,
        coalesce(subscribers_net, 0)                          as subscribers_net,
        coalesce(estimated_revenue, 0)                        as estimated_revenue,
        coalesce(likes, 0)                                    as likes,
        coalesce(comments, 0)                                 as comments,
        coalesce(shares, 0)                                   as shares
    from source
    qualify row_number() over (
        partition by cast(date as date)
        order by _dlt_load_id desc
    ) = 1
)

select * from cleaned