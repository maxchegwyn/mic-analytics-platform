with source as (
    select * from {{ source('raw_youtube', 'raw_video_daily_metrics') }}
),

cleaned as (
    select
        video_id,
        cast(date as date)                                    as date,
        coalesce(views, 0)                                    as views,
        coalesce(estimated_minutes_watched, 0) / 60.0         as watch_hours,
        coalesce(estimated_revenue, 0)                        as estimated_revenue,
        coalesce(subscribers_gained, 0)                       as subscribers_gained,
        coalesce(likes, 0)                                    as likes,
        coalesce(comments, 0)                                 as comments,
        coalesce(shares, 0)                                   as shares,
        coalesce(average_view_duration, 0)                    as average_view_duration_seconds,
        coalesce(average_view_percentage, 0)                  as average_view_percentage
    from source
    qualify row_number() over (
        partition by video_id, cast(date as date)
        order by _dlt_load_id desc
    ) = 1
)


select * from cleaned