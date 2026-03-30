with

daily as (
    select
        video_id,
        date,
        watch_hours,
        views,
        estimated_revenue,
        subscribers_gained
    from {{ ref('stg_youtube') }}
),

videos as (
    select
        video_id,
        publish_date
    from {{ ref('dim_videos') }}
    where publish_date is not null
),

with_quarter as (
    select
        d.video_id,
        d.date,
        d.watch_hours,
        d.views,
        d.estimated_revenue,
        d.subscribers_gained,
        v.publish_date,
        cast(
            ceil(date_diff(d.date, v.publish_date, day) / 91.25)
            as int64
        )                                       as quarter_number
    from daily as d
    inner join videos as v
        on d.video_id = v.video_id
    where d.date >= v.publish_date
),

aggregated as (
    select
        video_id,
        quarter_number,
        round(sum(watch_hours), 2)              as watch_hours,
        sum(views)                              as views,
        round(sum(estimated_revenue), 2)        as estimated_revenue,
        sum(subscribers_gained)                 as subscribers_gained,
        min(date)                               as quarter_start_date,
        max(date)                               as quarter_end_date
    from with_quarter
    group by video_id, quarter_number
)

select
    a.video_id,
    a.quarter_number,
    a.watch_hours,
    a.views,
    a.estimated_revenue,
    a.subscribers_gained,
    a.quarter_start_date,
    a.quarter_end_date,
    case
        when date_diff(
            a.quarter_end_date,
            a.quarter_start_date,
            day
        ) >= 85                                 then true
        else                                    false
    end                                         as is_complete_quarter
from aggregated as a