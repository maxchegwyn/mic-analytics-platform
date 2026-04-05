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
        case
            when d.date >= v.publish_date
             and d.date <  date_add(v.publish_date, interval 3  month) then 1
            when d.date >= date_add(v.publish_date, interval 3  month)
             and d.date <  date_add(v.publish_date, interval 6  month) then 2
            when d.date >= date_add(v.publish_date, interval 6  month)
             and d.date <  date_add(v.publish_date, interval 9  month) then 3
            when d.date >= date_add(v.publish_date, interval 9  month)
             and d.date <  date_add(v.publish_date, interval 12 month) then 4
            when d.date >= date_add(v.publish_date, interval 12 month)
             and d.date <  date_add(v.publish_date, interval 15 month) then 5
            when d.date >= date_add(v.publish_date, interval 15 month)
             and d.date <  date_add(v.publish_date, interval 18 month) then 6
            when d.date >= date_add(v.publish_date, interval 18 month)
             and d.date <  date_add(v.publish_date, interval 21 month) then 7
            when d.date >= date_add(v.publish_date, interval 21 month)
             and d.date <  date_add(v.publish_date, interval 24 month) then 8
            when d.date >= date_add(v.publish_date, interval 24 month)
             and d.date <  date_add(v.publish_date, interval 27 month) then 9
            when d.date >= date_add(v.publish_date, interval 27 month)
             and d.date <  date_add(v.publish_date, interval 30 month) then 10
            when d.date >= date_add(v.publish_date, interval 30 month)
             and d.date <  date_add(v.publish_date, interval 33 month) then 11
            when d.date >= date_add(v.publish_date, interval 33 month)
             and d.date <  date_add(v.publish_date, interval 36 month) then 12
            when d.date >= date_add(v.publish_date, interval 36 month)
             and d.date <  date_add(v.publish_date, interval 39 month) then 13
            when d.date >= date_add(v.publish_date, interval 39 month)
             and d.date <  date_add(v.publish_date, interval 42 month) then 14
            when d.date >= date_add(v.publish_date, interval 42 month)
             and d.date <  date_add(v.publish_date, interval 45 month) then 15
            when d.date >= date_add(v.publish_date, interval 45 month)
             and d.date <  date_add(v.publish_date, interval 48 month) then 16
            when d.date >= date_add(v.publish_date, interval 48 month)
             and d.date <  date_add(v.publish_date, interval 51 month) then 17
            when d.date >= date_add(v.publish_date, interval 51 month)
             and d.date <  date_add(v.publish_date, interval 54 month) then 18
            when d.date >= date_add(v.publish_date, interval 54 month)
             and d.date <  date_add(v.publish_date, interval 57 month) then 19
            when d.date >= date_add(v.publish_date, interval 57 month)
             and d.date <  date_add(v.publish_date, interval 60 month) then 20
        end as quarter_number
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
    where quarter_number is not null
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
        when date_add(v.publish_date, interval (a.quarter_number * 3) month) <= current_date()
            then true
        else false
    end                                         as is_complete_quarter
from aggregated as a
inner join videos as v using (video_id)