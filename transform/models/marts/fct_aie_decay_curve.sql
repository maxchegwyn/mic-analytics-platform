with aie_videos as (
    select video_id
    from {{ ref('dim_videos') }}
    where content_type = 'exercise'
    and exercise_category in (
        'Shadow Work',
        'Core Practice',
        'Emotional Integration',
        'Anima Animus',
        'Symbolic Investigation',
        'Applied Series'
    )
    and video_id != 'SVWr2pXDQ5k'
),

quarterly as (
    select
        p.video_id,
        p.quarter_number,
        p.watch_hours,
        sum(p.watch_hours) over (
            partition by p.video_id
            order by p.quarter_number
            rows between unbounded preceding and current row
        ) as cumulative_wh
    from {{ ref('fct_youtube_performance') }} p
    inner join aie_videos a using (video_id)
    where p.is_complete_quarter = true
),

counts as (
    select
        quarter_number,
        count(*) as n_videos
    from quarterly
    group by quarter_number
),

percentiles as (
    select distinct
        q.quarter_number,
        percentile_cont(q.cumulative_wh, 0.25) over (
            partition by q.quarter_number
        ) as p25_cumulative_wh,
        percentile_cont(q.cumulative_wh, 0.50) over (
            partition by q.quarter_number
        ) as p50_cumulative_wh,
        percentile_cont(q.cumulative_wh, 0.75) over (
            partition by q.quarter_number
        ) as p75_cumulative_wh
    from quarterly q
)

select
    p.quarter_number,
    c.n_videos,
    p.p25_cumulative_wh,
    p.p50_cumulative_wh,
    p.p75_cumulative_wh
from percentiles p
inner join counts c using (quarter_number)
where c.n_videos >= 10
order by p.quarter_number