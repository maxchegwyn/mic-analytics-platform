select
    s.date,
    s.video_id,
    v.title,
    v.content_type,
    v.exercise_category,
    s.watch_hours,
    s.views,
    s.estimated_revenue,
    s.subscribers_gained,
    s.likes,
    s.comments,
    s.shares
from {{ ref('stg_youtube') }} s
left join {{ ref('dim_videos') }} v
    on s.video_id = v.video_id