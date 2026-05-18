select
    date,
    views,
    watch_hours,
    subscribers_gained,
    subscribers_lost,
    subscribers_net,
    estimated_revenue,
    likes,
    comments,
    shares
from {{ ref('stg_youtube_channel') }}