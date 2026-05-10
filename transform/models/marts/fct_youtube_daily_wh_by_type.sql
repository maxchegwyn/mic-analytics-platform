with source as (
    select * from {{ ref('fct_youtube_video_daily') }}
)

select
    FORMAT_DATE('%Y-%m', date) as year_month,
    EXTRACT(YEAR FROM date) * 100 + EXTRACT(MONTH FROM date) as year_month_sort,
    content_type,
    SUM(watch_hours) as watch_hours
from source
where content_type in ('exercise', 'other')
group by year_month, year_month_sort, content_type