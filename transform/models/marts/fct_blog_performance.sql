-- NOTE: GA4 BigQuery export only started 21 March 2026.
-- No historical backfill is possible. This mart will grow daily from that date onward.
-- Document this date lock in the portfolio write-up.

with

ga4 as (
    select
        parse_date('%Y%m%d', event_date)        as date,
        (select value.string_value
         from unnest(event_params)
         where key = 'page_location')           as page_location,
        (select value.string_value
         from unnest(event_params)
         where key = 'medium')                  as medium,
        (select value.string_value
         from unnest(event_params)
         where key = 'source')                  as source
    from `make-it-conscious.analytics_322691207.events_*`
    where event_name = 'page_view'
),

blog_posts as (
    select
        post_id,
        slug,
        url
    from {{ ref('dim_blog_posts') }}
),

joined as (
    select
        g.date,
        b.post_id,
        b.slug,
        concat(g.source, ' / ', g.medium)       as source_medium,
        count(*)                                 as sessions
    from ga4 as g
    inner join blog_posts as b
        on regexp_contains(g.page_location, concat('/', b.slug))
    group by g.date, b.post_id, b.slug, source_medium
)

select
    date,
    post_id,
    slug,
    source_medium,
    sessions
from joined