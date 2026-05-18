-- NOTE: GA4 BigQuery export only started 21 March 2026.
-- No historical backfill is possible. This model will grow daily from that date onward.

with source as (
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
)

select * from source