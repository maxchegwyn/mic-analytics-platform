with

submissions as (
    select
        submission_id,
        email,
        submitted_at,
        personality_type,
        results_url,
        checkout_url,
        clarity_e,
        clarity_s,
        clarity_t,
        clarity_j,
        submission_source
    from {{ ref('stg_jotform') }}
),

repeat_takers as (
    select
        email,
        count(*) as submission_count
    from {{ ref('stg_jotform') }}
    group by email
)

select
    s.submission_id,
    s.email,
    s.submitted_at,
    s.personality_type,
    s.results_url,
    s.checkout_url,
    s.clarity_e,
    s.clarity_s,
    s.clarity_t,
    s.clarity_j,
    s.submission_source,
    case when r.submission_count > 1 then true else false end as is_repeat_taker
from submissions as s
left join repeat_takers as r on s.email = r.email