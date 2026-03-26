with standard as (
    select * from {{ source('raw_jotform', 'submissions_standard') }}
),

manual as (
    select * from {{ source('raw_jotform', 'submissions_manual_opt') }}
),

standard_cleaned as (
    select
        id                                                                          as submission_id,
        created_at                                                                  as submitted_at,
        lower(trim(answers___65__answer))                                           as email,
        answers___64__answer__first                                                 as first_name,
        answers___64__answer__last                                                  as last_name,
        answers___101__answer                                                       as personality_type,
        answers___157__answer                                                       as results_url,
        answers___158__answer                                                       as checkout_url,
        cast(REGEXP_EXTRACT(answers___157__answer, r'clarity_e=([0-9.]+)') as float64)  as clarity_e,
        cast(REGEXP_EXTRACT(answers___157__answer, r'clarity_s=([0-9.]+)') as float64)  as clarity_s,
        cast(REGEXP_EXTRACT(answers___157__answer, r'clarity_t=([0-9.]+)') as float64)  as clarity_t,
        cast(REGEXP_EXTRACT(answers___157__answer, r'clarity_j=([0-9.]+)') as float64)  as clarity_j,
        'standard'                                                                  as submission_source
    from standard
),

manual_cleaned as (
    select
        id                                                                          as submission_id,
        created_at                                                                  as submitted_at,
        lower(trim(answers___65__answer))                                           as email,
        answers___64__answer__first                                                 as first_name,
        answers___64__answer__last                                                  as last_name,
        answers___101__answer                                                       as personality_type,
        answers___157__answer                                                       as results_url,
        answers___158__answer                                                       as checkout_url,
        cast(REGEXP_EXTRACT(answers___157__answer, r'clarity_e=([0-9.]+)') as float64)  as clarity_e,
        cast(REGEXP_EXTRACT(answers___157__answer, r'clarity_s=([0-9.]+)') as float64)  as clarity_s,
        cast(REGEXP_EXTRACT(answers___157__answer, r'clarity_t=([0-9.]+)') as float64)  as clarity_t,
        cast(REGEXP_EXTRACT(answers___157__answer, r'clarity_j=([0-9.]+)') as float64)  as clarity_j,
        'manual_opt'                                                                as submission_source
    from manual
)

select * from standard_cleaned
union all
select * from manual_cleaned