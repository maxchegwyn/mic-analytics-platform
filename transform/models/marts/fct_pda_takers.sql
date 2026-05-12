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
),

first_purchase as (
    select
        email,
        min(created_at) as first_purchase_at
    from {{ ref('fct_revenue') }}
    where platform = 'woocommerce'
        and lower(product_name) like '%personality dynamics assessment%'
        and lower(email) not like '%maxchegwyn%'
        and lower(email) not like '%makeitconscious%'
    group by email
),

with_flags as (
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
        case when r.submission_count > 1 then true else false end as is_repeat_taker,
        case
            when row_number() over (partition by s.email order by s.submitted_at) = 1
            then true else false
        end as is_first_submission,
        case
            when row_number() over (partition by s.email order by s.submitted_at desc) = 1
            then true else false
        end as is_latest_submission,
        fp.first_purchase_at,
        timestamp_diff(fp.first_purchase_at, s.submitted_at, hour) as hours_to_first_purchase,
        case
            when fp.first_purchase_at is null
                then 'not_converted'
            when timestamp_diff(fp.first_purchase_at, s.submitted_at, hour) <= 12
                then 'immediate'
            when timestamp_diff(fp.first_purchase_at, s.submitted_at, day) <= 7
                then 'within_7_days'
            else 'after_7_days'
        end as conversion_window
    from submissions as s
    left join repeat_takers as r on s.email = r.email
    left join first_purchase as fp on lower(s.email) = lower(fp.email)
),

with_functions as (
    select
        *,
        case personality_type
            when 'INFJ' then 'Ni' when 'INTJ' then 'Ni'
            when 'ENFJ' then 'Fe' when 'ENTJ' then 'Te'
            when 'INFP' then 'Fi' when 'INTP' then 'Ti'
            when 'ENFP' then 'Ne' when 'ENTP' then 'Ne'
            when 'ISFJ' then 'Si' when 'ISTJ' then 'Si'
            when 'ESFJ' then 'Fe' when 'ESTJ' then 'Te'
            when 'ISFP' then 'Fi' when 'ISTP' then 'Ti'
            when 'ESFP' then 'Se' when 'ESTP' then 'Se'
        end as dominant_function,
        case personality_type
            when 'INFJ' then 'Fe' when 'INTJ' then 'Te'
            when 'ENFJ' then 'Ni' when 'ENTJ' then 'Ni'
            when 'INFP' then 'Ne' when 'INTP' then 'Ne'
            when 'ENFP' then 'Fi' when 'ENTP' then 'Ti'
            when 'ISFJ' then 'Fe' when 'ISTJ' then 'Te'
            when 'ESFJ' then 'Si' when 'ESTJ' then 'Si'
            when 'ISFP' then 'Se' when 'ISTP' then 'Se'
            when 'ESFP' then 'Fi' when 'ESTP' then 'Ti'
        end as auxiliary_function,
        case personality_type
            when 'INFJ' then 'Seer' when 'INTJ' then 'Seer'
            when 'ENFJ' then 'Caregiver' when 'ENTJ' then 'Commander'
            when 'INFP' then 'Lover' when 'INTP' then 'Sage'
            when 'ENFP' then 'Explorer' when 'ENTP' then 'Explorer'
            when 'ISFJ' then 'Stalwart' when 'ISTJ' then 'Stalwart'
            when 'ESFJ' then 'Caregiver' when 'ESTJ' then 'Commander'
            when 'ISFP' then 'Lover' when 'ISTP' then 'Sage'
            when 'ESFP' then 'Tactician' when 'ESTP' then 'Tactician'
        end as dominant_mic_name,
        case personality_type
            when 'INFJ' then 'Caregiver' when 'INTJ' then 'Commander'
            when 'ENFJ' then 'Seer' when 'ENTJ' then 'Seer'
            when 'INFP' then 'Explorer' when 'INTP' then 'Explorer'
            when 'ENFP' then 'Lover' when 'ENTP' then 'Sage'
            when 'ISFJ' then 'Caregiver' when 'ISTJ' then 'Commander'
            when 'ESFJ' then 'Stalwart' when 'ESTJ' then 'Stalwart'
            when 'ISFP' then 'Tactician' when 'ISTP' then 'Tactician'
            when 'ESFP' then 'Lover' when 'ESTP' then 'Sage'
        end as auxiliary_mic_name,
        case personality_type
            when 'INFJ' then 'Seer-Caregiver' when 'INTJ' then 'Seer-Commander'
            when 'ENFJ' then 'Caregiver-Seer' when 'ENTJ' then 'Commander-Seer'
            when 'INFP' then 'Lover-Explorer' when 'INTP' then 'Sage-Explorer'
            when 'ENFP' then 'Explorer-Lover' when 'ENTP' then 'Explorer-Sage'
            when 'ISFJ' then 'Stalwart-Caregiver' when 'ISTJ' then 'Stalwart-Commander'
            when 'ESFJ' then 'Caregiver-Stalwart' when 'ESTJ' then 'Commander-Stalwart'
            when 'ISFP' then 'Lover-Tactician' when 'ISTP' then 'Sage-Tactician'
            when 'ESFP' then 'Tactician-Lover' when 'ESTP' then 'Tactician-Sage'
        end as type_pair_mic,
        case personality_type
            when 'ESTP' then 'SP' when 'ESFP' then 'SP'
            when 'ISTP' then 'SP' when 'ISFP' then 'SP'
            when 'ESTJ' then 'SJ' when 'ESFJ' then 'SJ'
            when 'ISTJ' then 'SJ' when 'ISFJ' then 'SJ'
            when 'ENFJ' then 'NF' when 'ENFP' then 'NF'
            when 'INFJ' then 'NF' when 'INFP' then 'NF'
            when 'ENTJ' then 'NT' when 'ENTP' then 'NT'
            when 'INTJ' then 'NT' when 'INTP' then 'NT'
        end as temperament
    from with_flags
)

select
    *,
    case when dominant_function in ('Fe','Fi','Te','Ti') then dominant_function else auxiliary_function end as judging_function,
    case when dominant_function in ('Ne','Ni','Se','Si') then dominant_function else auxiliary_function end as perceiving_function,
    case when dominant_function in ('Fe','Fi','Te','Ti') then dominant_mic_name else auxiliary_mic_name end as judging_mic_name,
    case when dominant_function in ('Ne','Ni','Se','Si') then dominant_mic_name else auxiliary_mic_name end as perceiving_mic_name
from with_functions