-- fct_pda_conversion.sql
-- Grain: one row per person (email), deduplicated from cluster-level conversions.
-- A cluster is a group of submissions from the same email where each consecutive submission is within 24 hours of the previous one.
-- The first submission_id in the cluster is carried as the row identifier. The immediate/delayed classification and days_to_conversion are both measured from the last submission in the cluster, so that a purchase shortly after a repeat submission is not penalised by the gap from the first.
-- Conversion = first PDA Report purchase after the cluster's last submission timestamp.
-- Immediate = within 24 hours of last submission. Delayed = more than 24 hours.
-- Where a person has multiple converting clusters, the earliest converting cluster is used (first conversion wins).

WITH pda AS (
  SELECT
    submission_id,
    email,
    submitted_at,
    personality_type
  FROM {{ ref('fct_pda_takers') }}
),

cluster_flags AS (
  SELECT
    submission_id,
    email,
    submitted_at,
    personality_type,
    CASE
      WHEN TIMESTAMP_DIFF(
        submitted_at,
        LAG(submitted_at) OVER (PARTITION BY email ORDER BY submitted_at),
        HOUR
      ) <= 24 THEN 0
      ELSE 1
    END AS is_cluster_start
  FROM pda
),

clusters AS (
  SELECT
    submission_id,
    email,
    submitted_at,
    personality_type,
    is_cluster_start,
    SUM(is_cluster_start) OVER (
      PARTITION BY email
      ORDER BY submitted_at
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cluster_id
  FROM cluster_flags
),

canonical_submissions AS (
  SELECT
    email,
    cluster_id,
    MIN(submitted_at) AS first_submitted_at,
    MAX(submitted_at) AS last_submitted_at,
    ARRAY_AGG(submission_id ORDER BY submitted_at LIMIT 1)[OFFSET(0)] AS submission_id,
    ARRAY_AGG(personality_type ORDER BY submitted_at DESC LIMIT 1)[OFFSET(0)] AS personality_type
  FROM clusters
  GROUP BY email, cluster_id
),

revenue AS (
  SELECT
    email,
    created_at
  FROM {{ ref('fct_revenue') }}
  WHERE product_name LIKE '%Personality Dynamics Assessment%'
),

first_purchase_per_cluster AS (
  SELECT
    c.submission_id,
    c.email,
    c.personality_type,
    c.first_submitted_at,
    c.last_submitted_at,
    MIN(r.created_at) AS first_purchase_after_submission
  FROM canonical_submissions c
  LEFT JOIN revenue r
    ON r.email = c.email
    AND r.created_at > c.last_submitted_at
  GROUP BY
    c.submission_id,
    c.email,
    c.personality_type,
    c.first_submitted_at,
    c.last_submitted_at
),

cluster_level AS (
  SELECT
    submission_id,
    email,
    personality_type,
    first_submitted_at AS submitted_at,
    last_submitted_at,
    first_purchase_after_submission,
    CASE
      WHEN first_purchase_after_submission IS NULL THEN FALSE
      ELSE TRUE
    END AS converted,
    CASE
      WHEN first_purchase_after_submission IS NULL THEN NULL
      WHEN TIMESTAMP_DIFF(first_purchase_after_submission, last_submitted_at, HOUR) <= 24 THEN 'immediate'
      ELSE 'delayed'
    END AS conversion_type,
    CASE
      WHEN first_purchase_after_submission IS NULL THEN NULL
      ELSE DATE_DIFF(
        DATE(first_purchase_after_submission),
        DATE(last_submitted_at),
        DAY
      )
    END AS days_to_conversion
  FROM first_purchase_per_cluster
),

-- Deduplicate to person level. For converted people, keep the earliest converting cluster.
-- For non-converters, keep the earliest cluster.
person_level AS (
  SELECT
    email,
    ARRAY_AGG(
      STRUCT(submission_id, personality_type, submitted_at, last_submitted_at, first_purchase_after_submission, converted, conversion_type, days_to_conversion)
      ORDER BY
        CASE WHEN converted THEN 0 ELSE 1 END,
        submitted_at
      LIMIT 1
    )[OFFSET(0)] AS r
  FROM cluster_level
  GROUP BY email
)

SELECT
  r.submission_id,
  email,
  r.personality_type,
  r.submitted_at,
  r.last_submitted_at,
  r.first_purchase_after_submission,
  r.converted,
  r.conversion_type,
  r.days_to_conversion
FROM person_level