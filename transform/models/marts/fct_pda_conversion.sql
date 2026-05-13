-- fct_pda_conversion.sql
-- Grain: one row per submission cluster per email.
-- A cluster is a group of submissions from the same email where each consecutive submission is within 24 hours of the previous one.
-- The first submission_id in the cluster is carried as the row identifier.
-- The immediate/delayed classification and days_to_conversion are measured from the last submission in the cluster.
-- Superseded: a cluster where a later cluster exists for the same person. Excluded from conversion rate calculations.
-- Converted: the cluster was followed by a purchase of a PDA report whose type matches the cluster's personality type.
-- Non-converter: the person's latest cluster with no subsequent matching purchase.
-- Where a purchase type matches an earlier cluster's type rather than the latest cluster, the matching cluster gets the conversion.

WITH pda AS (
  SELECT
    submission_id,
    email,
    submitted_at,
    personality_type
  FROM {{ ref('fct_pda_takers') }}
  WHERE email NOT LIKE '%maxchegwyn%'
  AND email NOT LIKE '%makeitconscious%'
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
    MIN(submitted_at)                                                         AS first_submitted_at,
    MAX(submitted_at)                                                         AS last_submitted_at,
    ARRAY_AGG(submission_id ORDER BY submitted_at LIMIT 1)[OFFSET(0)]        AS submission_id,
    ARRAY_AGG(personality_type ORDER BY submitted_at DESC LIMIT 1)[OFFSET(0)] AS personality_type,
    MAX(cluster_id) OVER (PARTITION BY email)                                AS max_cluster_id
  FROM clusters
  GROUP BY email, cluster_id
),

clusters_with_superseded AS (
  SELECT
    *,
    CASE
      WHEN cluster_id < max_cluster_id THEN TRUE
      ELSE FALSE
    END AS superseded
  FROM canonical_submissions
),

revenue AS (
  SELECT
    email,
    created_at,
    REGEXP_EXTRACT(product_name, r'\b([A-Z]{4})\b') AS purchased_type
  FROM {{ ref('fct_revenue') }}
  WHERE product_name LIKE '%Personality Dynamics Assessment%'
),

first_matching_purchase AS (
  SELECT
    c.submission_id,
    c.email,
    c.personality_type,
    c.first_submitted_at,
    c.last_submitted_at,
    c.superseded,
    MIN(r.created_at) AS first_purchase_after_submission
  FROM clusters_with_superseded c
  LEFT JOIN revenue r
    ON r.email = c.email
    AND r.created_at > c.last_submitted_at
    AND r.purchased_type = c.personality_type
  GROUP BY
    c.submission_id,
    c.email,
    c.personality_type,
    c.first_submitted_at,
    c.last_submitted_at,
    c.superseded
)

SELECT
  submission_id,
  email,
  personality_type,
  first_submitted_at                                                      AS submitted_at,
  last_submitted_at,
  first_purchase_after_submission,
  superseded,
  CASE
    WHEN superseded THEN 'superseded'
    WHEN first_purchase_after_submission IS NULL THEN 'non_converter'
    WHEN TIMESTAMP_DIFF(first_purchase_after_submission, last_submitted_at, HOUR) <= 24 THEN 'immediate'
    ELSE 'delayed'
  END                                                                     AS conversion_type,
  CASE
    WHEN superseded THEN FALSE
    WHEN first_purchase_after_submission IS NULL THEN FALSE
    ELSE TRUE
  END                                                                     AS converted,
  CASE
    WHEN superseded THEN NULL
    WHEN first_purchase_after_submission IS NULL THEN NULL
    ELSE DATE_DIFF(
      DATE(first_purchase_after_submission),
      DATE(last_submitted_at),
      DAY
    )
  END                                                                     AS days_to_conversion
FROM first_matching_purchase