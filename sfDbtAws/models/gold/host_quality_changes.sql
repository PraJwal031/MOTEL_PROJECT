{# {{ config(materialized='view') }}

SELECT 
  HOST_ID,
  HOST_NAME,
  LAG(RESPONSE_RATE) OVER (PARTITION BY HOST_ID ORDER BY dbt_valid_from) as previous_response_rate,
  RESPONSE_RATE as current_response_rate,
  RESPONSE_RATE - LAG(RESPONSE_RATE) OVER (PARTITION BY HOST_ID ORDER BY dbt_valid_from) as change_points,
  LAG(RESPONSE_RATE_QUALITY) OVER (PARTITION BY HOST_ID ORDER BY dbt_valid_from) as previous_quality,
  RESPONSE_RATE_QUALITY as current_quality,
  LAG(IS_SUPERHOST) OVER (PARTITION BY HOST_ID ORDER BY dbt_valid_from) as was_superhost,
  IS_SUPERHOST as is_superhost_now,
  dbt_valid_from as change_date,
  dbt_valid_to as change_end_date,
  CASE 
    WHEN dbt_valid_to IS NULL THEN 'CURRENT'
    ELSE 'HISTORICAL'
  END as status
FROM {{ ref('dim_hosts_scd2') }}
WHERE LAG(RESPONSE_RATE) OVER (PARTITION BY HOST_ID ORDER BY dbt_valid_from) IS NOT NULL
ORDER BY change_date DESC, HOST_ID


{{ config(materialized='view') }} #}

WITH ranked_hosts AS (
  SELECT 
    HOST_ID,
    HOST_NAME,
    RESPONSE_RATE,
    RESPONSE_RATE_QUALITY,
    IS_SUPERHOST,
    dbt_valid_from,
    dbt_valid_to,
    LAG(RESPONSE_RATE) OVER (PARTITION BY HOST_ID ORDER BY dbt_valid_from) as previous_response_rate,
    LAG(RESPONSE_RATE_QUALITY) OVER (PARTITION BY HOST_ID ORDER BY dbt_valid_from) as previous_quality,
    LAG(IS_SUPERHOST) OVER (PARTITION BY HOST_ID ORDER BY dbt_valid_from) as was_superhost,
    RESPONSE_RATE - LAG(RESPONSE_RATE) OVER (PARTITION BY HOST_ID ORDER BY dbt_valid_from) as change_points
  FROM {{ ref('host_snapshots') }}
)

SELECT 
  HOST_ID,
  HOST_NAME,
  previous_response_rate,
  RESPONSE_RATE as current_response_rate,
  change_points,
  previous_quality,
  RESPONSE_RATE_QUALITY as current_quality,
  was_superhost,
  IS_SUPERHOST as is_superhost_now,
  dbt_valid_from as change_date,
  dbt_valid_to as change_end_date,
  CASE 
    WHEN dbt_valid_to IS NULL THEN 'CURRENT'
    ELSE 'HISTORICAL'
  END as status
FROM ranked_hosts
WHERE previous_response_rate IS NOT NULL
ORDER BY change_date DESC, HOST_ID



{# SELECT 
  COUNT(DISTINCT HOST_ID) as hosts_degraded,
  ROUND(AVG(change_points), 2) as avg_degradation
FROM MOTEL_PROJECT.GOLD.HOST_QUALITY_CHANGES
WHERE change_points < 0
  AND status = 'CURRENT'; #}