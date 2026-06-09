{{ config(materialized='table') }}


{# The model shows: latest captured snapshot version not real-time source data. 
If source changes after snapshot execution:
model will reflect changes only after next snapshot run #}

{# Business Scenario

In our medical domain project, patient and claim-related data was continuously updated from multiple source systems and APIs.

Examples of changing data:

patient insurance status updated
claim amount corrected
diagnosis codes modified
treatment status changed
billing adjustments applied

Business required:

1. Latest accurate data for reporting
2. Historical tracking of all important changes #}

WITH current_bookings AS (

    SELECT
        booking_id,
        booking_date,
        total_amount,
        service_fee,
        cleaning_fee,
        booking_status
    FROM {{ ref('booking_changes') }}
    WHERE dbt_valid_to IS NULL   --  only latest version

),

valid_bookings AS (

    SELECT *
    FROM current_bookings
    WHERE booking_status != 'cancelled'   --  exclude cancelled

),

aggregated AS (

    SELECT
        booking_date,
        COUNT(booking_id) AS bookings_count,
        SUM(total_amount) AS gross_revenue,
        SUM(COALESCE(service_fee, 0)) AS service_fee_total,
        SUM(COALESCE(cleaning_fee, 0)) AS cleaning_fee_total
    FROM valid_bookings
    GROUP BY booking_date

)

SELECT
    booking_date,
    bookings_count,
    gross_revenue,
    service_fee_total,
    cleaning_fee_total,
    (gross_revenue - service_fee_total - cleaning_fee_total) AS net_revenue

FROM aggregated


