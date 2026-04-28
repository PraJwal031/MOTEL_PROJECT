{{ config(materialized='incremental') }}

WITH bookings_confirmed AS (

    SELECT 
        booking_date,
        booking_id,
        TOTAL_AMOUNT,
        SERVICE_FEE,
        CLEANING_FEE
    FROM {{ ref("silver_bookings") }}
    WHERE booking_status IN ('confirmed')

    {% if is_incremental() %}
        AND booking_date > (SELECT MAX(booking_date) FROM {{ this }})
    {% endif %}

),

total_revenue AS (

    SELECT
        booking_date,
        COUNT(booking_id) AS bookings_count,
        SUM(TOTAL_AMOUNT) AS gross_revenue,
        SUM(COALESCE(SERVICE_FEE, 0)) AS service_fee_total,
        SUM(COALESCE(CLEANING_FEE, 0)) AS cleaning_fee_total
    FROM bookings_confirmed
    GROUP BY booking_date

)

SELECT 
    booking_date,
    bookings_count,
    gross_revenue,
    service_fee_total,
    cleaning_fee_total,
    (gross_revenue - service_fee_total - cleaning_fee_total) AS net_revenue 
FROM total_revenue