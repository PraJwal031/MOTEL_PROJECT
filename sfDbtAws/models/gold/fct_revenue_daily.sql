{{ config(materialized='incremental') }}


{# Medical Domain Scenario for Incremental Model
In our medical domain project, data was continuously ingested from multiple hospital systems, APIs, and insurance platforms into Snowflake.
One of the reporting requirements was to build: daily claim revenue reporting

which included:
total approved claims
billing amount
insurance charges
adjustments
net revenue

Problem
The claim tables were growing daily with large transactional data.
If we performed full refresh every run: entire historical claim data would be recalculated repeatedly 

which increased:
pipeline runtime
Snowflake compute cost
unnecessary processing

Solution
We implemented: dbt incremental models to process only newly arrived claim records. #}


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