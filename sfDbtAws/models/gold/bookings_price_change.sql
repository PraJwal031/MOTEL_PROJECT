{{ config(materialized='view') }}
 

 {# Problem: “Price Change Tracking”
Build a model that answers:
“How many times did a booking’s price change, and when?” #}

{# n the medical project, claim and billing amounts could change over time due to:

insurance adjustments
billing corrections
treatment updates
manual review changes

Business wanted to track:

how many times a claim amount changed,
what the previous amount was,
what the updated amount became,
and when the change happened
🔥 Problem

If claim data was overwritten directly:

historical billing changes would be lost #}



WITH price_changes AS (

    SELECT
        booking_id,
        dbt_valid_from,
        dbt_valid_to AS change_date,
        total_amount AS current_amount,
        LAG(total_amount) OVER (
            PARTITION BY booking_id 
            ORDER BY dbt_valid_from
        ) AS old_amount
    FROM {{ ref('booking_changes') }}

),

filtered_changes AS (
    
    SELECT
        booking_id,
        change_date,
        old_amount,
        current_amount
    FROM price_changes
    WHERE old_amount IS NOT NULL
      AND current_amount != old_amount

),

final AS (

    SELECT
        booking_id,
        ROW_NUMBER() OVER (
            PARTITION BY booking_id 
            ORDER BY change_date
        ) AS change_number,
        old_amount,
        current_amount,
        change_date
    FROM filtered_changes

)

SELECT *
FROM final