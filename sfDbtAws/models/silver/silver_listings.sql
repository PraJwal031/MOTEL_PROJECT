{{ config(materialized='incremental', unique_key='LISTING_ID') }}

SELECT 
    LISTING_ID,
    HOST_ID,
    PROPERTY_TYPE,
    CASE 
        WHEN UPPER(TRIM(ROOM_TYPE)) = 'ENTIRE HOME' THEN 'Entire home/apt'
        WHEN UPPER(TRIM(ROOM_TYPE)) = 'PRIVATE ROOM' THEN 'Private room'
        ELSE 'Unknown'
    END AS ROOM_TYPE,
    CITY,
    COUNTRY,
    ACCOMMODATES,
    BEDROOMS,
    BATHROOMS,
    PRICE_PER_NIGHT,
    {{ tag('CAST(PRICE_PER_NIGHT AS INT)') }} AS PRICE_PER_NIGHT_TAG,
    CREATED_AT,
    loaded_AT
FROM 
    {{ ref('bronze_listings') }}


    
{# select * from {{ ref('silver_listings')}}
limit 50 #}
