{{ config(materialized='incremental') }}


SELECT * , CURRENT_TIMESTAMP() AS loaded_at   FROM   
{{ source('staging', 'listings') }}

{% if is_incremental() %}
    WHERE CREATED_AT > (SELECT COALESCE(MAX(CREATED_AT), '1900-01-01') FROM {{ this }})
{% endif %}



{# select * from {{ref("bronze_listings")}}
limit 30 #}