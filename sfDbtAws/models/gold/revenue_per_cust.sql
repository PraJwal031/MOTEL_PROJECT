{{config(materialized = 'view')}}


WITH revenue_per_customer AS (
   
    select 
    listing_id as cust_list_id,
    sum(BOOKING_AMOUNT) as revenu_by_cust
    from {{ref('bronze_bookings')}}
    group by cust_list_id   
)

select 
cust_list_id,
revenu_by_cust 
from revenue_per_customer


{# select booking_id from {{ref('bronze_bookings')}}
limit 10 #}
