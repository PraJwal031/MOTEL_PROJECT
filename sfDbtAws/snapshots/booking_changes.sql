{% snapshot booking_changes %}

{{
   config (
       target_schema='snapshots',
       unique_key='booking_id',
       strategy='check',
       check_cols = 'all',
   )
}}

select * from {{ref('silver_bookings')}}

{% endsnapshot %}
