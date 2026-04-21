{% snapshot host_snapshots %}

{{
    config(
      target_schema='snapshots',
      unique_key='host_id',
      strategy='check',
      check_cols=['response_rate', 'host_name'],
    )
}}

select * from {{ ref('silver_hosts') }}

{% endsnapshot %}


{# SELECT 
    host_id, 
    host_name, 
    response_rate, 
    dbt_valid_from, 
    dbt_valid_to
FROM MOTEL_PROJECT.SNAPSHOTS.host_snapshots
ORDER BY host_id, dbt_valid_from; #}