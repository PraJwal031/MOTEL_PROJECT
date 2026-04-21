{% snapshot dim_hosts_scd2 %}

{{
    config(
      target_schema='snapshots',
      unique_key='host_id',
      strategy='timestamp',
      updated_at='created_at'
    )
}}

SELECT * FROM {{ ref('silver_hosts') }}

{% endsnapshot %}