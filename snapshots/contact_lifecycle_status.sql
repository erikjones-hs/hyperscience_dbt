{% snapshot contact_lifecycle_snapshot %}

{{
        config(
          target_schema='snapshots',
          strategy='check',
          unique_key='id',
          check_cols='all'
        )
}}

select * from {{ ref('lead_contact_life_cycle_status_changes') }}

{% endsnapshot %}