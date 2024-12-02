-- source model for Hubspot Deal

{{ config(materialized='table')}}


with 
    cleaned_column_names as (
        select
        {{ 
            remove_prefix_from_columns(
                columns=adapter.get_columns_in_relation(source('hubspot', 'deal')), 
                prefix='property_')
            }}
        from {{ source('hubspot', 'deal') }}
        where is_deleted = false
    )

select 
    deal_id
    ,dealname
    ,* exclude (deal_id, dealname)
    -- putting key fields at the front of the table for readability
from cleaned_column_names