-- source model for Hubspot Contact

{{ config(materialized='table')}}


with 
    cleaned_column_names as (
        select
        {{ 
            remove_prefix_from_columns(
                columns=adapter.get_columns_in_relation(source('hubspot', 'contact')), 
                prefix='property_')
            }}
        from {{ source('hubspot', 'contact') }}
        where is_deleted = false
    )

select 
    id
    ,email
    ,firstname
    ,lastname
    ,createdate
    ,company
    ,* exclude (id, email, firstname, lastname, createdate, company)
    -- putting key fields at the front of the table for readability
from cleaned_column_names