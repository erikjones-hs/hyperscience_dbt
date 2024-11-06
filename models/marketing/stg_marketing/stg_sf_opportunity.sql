-- source model for Salesforce Opportunity

{{ config(materialized='table')}}

select *
from {{ source('salesforce','opportunity')}}
where 
    is_deleted = false
    and _fivetran_active = true