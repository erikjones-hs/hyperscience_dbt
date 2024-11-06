-- source model for Salesforce Opportunity Contact Role

{{ config(materialized='table')}}

select *
from {{ source('salesforce','opportunity_contact_role')}}
where 
    is_deleted = false