-- source model for Salesforce Task / Activity

{{ config(materialized='table')}}

select *
from {{ source('salesforce','event')}}
where is_deleted = false