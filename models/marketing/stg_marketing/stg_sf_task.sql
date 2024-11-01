-- source model for Salesforce Task / Activity

{{ config(materialized='table')}}

select *
from {{ source('salesforce','task')}}
where is_deleted = false