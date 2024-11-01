-- source model for Salesforce Campaign

{{ config(materialized='table')}}

select *
from {{ source('salesforce','campaign')}}
where is_deleted = false