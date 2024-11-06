-- source model for Salesforce Lead

{{ config(materialized='table')}}

select *
from {{ source('salesforce','lead')}}
where is_deleted = false