-- source model for Salesforce Contact

{{ config(materialized='table')}}

select *
from {{ source('salesforce','contact')}}
where is_deleted = false