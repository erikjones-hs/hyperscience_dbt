-- source model for Salesforce Campaign Members

{{ config(materialized='table')}}

select *
from {{ source('salesforce','campaign_member')}}
where is_deleted = false