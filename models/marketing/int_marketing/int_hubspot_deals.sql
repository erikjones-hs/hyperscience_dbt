{{ config(materialized='table')}}

select *
from {{ ref('stg_hubspot_deals')}}