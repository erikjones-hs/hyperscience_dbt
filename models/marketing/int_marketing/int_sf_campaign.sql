{{ config(materialized='table')}}

select *
from {{ ref('stg_sf_campaign')}}