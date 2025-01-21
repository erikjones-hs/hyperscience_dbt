-- source model for Hubspot Monthly Web Sessions

{{ config(materialized='table')}}


select *
from {{ source('hubspot', 'sessions_analytics_monthly_report')}}