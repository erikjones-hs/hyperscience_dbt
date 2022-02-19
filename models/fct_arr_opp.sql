{{ config
(
    materialized='table',
    database = 'PROD',
    schema = 'MONTHLY_KPIS'
)
}}

select * from dev.erikjones.monthly_kpis_finance_arr 
