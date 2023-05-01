{{ config
(
    materialized='table',
    database = 'PROD',
    schema = 'MONTHLY_KPIS'
)
}}

with full_dataset as (
SELECT
TO_TIMESTAMP(TO_DATE(growth_accounting_qtr."QTR_END_DTE")) AS qtr_end_dte,
COALESCE(SUM(( growth_accounting_qtr."ARR_PER_CUSTOMER"  ) ), 0) AS avg_arr
FROM (select * from {{ref('monthly_kpis_growth_accounting_qtr')}}) AS growth_accounting_qtr
GROUP BY (TO_DATE(growth_accounting_qtr."QTR_END_DTE" ))
ORDER BY 1
),

forecast as (
select distinct 
to_timestamp(date) as qtr_end_dte,
avg_arr_customer as avg_arr
from "FIVETRAN_DATABASE"."GOOGLE_SHEETS"."FY_22_FORECAST_FINANCE_INPUTS"
where to_date(date) > add_months(date_trunc('month',to_date(current_date())),1)
and monthname(date) in ('Feb','May','Aug','Nov')
order by qtr_end_dte asc
),

fct_avg_arr_actuals_forecast as (
select * from full_dataset 
UNION 
select * from forecast
order by qtr_end_dte asc
)

select * from fct_avg_arr_actuals_forecast