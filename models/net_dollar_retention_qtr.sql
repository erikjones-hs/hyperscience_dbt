{{ config
(
    materialized='table',
    database = 'PROD',
    schema = 'MONTHLY_KPIS'
)
}}

with full_dataset as (
SELECT
DATE_TRUNC('month', agg_account_arr."DATE_MONTH") AS date_month_month,
(COALESCE(SUM(CASE WHEN (agg_account_arr."MONTHS_SINCE_START"  >= 12) THEN (agg_account_arr."MRR_ACCT") ELSE NULL END), 0) ) / NULLIFZERO(( lag((COALESCE(SUM((agg_account_arr."MRR_ACCT")), 0)),12,0) over (order by (TO_CHAR(DATE_TRUNC('month', agg_account_arr."DATE_MONTH" ), 'YYYY-MM')) asc) )) AS net_dollar_retention
FROM (select * from {{ref('fct_arr_account')}}) AS agg_account_arr
where DATE_TRUNC('month', agg_account_arr."DATE_MONTH" ) <= date_trunc(month,current_date())
GROUP BY (DATE_TRUNC('month', agg_account_arr."DATE_MONTH" ))
ORDER BY 1 DESC
),

quarter_end as (
select 
to_timestamp(date_month_month) as dte,
CASE WHEN to_date(dte) = '2023-02-01' then 1.09267361 
     WHEN to_date(dte) = '2023-11-01' then 1.06
     else net_dollar_retention end as net_dollar_retention
from full_dataset
where monthname(date_month_month) in ('Feb','May','Aug','Nov')
order by date_month_month asc
),

forecast as (
select distinct 
to_timestamp(date) as dte,
net_dollar_retention
from "FIVETRAN_DATABASE"."GOOGLE_SHEETS"."FY_22_FORECAST_FINANCE_INPUTS"
where to_date(date) >= add_months(date_trunc('month',to_date(current_date())),1)
and monthname(date) in ('Feb','May','Aug','Nov')
order by dte asc
),

fct_ndr_actuals_forecast as (
select * from quarter_end 
UNION 
select * from forecast
order by dte asc
)

select * from fct_ndr_actuals_forecast