{{ config
(
    materialized='table',
    database = 'PROD',
    schema = 'MONTHLY_KPIS'
)
}}

with net_new_actuals_monthly as (
select distinct
date_month,
'actuals' as category,
sum(mrr_change_acct) as arr
from {{ ref('fct_arr_account') }}
where revenue_category in ('new','expansion','churn')
and to_date(date_month) <= date_trunc(month,to_date(current_date()))
group by date_month, category
order by date_month asc
),

fy_dates as (
select distinct
date_trunc('month',dte) as date_month,
0 as arr_future
from "DEV"."MARTS"."FY_CALENDAR"
where date_trunc('month',dte) >= '2022-03-01'
and date_trunc('month',dte) <= '2023-02-01' 
),

actuals_combined_int as (
select distinct
fd.date_month,
CASE WHEN nnam.category IS NULL then 'actuals' else nnam.category end as category,
nnam.arr,
fd.arr_future
from fy_dates as fd
left join net_new_actuals_monthly as nnam on (to_date(nnam.date_month) = to_date(fd.date_month))
order by date_month asc
),

actuals_combined as (
select 
to_timestamp(date_month) as date_month,
category,
CASE WHEN arr is NULL then arr_future ELSE arr end as arr
from actuals_combined_int
order by date_month asc
),

net_new_budget_monthly as (
select 
to_date(date) as date_month,
'budget' as category,
net_new_arr_budget as arr
from "FIVETRAN_DATABASE"."GOOGLE_SHEETS"."FY_22_FORECAST_FINANCE_INPUTS"
where date_trunc(month,to_date(date)) >= '2022-03-01' 
order by date_month asc 
),

net_new_forecast_monthly as (
select
to_date(date) as date_month,
'forecast' as category,
net_new_arr_forecast as arr
from "FIVETRAN_DATABASE"."GOOGLE_SHEETS"."FY_22_FORECAST_FINANCE_INPUTS"
where date_trunc(month,to_date(date)) >= '2021-12-01' 
order by date_month asc 
),

fct_net_new as (
select * from actuals_combined
UNION 
select * from net_new_budget_monthly
UNION 
select * from net_new_forecast_monthly
order by date_month asc, category
)

select * from fct_net_new