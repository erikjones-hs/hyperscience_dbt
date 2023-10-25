{{ config
(
    materialized='table',
    database = 'PROD',
    schema = 'MONTHLY_KPIS'
)
}}

with arr as (
select to_date(current_date) as dte,
sum(mrr_acct) as arr
from {{ref('fct_arr_account')}}
where to_date(date_month) = date_trunc(month,to_date(current_date))
),

arr_yoy_growth_int as (
select to_date(current_date) as dte,
date_month,
sum(mrr_acct) as current_arr
from {{ref('fct_arr_account')}} 
where to_date(date_month) >= add_months(date_trunc(month,to_date(current_date)),-12)
and to_date(date_month) <= date_trunc(month,to_date(current_date))
group by date_month
order by date_month asc
),

arr_yoy_growth_int2 as (
select distinct 
dte,
date_month,
current_arr,
lag(current_arr,12,0) over (order by date_month asc) as arr_year_ago,
((current_arr - arr_year_ago) / NULLIFZERO(arr_year_ago)) as arr_growth_perc 
from arr_yoy_growth_int
order by date_month asc
),

arr_yoy_growth as (
select distinct 
dte,
arr_growth_perc
from arr_yoy_growth_int2 
where to_date(date_month) = date_trunc(month,to_date(current_date()))
),

avg_arr as (
select to_date(current_date) as dte,
avg(mrr_acct) as avg_arr
from {{ref('fct_arr_account')}}
where to_date(date_month) = date_trunc(month,to_date(current_date)) 
),

num_employees as (
select to_date(current_date) as dte,
count(distinct employee_id) as num_employees
from {{ref('hr_headcount_history')}}
where to_date(date_month) = date_trunc(month,to_date(current_date))
and is_employee = 1
),

nrr_int1 as (
select date_month,
sum(mrr_acct) as total_arr,
sum(case when months_since_start >= 12 then mrr_acct else NULL end) as net_retention_arr
from {{ref('fct_arr_account')}}  
where to_date(date_month) <= date_trunc(month,to_date(current_date))
group by date_month
order by date_month asc
),

nrr_int2 as (
select 
date_month,
total_arr,
net_retention_arr,
lag(total_arr,12,0) over (order by date_month asc) as arr_year_ago
from nrr_int1
order by date_month asc
),

nrr as ( 
select to_date(current_date) as dte,
(net_retention_arr / arr_year_ago) as net_arr_retention
from nrr_int2
where to_date(date_month) = date_trunc(month,to_date(current_date)) 
),

gm_ttm_int1 as (
select
sum(gross_profit) as gross_profit,
sum(revenue) as revenue
from {{ref('saas_metrics')}} 
WHERE dte >= (TO_TIMESTAMP('2022-08-31')) AND dte <= (TO_TIMESTAMP('2023-08-31'))
),

gm_ttm as (
select to_date(current_date) as dte,
(gross_profit / revenue) as gross_margin_ttm
from gm_ttm_int1
),

gross_dollar_retention as (
SELECT
to_date(current_date) as dte,
gross_dollar_retention as gross_dollar_retention_ttm
from {{ref('saas_metrics')}}
WHERE dte = (TO_TIMESTAMP('2023-08-31'))
),

combined as (
select 
a.dte,
a.arr,
ayg.arr_growth_perc,
aa.avg_arr,
ne.num_employees,
n.net_arr_retention,
gt.gross_margin_ttm,
gdr.gross_dollar_retention_ttm
from arr as a
left join arr_yoy_growth as ayg on (a.dte = ayg.dte)
left join avg_arr as aa on (a.dte = aa.dte)
left join num_employees as ne on (a.dte = ne.dte)
left join nrr as n on (a.dte = n.dte)
left join gm_ttm as gt on (a.dte = gt.dte)
left join gross_dollar_retention as gdr on (a.dte = gdr.dte)
)

select * from combined