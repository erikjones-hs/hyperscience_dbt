{{ config
(
    materialized='table',
    database = 'PROD',
    schema = 'MONTHLY_KPIS'
)
}}

/* Brining in ARR actuals for completed quarters */
with total_arr_qtr as (
select
last_day(to_date(date_month)) as qtr_end_dte,
'total' as revenue_category,
sum(mrr_acct) as arr
from {{ref('fct_arr_account')}}
where monthname(date_month) in ('Feb','May','Aug','Nov')
and to_date(date_month) <= date_trunc(month,to_date(current_date()))
group by date_month
order by date_month asc
),

fy_dates as (
select distinct 
dte,
CASE WHEN month in (1,2) then dateadd('year',-1,date_trunc(year,dte)) ELSE date_trunc(year,dte) end as fy_year,
fy_qtr_year,
qtr_end_dte
from "DEV"."MARTS"."FY_CALENDAR"
),

/* Bringing in Monthly ARR category Types through the current month */
/* Bring in types so we can subtract from Total ARR (brought in above) to get recurring */
new_expansion_churn_monthly as (
select distinct
date_month,
revenue_category,
sum(mrr_change_acct) as arr
from {{ref('fct_arr_account')}}
where revenue_category in ('new','expansion','churn','de-book')
and to_date(date_month) <= date_trunc(month,to_date(current_date()))
group by date_month, revenue_category
order by date_month asc, revenue_category 
),

/* Combining Monthly ARR types with FY QTR END dates */
new_expansion_churn_qtr_int as (
select distinct 
necm.date_month,
necm.revenue_category,
necm.arr,
fd.qtr_end_dte
from new_expansion_churn_monthly as necm
left join fy_dates as fd on (to_date(necm.date_month) = to_date(date_trunc(month,fd.dte)))
order by necm.date_month asc, necm.revenue_category
),

/* Rolling up by QTR END DTE */
new_expansion_churn_qtr_rollup as (
select 
qtr_end_dte,
revenue_category,
sum(arr) as arr
from new_expansion_churn_qtr_int
group by qtr_end_dte, revenue_category
order by qtr_end_dte asc
),

/* Combining Total ARR with New, Expansion, Churn ARR */
qtr_rev_cat_int as (
select * from total_arr_qtr
UNION 
select * from new_expansion_churn_qtr_rollup
order by qtr_end_dte asc, revenue_category
),

/* Pivoting data to get it in short, fat format */
pivot as (
select *
from qtr_rev_cat_int
pivot(sum(arr) for revenue_category in ('total','new','expansion','churn','de-book')) as p (qtr_end_dte, total, new, expansion, churn, de_book)
order by qtr_end_dte
),

/* Calculating recurring ARR and net new arr */ 
qtr_rev_cat_int1 as (
select distinct
qtr_end_dte as qtr_end_dte,
CASE WHEN total IS NULL then 0 else total end as total_arr,
CASE WHEN new IS NULL then 0 else new end as new_arr,
CASE WHEN expansion IS NULL then 0 else expansion end as expansion_arr,
CASE WHEN churn IS NULL then 0 else churn end as churn_arr,
CASE WHEN de_book IS NULL then 0 else de_book end as de_book_arr,
lag(total_arr,1,0) over (order by qtr_end_dte asc) as recurring_arr,
(new_arr + expansion_arr + churn_arr + de_book_arr) as net_new_arr
from pivot
order by qtr_end_dte
),

qtr_rev_cat as (
select distinct
qtr_end_dte,
CASE WHEN total_arr = 0 then (recurring_arr + net_new_arr) else total_arr end as total_arr,
new_arr,
expansion_arr,
churn_arr,
de_book_arr,
recurring_arr,
net_new_arr
from qtr_rev_cat_int1
order by qtr_end_dte asc
)

select * from qtr_rev_cat

