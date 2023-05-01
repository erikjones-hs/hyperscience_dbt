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
qtr_rev_cat as (
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
where to_date(qtr_end_dte) <= to_date(current_date())
order by qtr_end_dte
),

/* Bringing in forecast data */
forecast as (
select distinct
last_day(to_date(date)) as qtr_end_dte,
arr_budget as total_arr,
new_arr_budget as new_arr,
expansion_arr_budget as expansion_arr,
churn_arr_budget as churn_arr,
0 as de_book_arr,
recurring_arr_budget as recurring_arr,
net_new_arr_budget as net_new_arr
from "FIVETRAN_DATABASE"."GOOGLE_SHEETS"."FY_22_FORECAST_FINANCE_INPUTS"
order by last_day(to_date(date))
),

/* Combining Forecast Data with FY QTR End Dates */
/* Because we need to cum sum over each FY QTR to get correct numbers */
forecast_int as (
select distinct
f.qtr_end_dte as month_end_dte,
f.total_arr,
f.new_arr,
f.expansion_arr,
f.churn_arr,
f.de_book_arr,
f.recurring_arr,
f.net_new_arr,
fd.qtr_end_dte
from forecast as f
left join fy_dates as fd on (to_date(f.qtr_end_dte) = to_date(fd.dte))
order by f.qtr_end_dte asc
),

/* Aggregatig by fiscal quarter */
forecast_qtr_new_expansion_churn_agg as (
select distinct
qtr_end_dte,
sum(new_arr) as new_arr,
sum(expansion_arr) as expansion_arr,
sum(churn_arr) as churn_arr,
sum(de_book_arr) as de_book_arr,
sum(net_new_arr) as net_new_arr
from forecast_int
group by qtr_end_dte
order by qtr_end_dte asc
),

prev_qtr_ending_arr_actuals as (
select distinct
qtr_end_dte,
(new_arr + expansion_arr + churn_arr + de_book_arr + recurring_arr) as ending_arr,
row_number() over (order by qtr_end_dte desc) as row_num
from qtr_rev_cat
qualify row_num = 1
order by qtr_end_dte
),

/* Looking at just Total and Recurring ARR */
/* Because these do NOT get summed by QTR */
/* We look at these as of quarter end date */
forecast_qtr_total_recurring_int as (
select distinct
qtr_end_dte,
total_arr,
row_number() over (order by qtr_end_dte asc) as row_num
from forecast_int
where monthname(month_end_dte) in ('Feb','May','Aug','Nov')
and qtr_end_dte > to_date(current_date())
order by qtr_end_dte asc
),

/* FOR THE CURRENT QTR NEED TO PULL IN ENDING ACTUALS FROM LAST COMPLETE QTR */
forecast_qtr_total_recurring as (
select distinct
qtr_end_dte,
row_num,
total_arr,
CASE WHEN row_num = 1 then (select ending_arr from prev_qtr_ending_arr_actuals)
     ELSE lag(total_arr,1) over (order by qtr_end_dte asc) end as recurring_arr
from forecast_qtr_total_recurring_int 
),

/* Combining New, Churn, Expansion, Total and Recurring ARR */
forecast_combined as (
select 
fqneca.qtr_end_dte,
fqtr.total_arr,
fqneca.new_arr,
fqneca.expansion_arr,
fqneca.churn_arr,
fqneca.de_book_arr,
fqtr.recurring_arr,
fqneca.net_new_arr  
from forecast_qtr_new_expansion_churn_agg as fqneca
left join forecast_qtr_total_recurring as fqtr on (fqneca.qtr_end_dte = fqtr.qtr_end_dte)
where to_date(fqneca.qtr_end_dte) > to_date(current_date())
order by fqneca.qtr_end_dte asc
),

actuals_forecast_int as (
select * from qtr_rev_cat
UNION 
select * from forecast_combined
order by qtr_end_dte
),

actuals_forecast as (
select distinct
to_timestamp(qtr_end_dte) as qtr_end_dte,
total_arr,
new_arr,
expansion_arr,
churn_arr,
de_book_arr,
recurring_arr,
net_new_arr
from actuals_forecast_int
order by qtr_end_dte asc 
)

select * from actuals_forecast

