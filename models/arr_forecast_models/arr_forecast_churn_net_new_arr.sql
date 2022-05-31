{{ config
(
    materialized='table',
    database = 'PROD',
    schema = 'ARR_FORECAST'
)
}}

with forecast_agg as (
select distinct
dte,
sum(forecast_plan) as forecast_plan,
sum(budget) as original_plan,
sum(arr_low) as low,
sum(arr_committed) as arr_committed,
sum(high_best_case) as high,
sum(new_arr_actuals) as arr_mtd_actuals,
sum(sales_budget_running_total) as original_plan_running_total,
sum(actuals_running_total) as arr_running_total
from {{ ref('arr_forecast_sales_teams') }}
group by dte
order by dte asc
),

potential_churn_int as (
select distinct
last_day(renewal_month) as churn_month,
open_opp_commit_status,
sum(potential_churn_amount) as potential_churn_amount,
sum(net_new_arr) as open_opp_net_new_arr
from {{ ref('renewals_current_fy_opps') }}
group by churn_month, open_opp_commit_status
order by churn_month asc, open_opp_commit_status
),

fy_dates as (
select distinct 
dte,
CASE WHEN month in (1,2) then dateadd('year',-1,date_trunc(year,dte)) ELSE date_trunc(year,dte) end as fy_year,
fy_qtr_year,
qtr_end_dte
from "DEV"."MARTS"."FY_CALENDAR"
where to_date(dte) >= '2022-03-01'
and to_date(dte) <= '2023-02-28'
),

potential_churn as (
select distinct
churn_month,
sum(CASE WHEN open_opp_commit_status IS NULL OR open_opp_commit_status != 'Committed' then open_opp_net_new_arr else 0 end) as potential_churn_amount_non_commit,
sum(potential_churn_amount) as total_potential_churn_amount
from potential_churn_int
group by churn_month
order by churn_month asc
),

potential_churn_running_total_int as (
select distinct
pc.churn_month,
pc.total_potential_churn_amount,
pc.potential_churn_amount_non_commit,
fd.fy_year,
fd.fy_qtr_year,
fd.qtr_end_dte,
fd.dte
from potential_churn as pc 
right join fy_dates as fd on (pc.churn_month = fd.dte)
order by dte asc
),

potential_churn_running_total_fq as (
select distinct
churn_month,
qtr_end_dte,
sum(total_potential_churn_amount) over (partition by qtr_end_dte order by churn_month asc rows between unbounded preceding and current row) as total_potential_churn_running_total_fq,
sum(potential_churn_amount_non_commit) over (partition by qtr_end_dte order by churn_month asc rows between unbounded preceding and current row) as potential_churn_non_commit_running_total_fq
from potential_churn_running_total_int
where churn_month IS NOT NULL
order by qtr_end_dte asc, churn_month asc
),

churn_actuals as (
select distinct
last_day(date_month) as churn_month,
SUM(CASE WHEN mrr_change_acct < 0 then mrr_change_acct else 0.000000000001 end) as actual_churn_amount
from {{ ref('fct_arr_account') }}
where to_date(date_month) >= '2022-03-01'
and to_date(date_month) <= date_trunc('month',to_date(current_date()))
group by churn_month
order by churn_month asc 
),

actuals_running_total_int as (
select distinct
ca.churn_month,
ca.actual_churn_amount,
fd.fy_year,
fd.fy_qtr_year,
fd.qtr_end_dte,
fd.dte
from churn_actuals as ca 
right join fy_dates as fd on (ca.churn_month = fd.dte)
order by dte asc
),

actuals_running_total_fq as (
select distinct
churn_month,
qtr_end_dte,
sum(actual_churn_amount) over (partition by qtr_end_dte order by churn_month asc rows between unbounded preceding and current row) as churn_running_total_fq
from actuals_running_total_int
where churn_month is NOT NULL
order by qtr_end_dte asc, churn_month asc
),

churn_forecast as (
select distinct
last_day(to_date(date)) as dte,
churn_arr_budget
from "FIVETRAN_DATABASE"."GOOGLE_SHEETS"."FY_22_FORECAST_FINANCE_INPUTS"
where dte >= '2022-03-01'
order by dte asc
),

churn_forecast_running_total_int as (
select distinct
cf.dte as date,
cf.churn_arr_budget,
fd.fy_year,
fd.fy_qtr_year,
fd.qtr_end_dte,
fd.dte
from churn_forecast as cf 
right join fy_dates as fd on (cf.dte = fd.dte)
order by dte asc
),

churn_forecast_running_total_fq as (
select distinct
date,
qtr_end_dte,
sum(churn_arr_budget) over (partition by qtr_end_dte order by date asc rows between unbounded preceding and current row) as churn_budget_running_total_fq
from churn_forecast_running_total_int
where date IS NOT NULL
order by qtr_end_dte asc, date asc
),

arr_running_total_int as (
select distinct
fa.dte,
fa.original_plan,
fa.low,
fa.arr_committed,
fa.high,
fa.arr_mtd_actuals,
fd.qtr_end_dte,
fd.dte as dte1
from forecast_agg as fa 
right join fy_dates as fd on (fa.dte = fd.dte)
order by dte asc
),

arr_running_total_fq as (
select distinct
dte,
qtr_end_dte,
sum(original_plan) over (partition by qtr_end_dte order by dte asc rows between unbounded preceding and current row) as original_plan_running_total_fq,
sum(low) over (partition by qtr_end_dte order by dte asc rows between unbounded preceding and current row) as arr_low_running_total_fq,
sum(arr_committed) over (partition by qtr_end_dte order by dte asc rows between unbounded preceding and current row) as arr_committed_running_total_fq, 
sum(high) over (partition by qtr_end_dte order by dte asc rows between unbounded preceding and current row) as arr_high_running_total_fq, 
sum(arr_mtd_actuals) over (partition by qtr_end_dte order by dte asc rows between unbounded preceding and current row) as arr_actuals_running_total_fq
from arr_running_total_int
where dte IS NOT NULL
order by qtr_end_dte asc, dte asc
),

fct_actuals_forecast_int as (
select distinct
fa.dte,
fa.forecast_plan,
fa.original_plan,
fa.low,
fa.arr_committed,
fa.high,
fa.arr_mtd_actuals,
fa.original_plan_running_total,
fa.arr_running_total,
-1*(ZEROIFNULL(pc.total_potential_churn_amount)) as total_potential_churn_amount,
-1*(ZEROIFNULL(pc.potential_churn_amount_non_commit)) as potential_churn_amount_non_commit,
-1*(ZEROIFNULL(ca.actual_churn_amount)) as actual_churn_amount,
-1*(ZEROIFNULL(artf.churn_running_total_fq)) as actual_churn_running_total_fq,
-1*(cf.churn_arr_budget) as churn_budget,
-1*(cfrtf.churn_budget_running_total_fq) as churn_budget_running_total_fq,
-1*(pcrtf.total_potential_churn_running_total_fq) as total_potential_churn_running_total_fq,
-1*(pcrtf.potential_churn_non_commit_running_total_fq) as potential_churn_non_commit_running_total_fq,
artf2.original_plan_running_total_fq,
artf2.arr_low_running_total_fq,
artf2.arr_committed_running_total_fq,
artf2.arr_high_running_total_fq,
artf2.arr_actuals_running_total_fq
from forecast_agg as fa
left join potential_churn as pc on (to_date(fa.dte) = to_date(pc.churn_month))
left join churn_actuals as ca on (to_date(fa.dte) = to_date(ca.churn_month))
left join churn_forecast as cf on (to_date(fa.dte) = to_date(cf.dte)) 
left join actuals_running_total_fq as artf on (to_date(fa.dte) = to_date(artf.churn_month))
left join churn_forecast_running_total_fq as cfrtf on (to_date(fa.dte) = to_date(cfrtf.date)) 
left join potential_churn_running_total_fq as pcrtf on (to_date(fa.dte) = to_date(pcrtf.churn_month)) 
left join arr_running_total_fq as artf2 on (to_date(fa.dte) = to_date(artf2.dte)) 
order by fa.dte asc 
),

fct_actuals_forecast_int2 as (
select distinct
dte,
CASE WHEN dte < last_day(date_trunc('month',to_date(current_date()))) then original_plan else forecast_plan end as forecast_plan,
original_plan,
low as arr_low,
arr_committed,
high as arr_high,
arr_mtd_actuals,
(arr_committed + arr_mtd_actuals) as arr_committed_plus_actuals,
total_potential_churn_amount,
potential_churn_amount_non_commit,
actual_churn_amount,
actual_churn_running_total_fq,
lag(actual_churn_running_total_fq,1,0) over (order by dte asc) as actual_churn_running_total_fq_lag1,
churn_budget,
churn_budget_running_total_fq,
(total_potential_churn_amount + actual_churn_amount) as total_potential_churn_actuals,
(potential_churn_amount_non_commit + actual_churn_amount) as lowest_potential_churn_actuals,
CASE WHEN dte < last_day(date_trunc('month',to_date(current_date()))) then churn_budget else (churn_budget_running_total_fq - actual_churn_running_total_fq_lag1) end as churn_forecast_plan,
(arr_mtd_actuals - actual_churn_amount) as net_new_arr_actuals,
total_potential_churn_running_total_fq,
potential_churn_non_commit_running_total_fq,
original_plan_running_total_fq,
arr_low_running_total_fq,
arr_committed_running_total_fq,
arr_high_running_total_fq,
arr_actuals_running_total_fq
from fct_actuals_forecast_int
order by dte asc
),

fct_actuals_forecast as (
select distinct
dte,
forecast_plan,
original_plan,
arr_low,
arr_committed,
arr_high,
arr_mtd_actuals,
arr_committed_plus_actuals,
total_potential_churn_amount,
potential_churn_amount_non_commit,
actual_churn_amount,
actual_churn_running_total_fq,
actual_churn_running_total_fq_lag1,
churn_budget,
churn_budget_running_total_fq,
total_potential_churn_actuals,
lowest_potential_churn_actuals,
churn_forecast_plan,
net_new_arr_actuals,
(forecast_plan - churn_forecast_plan) as net_new_arr_forecast_plan,
CASE WHEN dte < last_day(date_trunc('month',to_date(current_date()))) then actual_churn_amount else (churn_forecast_plan + actual_churn_amount) end as churn_forecast_actuals,
((arr_low + arr_mtd_actuals) - total_potential_churn_amount) as net_new_arr_low,
((arr_committed + arr_mtd_actuals) - potential_churn_amount_non_commit) as net_new_arr_committed,
((arr_high + arr_mtd_actuals) - potential_churn_amount_non_commit) as net_new_arr_high,
(arr_committed_plus_actuals - (potential_churn_amount_non_commit + actual_churn_amount)) as net_new_arr_committed_plus_actuals,
total_potential_churn_running_total_fq,
potential_churn_non_commit_running_total_fq,
original_plan_running_total_fq,
arr_low_running_total_fq,
arr_committed_running_total_fq,
arr_high_running_total_fq,
arr_actuals_running_total_fq
from fct_actuals_forecast_int2
order by dte asc
)

select * from fct_actuals_forecast