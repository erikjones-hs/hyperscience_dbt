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
sum(arr_high) as high,
sum(new_arr_actuals) as arr_mtd_actuals,
sum(rollover_current_qtr) as arr_rollover_qtr
from {{ ref('arr_forecast_sales_teams') }}
group by dte
order by dte asc
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

potential_churn as (
select distinct
churn_month,
sum(CASE WHEN open_opp_commit_status IS NULL OR open_opp_commit_status != 'Committed' then open_opp_net_new_arr else 0 end) as potential_churn_amount_non_commit,
sum(potential_churn_amount) as total_potential_churn_amount
from potential_churn_int
group by churn_month
order by churn_month asc
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

churn_budget as (
select distinct
last_day(to_date(date)) as dte,
churn_arr_budget
from "FIVETRAN_DATABASE"."GOOGLE_SHEETS"."FY_22_FORECAST_FINANCE_INPUTS"
where dte >= '2022-03-01'
order by dte asc
),

fct_budget_actuals as (
select distinct
fa.dte,
fd.qtr_end_dte,
fa.forecast_plan,
fa.original_plan,
fa.low,
fa.arr_committed,
fa.high,
fa.arr_mtd_actuals,
fa.arr_rollover_qtr,
-1*(ZEROIFNULL(pc.total_potential_churn_amount)) as total_potential_churn_amount,
-1*(ZEROIFNULL(pc.potential_churn_amount_non_commit)) as potential_churn_amount_non_commit,
-1*(ZEROIFNULL(ca.actual_churn_amount)) as actual_churn_amount,
-1*(cb.churn_arr_budget) as churn_budget
from forecast_agg as fa
left join potential_churn as pc on (to_date(fa.dte) = to_date(pc.churn_month))
left join churn_actuals as ca on (to_date(fa.dte) = to_date(ca.churn_month))
left join churn_budget as cb on (to_date(fa.dte) = to_date(cb.dte)) 
left join fy_dates as fd on (to_date(fa.dte) = to_date(fd.dte))
order by fa.dte asc 
),

fct_budget_actuals_variance as (
select distinct
dte,
qtr_end_dte,
forecast_plan as arr_forecast_plan,
original_plan as arr_budget,
low as arr_low,
arr_committed,
high as arr_high,
arr_mtd_actuals,
arr_rollover_qtr,
total_potential_churn_amount,
potential_churn_amount_non_commit,
actual_churn_amount,
churn_budget,
CASE WHEN to_date(dte) >= date_trunc('month',to_date(current_date())) then 0 else (churn_budget - actual_churn_amount) end as churn_budget_variance,
sum(churn_budget_variance) over (order by dte asc rows between unbounded preceding and current row) as churn_budget_variance_running_total,
datediff(month,to_date(dte), qtr_end_dte) + 1 as num_months_to_end_of_qtr
from fct_budget_actuals
order by dte asc
),

/* Calculating Budget Variance Rollover */
rollover_int as (
select distinct
dte,
CASE WHEN to_date(dte) = last_day(date_trunc('month', to_date(current_date()))) then (churn_budget_variance_running_total / num_months_to_end_of_qtr) else NULL end as rollover_monthly_int
from fct_budget_actuals_variance
order by dte asc                                 
),

/* Deriving only current month rollover */
current_rollover as (
select distinct
dte,
last_value(rollover_monthly_int ignore nulls) over (order by dte asc) as rollover_current_month
from rollover_int
order by dte asc
),

/* Pulling current QTR date */
current_qtr_int as (
select distinct
dte,
CASE WHEN to_date(dte) = last_day(date_trunc('month',to_date(current_date()))) then qtr_end_dte else NULL end as qtr_end_dte
from fct_budget_actuals_variance
),

current_qtr as (
select 
dte,
last_value(qtr_end_dte ignore nulls) over (order by dte asc) as current_qtr
from current_qtr_int
order by dte asc
),

fct_budget_variance_rollover as (
select distinct
fbav.dte,
fbav.qtr_end_dte,
cq.current_qtr,
fbav.arr_forecast_plan,
fbav.arr_budget,
fbav.arr_low,
fbav.arr_committed,
fbav.arr_high,
fbav.arr_mtd_actuals,
fbav.arr_rollover_qtr,
fbav.total_potential_churn_amount,
fbav.potential_churn_amount_non_commit,
fbav.actual_churn_amount,
fbav.churn_budget,
fbav.churn_budget_variance,
fbav.churn_budget_variance_running_total,
fbav.num_months_to_end_of_qtr,
cr.rollover_current_month
from fct_budget_actuals_variance as fbav 
left join current_rollover as cr on (to_date(fbav.dte) = to_date (cr.dte))
left join current_qtr as cq on (to_date(fbav.dte) = to_date(cq.dte))
order by fbav.dte asc
),

fct_budget_variance_forecast as (
select distinct 
dte,
qtr_end_dte,
current_qtr,
arr_forecast_plan,
arr_budget,
arr_low,
arr_committed,
arr_high,
arr_mtd_actuals,
arr_rollover_qtr,
total_potential_churn_amount,
potential_churn_amount_non_commit,
actual_churn_amount,
churn_budget,
churn_budget_variance,
churn_budget_variance_running_total,
num_months_to_end_of_qtr,
CASE WHEN qtr_end_dte = current_qtr then rollover_current_month else 0 end as rollover_current_month,
CASE WHEN qtr_end_dte = current_qtr then (churn_budget + rollover_current_month) else churn_budget end as churn_forecast_plan,
(arr_low - total_potential_churn_amount) as net_new_arr_low,
(arr_committed - potential_churn_amount_non_commit) as net_new_arr_committed,
(arr_high - potential_churn_amount_non_commit) as net_new_arr_high,
(arr_mtd_actuals - actual_churn_amount) as net_new_arr_actuals,
((arr_mtd_actuals + arr_committed) - (potential_churn_amount_non_commit + actual_churn_amount)) net_new_arr_committed_plus_actuals
from fct_budget_variance_rollover 
order by dte asc
)

select * from fct_budget_variance_forecast