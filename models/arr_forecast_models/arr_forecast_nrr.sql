{{ config
(
    materialized='table',
    database = 'PROD',
    schema = 'ARR_FORECAST'
)
}}

with nrr_forecast as (
select distinct
to_date(date) as dte,
nrr_forecast
from "FIVETRAN_DATABASE"."GOOGLE_SHEETS"."FY_22_FORECAST_FINANCE_INPUTS"
where to_date(date) >= '2022-03-01'
and to_date(date) <= '2023-02-01'
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

nrr_opps_pipeline as (
select distinct
account_id,
account_name,
opp_id,
opp_name,
opp_commit_status,
opp_services_nrr,
opp_stage_name,
opp_start_dte,
opp_close_dte
from {{ ref('agg_opportunity_incremental') }}
where to_date(date_ran) = dateadd(day,-1,(to_date(current_date)))
and opp_stage_name not in ('Opp DQed', 'Closed Won')
and opp_revenue_type not in ('Renewal','License Overage')
and opportunity_owner not in ('Eli Berman')
and opp_commit_status in ('Committed','Best Case','Visible Opportunity','Pipeline')
and opp_name not in ('Mutual of Omaha-2020-21 auto renew')
and to_date(opp_close_dte) >= '2021-03-01'
and to_date(opp_close_dte) <= '2023-02-28'
),

nrr_opps_closed_won as (
select distinct
account_id,
account_name,
opp_id,
opp_name,
opp_commit_status,
opp_services_nrr,
opp_stage_name,
opp_start_dte,
opp_close_dte
from {{ ref('agg_opportunity_incremental') }}
where to_date(date_ran) = dateadd(day,-1,(to_date(current_date)))
and opp_stage_name in ('Closed Won')
and to_date(opp_close_dte) >= '2020-03-01'
and to_date(opp_close_dte) <= '2023-02-28'
),

nrr_pipeline_int1 as (
select distinct 
opp_id,
opp_name,
opp_commit_status,
opp_stage_name,
CASE WHEN opp_id = '0061R0000137hOKQAY' then 0
     WHEN opp_id = '0061R00000zAM8wQAG' then 0 
     WHEN opp_id = '0061R00000oE2hbQAC' then 0 
     else ZEROIFNULL(opp_services_nrr) end as opp_services_nrr,
opp_start_dte,
opp_close_dte
from nrr_opps_pipeline
order by opp_close_dte asc
),

nrr_closed_won_int1 as (
select distinct 
opp_id,
opp_name,
opp_commit_status,
opp_stage_name,
CASE WHEN opp_id = '0061R0000137hOKQAY' then 0
     WHEN opp_id = '0061R00000zAM8wQAG' then 0 
     WHEN opp_id = '0061R00000oE2hbQAC' then 0 
     else ZEROIFNULL(opp_services_nrr) end as opp_services_nrr,
opp_start_dte,
opp_close_dte
from nrr_opps_closed_won
order by opp_start_dte asc
),

nrr_pipeline_int2 as (
select distinct
opp_id,
opp_name,
opp_commit_status,
opp_stage_name,
opp_close_dte,
sum(opp_services_nrr) as opp_services_nrr
from nrr_pipeline_int1
group by opp_id, opp_commit_status, opp_stage_name, opp_close_dte, opp_name
order by opp_close_dte
),

nrr_closed_won_int2 as (
select distinct
opp_id,
opp_name,
opp_commit_status,
opp_stage_name,
opp_start_dte,
sum(opp_services_nrr) as opp_services_nrr
from nrr_closed_won_int1
group by opp_id, opp_commit_status, opp_stage_name, opp_start_dte, opp_name
order by opp_start_dte
),

nrr_actuals_agg as (
select distinct
date_trunc('month',opp_start_dte) as dte,
ZEROIFNULL(sum(opp_services_nrr)) as nrr_actuals
from nrr_closed_won_int2
where to_date(dte) >= '2022-03-01'
and to_date(dte) <= date_trunc('month',to_date(current_date()))
group by dte
order by dte asc 
),

nrr_pipeline_agg as (
select distinct 
date_trunc('month',opp_close_dte) as dte,
sum(CASE WHEN opp_commit_status = 'Committed' then opp_services_nrr else 0 end) as nrr_committed,
sum(CASE WHEN opp_commit_status = 'Best Case' then opp_services_nrr else 0 end) as best_case_nrr,
.75 * nrr_committed as nrr_low,
best_case_nrr as arr_high_int,
(arr_high_int + nrr_committed) as nrr_high
from nrr_pipeline_int2
where to_date(dte) >= date_trunc('month',to_date(current_date()))
and to_date(dte) <= '2023-02-01' 
group by dte
order by dte asc
),

/* Calculating Variance from Budget and Actuals */
fct_budget_variance as (
select distinct
to_timestamp(nf.dte) as dte,
fd.qtr_end_dte,
nf.nrr_forecast as nrr_budget,
naa.nrr_actuals,
CASE WHEN to_date(nf.dte) >= date_trunc('month',to_date(current_date())) then 0 else (nrr_budget - nrr_actuals) end as budget_variance,
sum(budget_variance) over (order by nf.dte asc rows between unbounded preceding and current row) as budget_variance_running_total,
datediff(month,to_date(nf.dte), qtr_end_dte) + 1 as num_months_to_end_of_qtr,
npa.nrr_low,
npa.nrr_committed,
npa.nrr_high
from nrr_forecast as nf
left join nrr_actuals_agg as naa on (to_date(naa.dte) = to_date(nf.dte))
left join nrr_pipeline_agg as npa on (to_date(npa.dte) = to_date(nf.dte))
left join fy_dates as fd on (to_date(nf.dte) = to_date(fd.dte))
order by dte asc
),

/* Calculating Budget Variance Rollover */
rollover_int as (
select distinct
dte,
CASE WHEN to_date(dte) = date_trunc('month', to_date(current_date())) then (budget_variance_running_total / num_months_to_end_of_qtr) else NULL end as rollover_monthly_int
from fct_budget_variance
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
CASE WHEN to_date(dte) = date_trunc('month',to_date(current_date())) then qtr_end_dte else NULL end as qtr_end_dte
from fct_budget_variance
),

current_qtr as (
select 
dte,
last_value(qtr_end_dte ignore nulls) over (order by dte asc) as current_qtr
from current_qtr_int
order by dte asc
),

/* Combining Budget, Actuals, Variance and Rollover */
fct_budget_variance_rollover as (
select distinct
fbv.dte,
fbv.qtr_end_dte,
cq.current_qtr,
fbv.nrr_budget,
fbv.nrr_actuals,
fbv.budget_variance,
fbv.budget_variance_running_total,
fbv.num_months_to_end_of_qtr,
cr.rollover_current_month,
fbv.nrr_low,
fbv.nrr_committed,
fbv.nrr_high
from fct_budget_variance as fbv
left join current_rollover as cr on (to_date(fbv.dte) = to_date (cr.dte))
left join current_qtr as cq on (to_date(fbv.dte) = to_date(cq.dte))
order by fbv.dte asc
),

/* Calculating Forecast Plan from rollover */
fct_budget_variance_forecast as (
select distinct 
dte,
qtr_end_dte,
current_qtr,
nrr_budget,
ZEROIFNULL(nrr_actuals) as nrr_actuals,
budget_variance,
budget_variance_running_total,
num_months_to_end_of_qtr,
rollover_current_month,
CASE WHEN qtr_end_dte = current_qtr then (nrr_budget + rollover_current_month) else nrr_budget end as forecast_plan,
nrr_low,
nrr_committed,
nrr_high
from fct_budget_variance_rollover
order by dte
)

select * from fct_budget_variance_forecast

