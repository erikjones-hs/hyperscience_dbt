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
sum(opp_services_nrr) as nrr_actuals
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
.4 * best_case_nrr as arr_high_int,
(arr_high_int + nrr_committed) as nrr_high
from nrr_pipeline_int2
where to_date(dte) >= date_trunc('month',to_date(current_date()))
and to_date(dte) <= '2023-02-01' 
group by dte
order by dte asc
),

nrr_actuals_running_total_int as (
select distinct
naa.dte,
naa.nrr_actuals,
fd.fy_year,
fd.fy_qtr_year,
fd.qtr_end_dte,
fd.dte as dte1
from nrr_actuals_agg as naa 
right join fy_dates as fd on (naa.dte = fd.dte)
order by dte asc
),

nrr_actuals_running_total_fq as (
select distinct
dte,
qtr_end_dte,
sum(nrr_actuals) over (partition by qtr_end_dte order by dte asc rows between unbounded preceding and current row) as nrr_actuals_running_total_fq
from nrr_actuals_running_total_int
where dte is NOT NULL
order by qtr_end_dte asc, dte asc
),

nrr_pipeline_running_total_int as (
select distinct
npa.dte,
npa.nrr_committed,
npa.nrr_low,
npa.nrr_high,
fd.fy_year,
fd.fy_qtr_year,
fd.qtr_end_dte,
fd.dte as dte1
from nrr_pipeline_agg as npa 
right join fy_dates as fd on (npa.dte = fd.dte)
order by dte asc
),

nrr_pipeline_running_total_fq as (
select distinct
dte,
qtr_end_dte,
sum(nrr_committed) over (partition by qtr_end_dte order by dte asc rows between unbounded preceding and current row) as nrr_committed_running_total_fq,
sum(nrr_low) over (partition by qtr_end_dte order by dte asc rows between unbounded preceding and current row) as nrr_low_running_total_fq,
sum(nrr_high) over (partition by qtr_end_dte order by dte asc rows between unbounded preceding and current row) as nrr_high_running_total_fq
from nrr_pipeline_running_total_int
where dte is NOT NULL
order by qtr_end_dte asc, dte asc
),

nrr_forecast_running_total_int as (
select distinct
nf.dte,
nf.nrr_forecast,
fd.fy_year,
fd.fy_qtr_year,
fd.qtr_end_dte,
fd.dte as dte1
from nrr_forecast as nf 
right join fy_dates as fd on (nf.dte = fd.dte)
order by dte asc
),

nrr_forecast_running_total_fq as (
select distinct
dte,
qtr_end_dte,
sum(nrr_forecast) over (partition by qtr_end_dte order by dte asc rows between unbounded preceding and current row) as nrr_forecast_running_total_fq
from nrr_forecast_running_total_int
where dte is NOT NULL
order by qtr_end_dte asc, dte asc
),

fct_nrr as (
select distinct
to_timestamp(nf.dte) as dte,
nf.nrr_forecast,
naa.nrr_actuals,
npa.nrr_low,
npa.nrr_committed,
npa.nrr_high,
(npa.nrr_committed + naa.nrr_actuals) as nrr_committed_plus_actuals,
nfrtf.nrr_forecast_running_total_fq,
nprtf.nrr_low_running_total_fq,
nprtf.nrr_committed_running_total_fq,
nprtf.nrr_high_running_total_fq,
nartf.nrr_actuals_running_total_fq,
(nartf.nrr_actuals_running_total_fq + nprtf.nrr_committed_running_total_fq) as nrr_actuals_plus_committed_running_total_fq
from nrr_forecast as nf
left join nrr_actuals_agg as naa on (to_date(naa.dte) = to_date(nf.dte))
left join nrr_pipeline_agg as npa on (to_date(npa.dte) = to_date(nf.dte))
left join nrr_forecast_running_total_fq as nfrtf on (to_date(nfrtf.dte) = to_date(nf.dte))
left join nrr_pipeline_running_total_fq as nprtf on (to_date(nprtf.dte) = to_date(nf.dte))
left join nrr_actuals_running_total_fq as nartf on (to_date(nartf.dte) = to_date(nf.dte))
order by dte asc
)

select * from fct_nrr
