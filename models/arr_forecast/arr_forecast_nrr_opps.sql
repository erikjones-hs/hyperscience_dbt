{{ config
(
    materialized='table',
    database = 'PROD',
    schema = 'ARR_FORECAST'
)
}}

with nrr_opps_pipeline as (
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
opp_close_dte,
account_name
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
opp_close_dte,
account_name
from nrr_opps_closed_won
order by opp_start_dte asc
),

nrr_pipeline_int2 as (
select distinct
opp_id,
opp_name,
opp_commit_status,
opp_stage_name,
opp_close_dte as dte,
account_name,
sum(opp_services_nrr) as opp_services_nrr
from nrr_pipeline_int1
group by opp_id, opp_commit_status, opp_stage_name, opp_close_dte, opp_name, account_name
order by opp_close_dte
),

nrr_closed_won_int2 as (
select distinct
opp_id,
opp_name,
opp_commit_status,
opp_stage_name,
opp_start_dte as dte,
account_name,
sum(opp_services_nrr) as opp_services_nrr
from nrr_closed_won_int1
group by opp_id, opp_commit_status, opp_stage_name, opp_start_dte, opp_name, account_name
order by opp_start_dte
),

fct_nrr_opp_int as (
select * from nrr_closed_won_int2
UNION 
select * from nrr_pipeline_int2
order by dte asc
),

fct_nrr_opp as (
select distinct
to_timestamp(dte) as dte,
opp_id,
opp_name,
account_name,
opp_commit_status,
opp_stage_name,
opp_services_nrr
from fct_nrr_opp_int
where to_date(dte) >= '2022-03-01'
order by dte asc
)

select * from fct_nrr_opp