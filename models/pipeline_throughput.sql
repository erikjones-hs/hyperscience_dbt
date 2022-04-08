{{ config
(
    materialized='table',
    database = 'PROD',
    schema = 'MONTHLY_KPIS'
)
}}

with fct_pipeline as (
select 
to_timestamp(date_ran) as date_ran,
opp_id,
opp_name,
account_name,
opp_stage_name,
opp_revenue_type,
opp_arr,
opp_net_new_arr,
opp_created_dte,
opp_closed_won_dte
from {{ ref('agg_opportunity_incremental') }}
where opp_stage_name not in ('Closed Won','Opp DQed') 
and opp_pipeline_category not in ('other')
and date_ran = last_day(to_date(date_ran))
order by date_ran asc
),

fct_closed_dq as (
select 
to_timestamp(date_ran) as date_ran,
opp_id,
opp_name,
account_name,
opp_stage_name,
opp_revenue_type,
opp_arr,
opp_net_new_arr,
opp_created_dte,
opp_closed_won_dte
from {{ ref('agg_opportunity_incremental') }}
where opp_stage_name in ('Closed Won','Opp DQed')
and opp_pipeline_category not in ('other')
and date_ran = last_day(to_date(date_ran))
order by date_ran asc
),

fct_current as (
select
to_timestamp(date_ran) as date_ran,
opp_id,
opp_name,
account_name,
opp_stage_name,
opp_revenue_type,
opp_arr,
opp_net_new_arr,
opp_created_dte,
opp_closed_won_dte
from {{ ref('agg_opportunity_incremental') }}
where date_ran = dateadd(day,-1,(to_date(current_date)))
and opp_pipeline_category not in ('other')
order by date_ran asc
),

fct_all as (
select * from fct_pipeline
UNION 
select * from fct_closed_dq
UNION 
select * from fct_current
order by opp_id, date_ran
),

fct_all_next_month_int as (
select distinct
date_ran,
opp_id,
opp_name,
account_name,
opp_revenue_type,
opp_arr,
opp_net_new_arr,
lag(opp_stage_name,1) over (partition by opp_id order by date_ran asc) as prev_month_stage_name,
opp_stage_name as current_stage_name,
lead(opp_stage_name,1) over (partition by opp_id order by date_ran asc) as next_month_stage_name,
opp_created_dte,
opp_closed_won_dte
from fct_all
order by opp_id, date_ran  
),

/* Excluding Opps that came into October as either Closed Won or Opp DQed already */
/* Only want to look at pipeline opps */
/* Ignorig September 2021 since it was our first snapshot month */
opp_exclusions as (
select distinct
opp_id
from fct_all_next_month_int
where date_ran = '2021-10-31' 
AND ((prev_month_stage_name = 'Opp DQed' and current_stage_name = 'Opp DQed' and next_month_stage_name = 'Opp DQed')
       OR (prev_month_stage_name = 'Closed Won' and current_stage_name = 'Closed Won' and next_month_stage_name = 'Closed Won')) 
),

fct_base_table as (
select distinct 
date_ran,
opp_id,
opp_name,
account_name,
opp_revenue_type,
opp_arr,
opp_net_new_arr,
prev_month_stage_name,
current_stage_name,
next_month_stage_name
from fct_all_next_month_int
where opp_id not in (select * from opp_exclusions)
),

fct_all_next_month as (
select distinct
date_ran,
opp_id,
opp_name,
account_name,
opp_revenue_type,
prev_month_stage_name,
current_stage_name,
next_month_stage_name,
opp_arr,
opp_net_new_arr,
CASE WHEN ((prev_month_stage_name IS NULL and current_stage_name not in ('Opp DQed','Closed Won')) or (prev_month_stage_name = 'Opp DQed' and current_stage_name not in ('Opp DQed'))) then 1 else 0 end as opp_new_flag, 
CASE WHEN (prev_month_stage_name not in ('Opp DQed') or prev_month_stage_name IS NULL) and current_stage_name = 'Opp DQed' then 1 else 0 end as opp_dq_flag,
CASE WHEN (prev_month_stage_name not in ('Closed Won') or prev_month_stage_name IS NULL) and current_stage_name = 'Closed Won' then 1 else 0 end as opp_closed_won_flag,
CASE WHEN prev_month_stage_name in ('AE Discovery','EB Go/No-Go','TDD','TVE','EB Revisit','Value/Fit','Negotiate and Close') 
                                and current_stage_name in ('AE Discovery','EB Go/No-Go','TDD','TVE','EB Revisit','Value/Fit','Negotiate and Close') then 1 else 0 end as opp_did_not_move_flag,
CASE WHEN current_stage_name in ('AE Discovery','EB Go/No-Go','TDD','TVE','EB Revisit','Value/Fit','Negotiate and Close') and next_month_stage_name IS NULL then 1 else 0 end as opp_deleted_flag, 
CASE WHEN current_stage_name in ('AE Discovery','EB Go/No-Go','TDD','TVE','EB Revisit','Value/Fit','Negotiate and Close') then 1 else 0 end as ending_opps_flag
from fct_base_table
where date_ran < to_date(current_date())
order by opp_id, date_ran
),

month_rollup as (
select 
date_trunc('month',to_date(date_ran)) as dte_month,
count(distinct opp_id) as num_opps,
sum(opp_new_flag) as new,
sum(opp_closed_won_flag) as closed,
sum(opp_dq_flag) as dqed,  
sum(opp_did_not_move_flag) as did_not_move,
sum(opp_deleted_flag) as deleted,
sum(ending_opps_flag) as ending_opps
from fct_all_next_month
group by dte_month
order by dte_month asc
),

monthly_balance_sheet_int as (
select distinct
dte_month,
new as new_opps,
closed as closed_opps,
dqed as dq_opps,
did_not_move as pipeline_opps,
ending_opps as ending_opps_int,
deleted as deleted_opps,
row_number() over (order by dte_month asc) as row_num
from month_rollup
order by dte_month asc
),

monthly_throughput as (
select distinct 
to_timestamp(dte_month) as dte_month,
lag(ending_opps_int,1) over (order by dte_month asc) as beginning_opps,
new_opps,
closed_opps,
dq_opps,
(dq_opps + deleted_opps) as tot_dq_opps,
pipeline_opps,
ending_opps_int,
deleted_opps,
(beginning_opps + new_opps - closed_opps - dq_opps - deleted_opps) as ending_opps
from monthly_balance_sheet_int
order by dte_month asc
)

select * from monthly_throughput