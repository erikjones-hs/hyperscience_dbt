{{ config
(
    materialized='table',
    database = 'PROD',
    schema = 'MONTHLY_KPIS'
)
}}

with raw_data as (
select * from {{ ref('fct_pipeline_history') }}
where opp_revenue_type in ('New Customer','Pilot')
),

qtr_rollup as (
select
qtr_end_dte,
sum(created_dte_fl) as new_opps,
sum(closed_won_dte_fl) as closed_won_opps,
sum(closed_lost_dte_fl) as closed_lost_opps,
sum(closed_dte_fl) as closed_opps
from raw_data
group by qtr_end_dte
order by qtr_end_dte asc
),

growth_acct_int as (
select distinct
qtr_end_dte,
new_opps,
closed_won_opps,
closed_lost_opps,
(new_opps - closed_won_opps - closed_lost_opps) as opp_change,
sum(opp_change) over (order by qtr_end_dte asc) as opp_running_total
from qtr_rollup 
order by qtr_end_dte asc
),

growth_acct as (
select distinct 
to_timestamp(qtr_end_dte) as qtr_end_dte,
CASE WHEN to_date(qtr_end_dte) = '2016-05-31' then 0 else lag(opp_running_total,1,0) over (order by qtr_end_dte asc) end as beginning_opps,
new_opps,
closed_won_opps,
closed_lost_opps,
opp_change,
CASE WHEN to_date(qtr_end_dte) = '2016-05-31' then opp_change else (beginning_opps + opp_change) end as ending_opps
from growth_acct_int
order by qtr_end_dte asc
)

select * from growth_acct