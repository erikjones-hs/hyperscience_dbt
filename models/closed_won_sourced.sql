{{ config
(
    materialized='table',
    database = 'PROD',
    schema = 'MONTHLY_KPIS'
)
}}

with lead_source as (
select distinct 
opp_id,
opp_lead_source
from "DBT"."DBT_EJONES"."AGG_OPPORTUNITY_INCREMENTAL"
where to_date(date_ran) = dateadd(day,-1,(to_date(current_date)))
),

/* Pulling in transformed SFDC data from arr_opp_history_transform model*/
raw_data_transformed as (
select * from "PROD"."MONTHLY_KPIS"."ARR_OPP_HISTORY_TRANSFORMED"
),

fct_sourced_int as (
select distinct
rdt.account_id,
rdt.account_name,
rdt.opp_id,
rdt.opp_name,
rdt.opp_revenue_type,
to_timestamp(rdt.end_dte) as end_dte,
to_timestamp(rdt.start_dte) as start_dte,  
to_timestamp(rdt.closed_won_dte) as closed_won_dte,
to_timestamp(rdt.start_dte_month) as start_dte_month,
to_timestamp(rdt.end_dte_month) as end_dte_month,
to_timestamp(rdt.closed_won_dte_month) as closed_won_dte_month,
rdt.opp_arr,
rdt.opp_net_new_arr,
ls.opp_lead_source
from raw_data_transformed as rdt
left join lead_source as ls on (rdt.opp_id = ls.opp_id)
order by rdt.account_id, start_dte asc
),

fy_dates as (
select distinct
dte,
month,
day_of_year,
day_of_qtr,
fy_quarter,
fy_year
from "DEV"."MARTS"."FY_CALENDAR"
where to_date(dte) >= '2016-01-01'
),

fct_sourced as (
select distinct 
account_id,
account_name,
opp_id,
opp_name,
opp_revenue_type,
end_dte,
start_dte,  
closed_won_dte,
start_dte_month,
end_dte_month,
closed_won_dte_month,
opp_arr,
opp_net_new_arr,
opp_lead_source,
fy.dte,
fy.month,
fy.day_of_qtr,
fy.fy_quarter,
fy.fy_year
from fct_sourced_int
left join fy_dates as fy on (to_date(start_dte) = to_date(fy.dte))
order by fy.dte asc
),

fy_agg as (
select distinct
fy_year, 
opp_lead_source,
sum(opp_net_new_arr) as net_new_arr,
sum(opp_arr) as total_arr,
count(distinct opp_id) as num_opps
from fct_sourced
group by opp_lead_source, fy_year
order by fy_year asc
)

select * from fy_agg