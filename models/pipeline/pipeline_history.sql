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
account_industry,
account_sales_region,
partner_account_name,
opp_revenue_type,
opp_stage_name,
opp_lead_source,
opp_is_marketing_influenced_flag,
opp_created_dte,
opp_close_dte,
opp_closed_won_dte,
opp_arr,
opp_net_new_arr, 
opportunity_owner,
owner_description,
opp_pipeline_category
from {{ ref('agg_opportunity_incremental') }}
where opp_stage_name not in ('Closed Won','Opp DQed','Closed Lost')  
and date_ran = last_day(to_date(date_ran))
order by date_ran asc
),

fct_closed_won_int as (
select
to_timestamp(date_ran) as date_ran,
opp_id,
opp_name,
account_name,
account_industry,
account_sales_region,
partner_account_name,
opp_revenue_type,
opp_stage_name,
opp_lead_source,
opp_is_marketing_influenced_flag,
opp_created_dte,
opp_close_dte,
opp_closed_won_dte,
opp_arr,
CASE WHEN opp_net_new_arr < 0 then 0 else opp_net_new_arr end as opp_net_new_arr, 
opportunity_owner,
owner_description,
opp_pipeline_category
from {{ ref('agg_opportunity_incremental') }}
where opp_stage_name in ('Closed Won')
and date_ran = last_day(to_date(date_ran))
order by date_ran asc
),

fct_closed_won as (
select
to_timestamp(date_ran) as date_ran,
opp_id,
opp_name,
account_name,
account_industry,
account_sales_region,
partner_account_name,
opp_revenue_type,
opp_stage_name,
opp_lead_source,
opp_is_marketing_influenced_flag,
opp_created_dte,
opp_close_dte,
opp_closed_won_dte,
opp_arr,
opp_net_new_arr, 
opportunity_owner,
owner_description,
opp_pipeline_category
from fct_closed_won_int
where date_trunc('month',to_date(opp_closed_won_dte)) = date_trunc('month',to_date(date_ran))
AND date_trunc('year',to_date(opp_closed_won_dte)) = date_trunc('year',to_date(date_ran)) 
and date_ran = last_day(to_date(date_ran))
order by date_ran asc
),

fct_pipeline_all_stages as (
select * from fct_pipeline
UNION
select * from fct_closed_won
order by date_ran asc, opp_stage_name
)

select * from fct_pipeline_all_stages