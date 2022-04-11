{{ config
(
    materialized='table',
    database = 'PROD',
    schema = 'MONTHLY_KPIS'
)
}}

with fct_opp_stage_flags as (
select distinct
opp_id,
opp_name,
account_id,
account_name,
account_industry,
account_sales_region,
opp_partner_account,
partner_account_name,
opp_revenue_type,
opp_is_marketing_influenced_flag,
opp_is_partner_influenced_flag,
opp_created_dte,
opp_had_discovery_call_flag,
opp_had_vf_flag,
opp_had_tdd_flag,
opp_had_eb_go_no_go_flag,
opp_had_poc_flag,
opp_had_eb_review_flag,
opp_had_neg_and_close_flag,
opp_closed_won_flag,
opp_arr,
opp_net_new_arr,
opportunity_owner
from {{ ref('agg_opportunity_incremental') }}
where date_ran = dateadd(day,-1,(to_date(current_date)))
),

pilots as (
select * 
from fct_opp_stage_flags 
where lower(opp_name) like '%pilot%'
),

fct_stage_yield_rates as (
select * from fct_opp_stage_flags
where opp_id not in (select opp_id from pilots)
)

select * from fct_stage_yield_rates