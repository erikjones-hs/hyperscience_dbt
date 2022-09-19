{{ config
(
    materialized='table',
    database = 'PROD',
    schema = 'MONTHLY_KPIS'
)
}}

with fct_pipeline_time_in_stage as (
select 
to_timestamp(date_ran) as date_ran,
opp_id,
opp_name,
account_name,
account_industry,
account_sales_region,
partner_account_name,
opp_stage_name,
opp_lead_source,
opp_is_marketing_influenced_flag,
opp_created_dte,
opp_discovery_call_dte,
opp_vf_dte,
opp_tdd_dte,
opp_eb_go_no_go_dte,
opp_poc_dte,
opp_eb_review_dte,
opp_neg_and_close_dte,
opp_close_dte,
opp_closed_won_dte,
opp_arr,
opp_net_new_arr, 
opportunity_owner,
owner_description,
opp_pipeline_category,
opp_revenue_type,
datediff(days,to_date(opp_created_dte),to_date(current_date())) as time_in_pipeline,
CASE WHEN opp_discovery_call_dte IS NOT NULL then datediff(day,to_date(opp_discovery_call_dte),to_date(current_date())) else NULL end as time_in_discovery,
CASE WHEN opp_vf_dte IS NOT NULL then datediff(day,to_date(opp_vf_dte),to_date(current_date())) else NULL end as time_in_vf,
CASE WHEN opp_tdd_dte IS NOT NULL then datediff(day,to_date(opp_tdd_dte),to_date(current_date())) else NULL end as time_in_tdd,
CASE WHEN opp_eb_go_no_go_dte IS NOT NULL then datediff(day,to_date(opp_eb_go_no_go_dte),to_date(current_date())) else NULL end as time_in_go_no,
CASE WHEN opp_poc_dte IS NOT NULL then datediff(day,to_date(opp_poc_dte),to_date(current_date())) else NULL end as time_in_poc,
CASE WHEN opp_eb_review_dte IS NOT NULL then datediff(day,to_date(opp_eb_review_dte),to_date(current_date())) else NULL end as time_in_eb_review,
CASE WHEN opp_neg_and_close_dte IS NOT NULL then datediff(day,to_date(opp_neg_and_close_dte),to_date(current_date())) else NULL end as time_in_neg_and_close
from {{ ref('agg_opportunity_incremental') }}
where opp_stage_name not in ('Closed Won','Opp DQed')  
and date_ran = dateadd(day,-1,(to_date(current_date)))
order by date_ran asc, opp_stage_name
)

select * from fct_pipeline_time_in_stage