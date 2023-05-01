{{ config
(
    materialized='table',
    database = 'PROD',
    schema = 'MONTHLY_KPIS'
)
}}

with fct_pipeline as (
select distinct 
to_timestamp(date_ran) as date_ran,
opp_id,
opp_name,
account_name,
account_industry,
account_sales_region,
partner_account_name,
opp_stage_name,
CASE WHEN opp_lead_source = 'Zoominfo' then 'Outbound Sales'
     WHEN opp_lead_source IS NULL and opp_is_partner_influenced_flag = 1 then 'Partner'
     WHEN opp_lead_source IS NULL and opp_is_partner_influenced_flag = 0 then 'Outbound Sales'
     else opp_lead_source end as opp_lead_source,
opp_is_marketing_influenced_flag,
opp_is_partner_influenced_flag,
opp_close_dte,
opp_arr,
opp_net_new_arr, 
opportunity_owner,
owner_description,
opp_pipeline_category,
opp_revenue_type,
opp_commit_status
from {{ ref('agg_opportunity_incremental') }}
where opp_stage_name not in ('Closed Won','Opp DQed','Closed Lost') 
and date_ran = dateadd(day,-1,(to_date(current_date)))
and to_date(opp_close_dte) >= dateadd(days,-90,to_date(current_date()))
and opp_revenue_type in ('New Customer','Pilot')
and opp_arr > 0 
and opp_arr IS NOT NULL 
order by date_ran asc
)

select * from fct_pipeline order by date_ran asc