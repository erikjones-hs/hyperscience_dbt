{{ config
(
    materialized ='incremental',
    database = 'DBT',
    schema = 'DBT_EJONES'
)
}}

with opp as (
select distinct
id as opp_id,
name as opp_name,
account_id as account_id,
partner_account_c as opp_partner_account,
stage_name as opp_stage_name,
CASE WHEN stage_name = 'AE Discovery' then '1. AE Discoverey'
     WHEN stage_name = 'Value/Fit' then '2. Value/Fit'
     WHEN stage_name = 'TDD' then '3. TDD'
     WHEN stage_name = 'EB Go/No-Go' then '4. EB Go/No-Go'
     WHEN stage_name = 'TVE' then '5. TVE'
     WHEN stage_name = 'EB Revisit' then '6. EB Revisit'
     WHEN stage_name = 'Negotiate and Close' then '7. Negotiate and Close'
     WHEN stage_name = 'Closed Won' then '8. Closed Won'
     WHEN stage_name = 'Opp DQed' then '9. Opp DQed'
     ELSE 'Other' end as opp_stage_name_ordered,
CASE WHEN stage_name in ('AE Discovery','Value/Fit','TDD','EB Go/No-Go','TVE','EB Revisit','Negotiate and Close','Closed Won') then 1 else 0 end as opp_discovery_flag,
CASE WHEN stage_name in ('Value/Fit','TDD','EB Go/No-Go','TVE','EB Revisit','Negotiate and Close','Closed Won') then 1 else 0 end as opp_vf_flag,
CASE WHEN stage_name in ('TDD','EB Go/No-Go','TVE','EB Revisit','Negotiate and Close','Closed Won') then 1 else 0 end as opp_tdd_flag,
CASE WHEN stage_name in ('EB Go/No-Go','TVE','EB Revisit','Negotiate and Close','Closed Won') then 1 else 0 end as opp_eb_go_no_go_flag,
CASE WHEN stage_name in ('TVE','EB Revisit','Negotiate and Close','Closed Won') then 1 else 0 end as opp_tve_flag,
CASE WHEN stage_name in ('EB Revisit','Negotiate and Close','Closed Won') then 1 else 0 end as opp_eb_revisit_flag,
CASE WHEN stage_name in ('Negotiate and Close','Closed Won') then 1 else 0 end as opp_neg_close_flag,
CASE WHEN stage_name in ('Closed Won') then 1 else 0 end as opp_closed_won_flag,
CASE WHEN stage_name in ('Opp DQed') then 1 else 0 end as opp_dq_flag,
CASE WHEN stage_name in ('Cloed Lost') then 1 else 0 end as opp_closed_lost_flag,
active_opportunity_c as opp_is_active,
CASE WHEN lower(active_opportunity_c) = 'true' then 1 else 0 end as opp_is_active_flag,
revenue_type_c as opp_revenue_type,
lead_source as opp_lead_source,
CASE WHEN (lead_source in ('Partner') or partner_account_c IS NOT NULL) then 'partner' else 'non-partner' end as opp_partner_influence,
CASE WHEN (lead_source in ('Partner') or partner_account_c IS NOT NULL) then 1 else 0 end as opp_is_partner_influenced_flag,  
secondary_lead_source_c as opp_secondary_lead_source,
owner_id as opp_owner_id,
commit_status_c as opp_commit_status,
CASE WHEN commit_status_c != 'Pipeline' then 'qualified_pipeline'
     WHEN commit_status_c = 'Pipeline' then 'pipeline'
     ELSE 'other' end as opp_pipeline_category,
CASE WHEN commit_status_c != 'Pipeline' then 1 else 0 end as opp_qualified_pipeline_flag,
CASE WHEN commit_status_c = 'Pipeline' then 1 else 0 end as opp_pipeline_flag,
fiscal as opp_fiscal,
loss_reason_c as opp_loss_reason,
to_timestamp(closed_won_date_c) as opp_closed_won_dte,
to_timestamp(closed_lost_date_c) as opp_closed_lost_dte,
to_timestamp(created_date) as opp_created_dte,
datediff(days,created_date,closed_won_date_c) as opp_sales_cycle_days,
to_timestamp(start_date_c) as opp_start_dte,
to_timestamp(close_date) as opp_close_dte,
to_timestamp(discovery_call_date_c) as opp_discovery_call_dte,
to_timestamp(vf_date_c) as opp_vf_dte,
to_timestamp(tdd_date_c) as opp_tdd_dte,
to_timestamp(eb_go_no_go_date_c) as opp_eb_go_no_go_dte,
to_timestamp(poc_date_c) as opp_poc_dte,
to_timestamp(eb_review_date_c) as opp_eb_review_dte,
to_timestamp(negotiate_and_close_c) as opp_neg_and_close_dte,
to_timestamp(vo_date_c) as opp_vo_dte,
to_timestamp(nbm_meeting_date_c) as opp_nbm_meeting_dte,
CASE WHEN (discovery_call_date_c IS NOT NULL 
           OR vf_date_c IS NOT NULL 
           OR tdd_date_c IS NOT NULL 
           OR eb_go_no_go_date_c IS NOT NULL 
           OR poc_date_c IS NOT NULL 
           OR eb_review_date_c IS NOT NULL
           OR negotiate_and_close_c IS NOT NULL
           OR closed_won_date_c IS NOT NULL) THEN 1 else 0 END as opp_had_discovery_call_flag,
CASE WHEN (vf_date_c IS NOT NULL 
           OR tdd_date_c IS NOT NULL 
           OR eb_go_no_go_date_c IS NOT NULL 
           OR poc_date_c IS NOT NULL 
           OR eb_review_date_c IS NOT NULL
           OR negotiate_and_close_c IS NOT NULL
           OR closed_won_date_c IS NOT NULL) THEN 1 else 0 END as opp_had_vf_flag,
CASE WHEN (tdd_date_c IS NOT NULL 
           OR eb_go_no_go_date_c IS NOT NULL 
           OR poc_date_c IS NOT NULL 
           OR eb_review_date_c IS NOT NULL
           OR negotiate_and_close_c IS NOT NULL
           OR closed_won_date_c IS NOT NULL) THEN 1 else 0 END as opp_had_tdd_flag,
CASE WHEN (eb_go_no_go_date_c IS NOT NULL 
           OR poc_date_c IS NOT NULL 
           OR eb_review_date_c IS NOT NULL
           OR negotiate_and_close_c IS NOT NULL
           OR closed_won_date_c IS NOT NULL) THEN 1 else 0 END as opp_had_eb_go_no_go_flag,
CASE WHEN (poc_date_c IS NOT NULL 
           OR eb_review_date_c IS NOT NULL
           OR negotiate_and_close_c IS NOT NULL
           OR closed_won_date_c IS NOT NULL) THEN 1 else 0 END as opp_had_poc_flag,
CASE WHEN (eb_review_date_c IS NOT NULL
           OR negotiate_and_close_c IS NOT NULL
           OR closed_won_date_c IS NOT NULL) THEN 1 else 0 END as opp_had_eb_review_flag,
CASE WHEN (negotiate_and_close_c IS NOT NULL
           OR closed_won_date_c IS NOT NULL) THEN 1 else 0 END as opp_had_neg_and_close_flag,
forecasted_arr_c as opp_arr,
net_new_arr_forecast_c as opp_net_new_arr,
is_deleted
from "FIVETRAN_DATABASE"."SALESFORCE"."OPPORTUNITY"
where is_deleted = 'FALSE'
order by id asc
),

acct as (
select 
id as account_id,
name as account_name,
industry_cleaned_c as account_industry,
sales_region_c as account_sales_region
from "FIVETRAN_DATABASE"."SALESFORCE"."ACCOUNT"
order by account_id
),

opp_owner as (
select 
u.id as user_id,
concat(u.first_name,' ',u.last_name) as opportunity_owner,
u.username,
ur.rollup_description as owner_description
from "FIVETRAN_DATABASE"."SALESFORCE"."USER" as u
left join "FIVETRAN_DATABASE"."SALESFORCE"."USER_ROLE" as ur on u.user_role_id = ur.id 
),

services_nrr as (
select distinct
opportunity_id,
revenue_type_c,
total_price as opp_services_nrr
from "FIVETRAN_DATABASE"."SALESFORCE"."OPPORTUNITY_LINE_ITEM"
where revenue_type_c = 'Services NRR'
order by opportunity_id
),

marketing_influenced as (
select distinct
bizible_2_opportunity_c as marketing_influenced_opportunity_id
from "FIVETRAN_DATABASE"."SALESFORCE"."BIZIBLE_2_BIZIBLE_ATTRIBUTION_TOUCHPOINT_C"
),

fy_dates as (
select 
dte,
fy_quarter,
fy_year,
fy_qtr_year,
qtr_end_dte
from "DEV"."MARTS"."FY_CALENDAR"
),

fct_opportunity as (
select distinct
opp_id,
opp_name,
opp.account_id,
acct.account_name,
acct.account_industry,
acct.account_sales_region,
opp.opp_partner_account,
acct2.account_name as partner_account_name,
opp.opp_stage_name,
opp.opp_stage_name_ordered,
opp.opp_discovery_flag,
opp.opp_vf_flag,
opp.opp_tdd_flag,
opp.opp_eb_go_no_go_flag,
opp.opp_tve_flag,
opp.opp_eb_revisit_flag,
opp.opp_neg_close_flag,
opp.opp_closed_won_flag,
opp.opp_dq_flag,
opp.opp_closed_lost_flag,
opp.opp_is_active,
opp.opp_is_active_flag,
opp.opp_revenue_type,
opp.opp_lead_source,
opp.opp_partner_influence,
opp.opp_is_partner_influenced_flag,
CASE WHEN mi.marketing_influenced_opportunity_id IS NOT NULL then 1 else 0 end as opp_is_marketing_influenced_flag, 
opp.opp_secondary_lead_source,
opp.opp_commit_status,
opp.opp_pipeline_category,
opp.opp_qualified_pipeline_flag,
opp.opp_pipeline_flag,
opp.opp_fiscal,
opp.opp_loss_reason,
opp.opp_closed_won_dte,
opp.opp_closed_lost_dte,
opp.opp_created_dte,
opp.opp_sales_cycle_days,
opp.opp_start_dte,
opp.opp_close_dte,
opp.opp_discovery_call_dte,
opp.opp_vf_dte,
opp.opp_tdd_dte,
opp.opp_eb_go_no_go_dte,
opp.opp_poc_dte,
opp.opp_eb_review_dte,
opp.opp_neg_and_close_dte,
opp.opp_vo_dte,
opp.opp_nbm_meeting_dte,
fy.fy_qtr_year as closed_won_fy_qtr,
fy.qtr_end_dte as closed_won_qtr_end_dte,
fy1.fy_qtr_year as close_fy_qtr,
fy1.qtr_end_dte as close_qtr_end_dte,
fy2.fy_qtr_year as start_fy_qtr,
fy2.qtr_end_dte as start_qtr_end_dte,
opp.opp_had_discovery_call_flag,
opp.opp_had_vf_flag,
opp.opp_had_tdd_flag,
opp.opp_had_eb_go_no_go_flag,
opp.opp_had_poc_flag,
opp.opp_had_eb_review_flag,
opp.opp_had_neg_and_close_flag,
opp.opp_arr,
opp.opp_net_new_arr,
sn.opp_services_nrr,
opp.is_deleted,
opp.opp_owner_id,
oo.opportunity_owner,
oo.username,
oo.owner_description
from opp
left join acct on (opp.account_id = acct.account_id)
left join acct as acct2 on (opp.opp_partner_account = acct2.account_id)
left join opp_owner as oo on (opp.opp_owner_id = oo.user_id)
left join services_nrr as sn on (opp.opp_id = sn.opportunity_id)
left join marketing_influenced as mi on (opp.opp_id = mi.marketing_influenced_opportunity_id)
left join fy_dates as fy on (to_date(opp.opp_closed_won_dte) = fy.dte)
left join fy_dates as fy1 on (to_date(opp.opp_close_dte) = fy1.dte)
left join fy_dates as fy2 on (to_date(opp.opp_start_dte) = fy2.dte)
),

combined_opp as (
select distinct
opp_id
from opp
where opp_loss_reason in ('Opportunity Combined with another Opportunity')
),

agg_opportunity as (
select *,
to_date(current_date()) as date_ran
from fct_opportunity
where opp_id not in (select * from combined_opp)
order by opp_id
)

select * from agg_opportunity

{% if is_incremental() %}

  where date_ran >= (select max(date_ran) from {{ this }})

{% endif %}