{{ config
(
    materialized='table',
    database = 'PROD',
    schema = 'CLOSED_LOST'
)
}}

with opps as (
select distinct
opp_id,
opp_name,
opp_stage_name,
account_id,
account_name,
opp_created_dte,
account_industry,
opp_partner_account,
partner_account_name,
opp_revenue_type,
opp_lead_source,
opp_loss_reason,
opp_arr,
opp_net_new_arr,
opp_is_partner_influenced_flag,
opp_is_marketing_influenced_flag
from {{ref('agg_opportunity_incremental')}}
where to_date(date_ran) = dateadd(day,-1,(to_date(current_date)))
and opp_stage_name = 'Closed Lost'
),

sfdc_closed_lost_dates as (
select distinct
id,
name,
closed_lost_date_c as closed_lost_dte
from "FIVETRAN_DATABASE"."SALESFORCE"."OPPORTUNITY"
where _fivetran_active = 'TRUE'
),

opp_meta_data as (
select distinct
o.opp_id,
o.opp_name,
o.opp_stage_name,
o.account_id,
o.account_name,
o.opp_created_dte,
o.account_industry,
o.opp_partner_account,
o.partner_account_name,
o.opp_revenue_type,
o.opp_lead_source,
o.opp_loss_reason,
o.opp_arr,
o.opp_net_new_arr,
o.opp_is_partner_influenced_flag,
o.opp_is_marketing_influenced_flag,
scld.closed_lost_dte
from opps as o
left join sfdc_closed_lost_dates as scld on (o.opp_id = scld.id)
),

gong_calls as (
select * from {{ref('calls_with_sfdc_metadata')}}
),

fct_closed_lost as (
select distinct
omd.opp_id,
omd.opp_name,
omd.opp_stage_name,
omd.account_id,
omd.account_name,
omd.opp_created_dte,
omd.account_industry,
omd.opp_partner_account,
omd.partner_account_name,
omd.opp_revenue_type,
omd.opp_lead_source,
omd.opp_loss_reason,
omd.opp_arr,
omd.opp_net_new_arr,
omd.opp_is_partner_influenced_flag,
omd.opp_is_marketing_influenced_flag,
to_timestamp(omd.closed_lost_dte) as closed_lost_dte,
CASE WHEN omd.opp_id in (select distinct opp_id from gong_calls) then 1 else 0 end as had_gong_call_flag
from opp_meta_data as omd
order by closed_lost_dte asc
)

select * from fct_closed_lost