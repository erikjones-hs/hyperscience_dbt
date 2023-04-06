{{ config
(
    materialized='table',
    database = 'PROD',
    schema = 'GONG'
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
opp_closed_won_dte,
opp_close_dte
from {{ref('agg_opportunity_incremental')}}
where to_date(date_ran) = dateadd(day,-1,(to_date(current_date)))  
),

sfdc_closed_lost_dates as (
select distinct
id,
name,
closed_lost_date_c as closed_lost_dte
from "FIVETRAN_DATABASE"."SALESFORCE"."OPPORTUNITY"
where _fivetran_active = 'TRUE'
),

opp_meta_data_int as (
select distinct
o.opp_id,
o.opp_name,
o.opp_stage_name,
o.account_id,
o.account_name,
o.opp_created_dte,
CASE WHEN o.opp_stage_name = 'Closed Won' and o.opp_closed_won_dte IS NULL then o.opp_close_dte else o.opp_closed_won_dte end as closed_won_dte,
scld.closed_lost_dte
from opps as o
left join sfdc_closed_lost_dates as scld on (o.opp_id = scld.id)
),

opp_meta_data as (
select distinct
account_id,
account_name,
opp_id,
opp_name,
opp_stage_name,
opp_created_dte,
CASE WHEN opp_stage_name = 'Closed Won' then closed_won_dte
     WHEN opp_stage_name = 'Closed Lost' then closed_lost_dte
     ELSE NULL end as opp_closed_dte
from opp_meta_data_int
),

gong_data as (
select cat.*,
cc.object_id,
cc.object_type
from {{ref('calls_and_trackers')}} as cat
left join "GONG"."HYPERSCIENCE_GONG"."CONVERSATION_CONTEXTS" as cc on (cat.conversation_key = cc.conversation_key) 
where cc.object_type = 'opportunity'
),

fct_gong_sfdc as (
select distinct
gd.call_id,
gd.conversation_key,
gd.owner_id,
gd.title,
to_timestamp(gd.call_date) as call_date,
gd.call_start,
gd.call_end,
gd.call_duration,
gd.user_id,
gd.email_address,
gd.first_name,
gd.last_name,
gd.tracker_id,
gd.tracker_type,
gd.keywords,
gd.tracker_count,
gd.tracker_category,
gd.tracker_specific,
gd.object_id,
gd.object_type,
omd.account_id,
omd.account_name,
omd.opp_id,
omd.opp_name,
omd.opp_stage_name,
omd.opp_created_dte,
omd.opp_closed_dte
from gong_data as gd
left join opp_meta_data as omd on (gd.object_id = omd.opp_id)
order by account_id, call_date asc
)

select * from fct_gong_sfdc 