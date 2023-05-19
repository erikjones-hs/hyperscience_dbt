{{ config
(
    materialized='table',
    database = 'PROD',
    schema = 'MONTHLY_KPIS'
)
}}

with missing_closed_won_dates as (
select distinct
id
from "FIVETRAN_DATABASE"."SALESFORCE"."OPPORTUNITY"
where id in (
'0061R000016jr2lQAA', /*MARS upsell */
'0061R00001BAPkAQAX', /* IRS Phase 3 */
'0061R00001A5k8bQAB', /* SSA Expansion */
'0061R00001A4pwsQAB', /*Ascensus Renewal */
'0061R000014wcRjQAI', /*Plus Platform */
'0061R00000oE2hbQAC' /*Raymond James */
)
),

opps as (
select distinct
id,
account_id,
name,
stage_name,
active_opportunity_c,
revenue_type_c,
commit_status_c,
CASE WHEN closed_won_date_c IS NOT NULL AND stage_name != 'Closed Won' then NULL 
     WHEN id in (select id from missing_closed_won_dates) then to_timestamp(start_date_c)
     else to_timestamp(closed_won_date_c) end as opp_closed_won_dte,
CASE WHEN closed_lost_date_c IS NOT NULL AND stage_name != 'Closed Lost' then NULL else to_timestamp(closed_lost_date_c) end as opp_closed_lost_dte,
to_timestamp(created_date) as opp_created_dte,
to_timestamp(close_date) as opp_close_dte,
forecasted_arr_c as opp_arr,
net_new_arr_forecast_c as opp_net_new_arr
from "FIVETRAN_DATABASE"."SALESFORCE"."OPPORTUNITY"
where is_deleted = 'FALSE'
and _fivetran_active = 'TRUE'
and forecasted_arr_c > 0 
),

opps_to_delete as (
select distinct
id
from "FIVETRAN_DATABASE"."SALESFORCE"."OPPORTUNITY"
where closed_won_date_c IS NULL and stage_name = 'Closed Won'
and id not in (
'0061R000016jr2lQAA', /*MARS upsell */
'0061R00001BAPkAQAX', /* IRS Phase 3 */
'0061R00001A5k8bQAB', /* SSA Expansion */
'0061R00001A4pwsQAB', /*Ascensus Renewal */
'0061R000014wcRjQAI', /*Plus Platform */
'0061R00000oE2hbQAC' /*Raymond James */
)
),

fct_opps as (
select distinct
id as opp_id,
account_id,
name as opp_name,
stage_name as opp_stage_name,
active_opportunity_c as opp_is_active_fl,
revenue_type_c as opp_revenue_type,
commit_status_c as opp_commit_status,
opp_closed_won_dte,
opp_closed_lost_dte,
opp_created_dte,
opp_close_dte,
opp_arr,
opp_net_new_arr,
CASE WHEN opp_closed_won_dte IS NOT NULL then opp_closed_won_dte
     WHEN opp_closed_lost_dte IS NOT NULL then opp_closed_lost_dte
     ELSE dateadd(days,30,to_date(current_date()))
     END as opp_closed_dte
from opps
where opp_id not in (select * from opps_to_delete)
),

fy_dates as (
select distinct 
dte,
CASE WHEN month in (1,2) then dateadd('year',-1,date_trunc(year,dte)) ELSE date_trunc(year,dte) end as fy_year,
fy_qtr_year,
qtr_end_dte
from "DEV"."MARTS"."FY_CALENDAR"
),

opp_dates as (
select distinct 
fd.dte,
fd.qtr_end_dte,
fo.account_id,
fo.opp_id,
fo.opp_name,
fo.opp_stage_name,
fo.opp_is_active_fl,
fo.opp_revenue_type,
fo.opp_commit_status,
fo.opp_closed_won_dte,
fo.opp_closed_lost_dte,
fo.opp_created_dte,
fo.opp_closed_dte,
fo.opp_close_dte,
fo.opp_arr,
fo.opp_net_new_arr,
CASE WHEN to_date(fd.dte) < to_date(fo.opp_closed_dte) then 1 else 0 end as open_pipeline_fl,
CASE WHEN to_date(fd.dte) < to_date(fo.opp_closed_dte) then 'open' else 'closed' end as status_category,
CASE WHEN to_date(fd.dte) = to_date(fo.opp_created_dte) then 1 else 0 end as created_dte_fl,
CASE WHEN to_date(fd.dte) = to_date(fo.opp_closed_dte) then 1 else 0 end as closed_dte_fl,
CASE WHEN to_date(fd.dte) = to_date(fo.opp_closed_won_dte) then 1 else 0 end as closed_won_dte_fl,
CASE WHEN to_date(fd.dte) = to_date(fo.opp_closed_lost_dte) then 1 else 0 end as closed_lost_dte_fl
from fct_opps as fo
inner join fy_dates as fd on (fd.dte >= to_date(fo.opp_created_dte) AND fd.dte <= to_date(fo.opp_closed_dte))
order by opp_id, dte asc
)

select * from opp_dates