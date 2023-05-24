{{ config
(
    materialized='table',
    database = 'PROD',
    schema = 'SCHEDULES_SUMMARY'
)
}}

with change_history as (
select distinct
to_timestamp(created_date) as updated_dte,
opportunity_id as opp_id,
field,
old_value,
new_value
from "FIVETRAN_DATABASE"."SALESFORCE"."OPPORTUNITY_FIELD_HISTORY"
where field in ('Forecasted_ARR__c','CloseDate','Commit_Status__c')
),

fy_dates as (
select distinct 
dte,
CASE WHEN month in (1,2) then dateadd('year',-1,date_trunc(year,dte)) ELSE date_trunc(year,dte) end as fy_year,
fy_qtr_year,
qtr_end_dte
from "DEV"."MARTS"."FY_CALENDAR"
),

opps as (
select distinct
opp_id,
opp_name,
account_id,
account_name,
opp_arr
from {{ref('agg_opportunity_incremental')}}
where to_date(date_ran) = dateadd(day,-1,to_date(current_date())) 
),

current_dte as (
select
to_date(current_date()) as current_dte,
fd.qtr_end_dte as current_qtr_end_dte
from fy_dates as fd where to_date(current_date) = to_date(fd.dte)
),

fct_changes as (
select distinct
ch.updated_dte,
fd.qtr_end_dte as updated_qtr_end_dte,
o.account_id,
o.account_name,
ch.opp_id,
o.opp_name,
o.opp_arr,
CASE WHEN ch.field = 'Forecasted_ARR__c' then 'ARR'
     WHEN ch.field = 'CloseDate' then 'Close Date'
     WHEN ch.field = 'Commit_Status__c' then 'Commit Status'
     end as sfdc_field,
CASE WHEN sfdc_field = 'Close Date' then to_timestamp(to_date(old_value)) else NULL end as old_close_dte,
CASE WHEN sfdc_field = 'Close Date' then to_timestamp(to_date(new_value)) else NULL end as new_close_dte,
ch.old_value,
ch.new_value,
CASE WHEN to_date(ch.updated_dte) <= (select current_qtr_end_dte from current_dte) 
     AND to_date(ch.updated_dte) >= dateadd(month,-2,date_trunc(month,(select current_qtr_end_dte from current_dte))) 
     THEN 1 else 0 end as current_qtr_change_fl, 
CASE WHEN sfdc_field = 'Close Date' 
     AND to_date(old_close_dte) <= (select current_qtr_end_dte from current_dte) 
     AND to_date(old_close_dte) >= dateadd(month,-2,date_trunc(month,(select current_qtr_end_dte from current_dte)))
     AND to_date(new_close_dte) > (select current_qtr_end_dte from current_dte) 
     THEN 1 else 0 end as close_date_push_new_qtr_fl,
CASE WHEN sfdc_field = 'Close Date' 
     AND to_date(old_close_dte) > (select current_qtr_end_dte from current_dte)  
     AND to_date(new_close_dte) >= dateadd(month,-2,date_trunc(month,(select current_qtr_end_dte from current_dte)))
     AND to_date(new_close_dte) <= (select current_qtr_end_dte from current_dte) 
     THEN 1 else 0 end as close_date_brought_into_current_qtr_fl
from change_history as ch 
left join fy_dates as fd on (to_date(ch.updated_dte) = to_date(fd.dte))
left join opps as o on (ch.opp_id = o.opp_id)
order by ch.updated_dte asc
)

select * from fct_changes

