{{ config
(
    materialized='table',
    database = 'PROD',
    schema = 'DATA_QC'
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
where field in ('Actual_Go_Live_Date__c')
and to_date(created_date) >= dateadd(day,-30,(to_date(current_date))) 
),

opp as (
select distinct
opp_id,
opp_name,
account_id,
account_name,
opp_arr,
opp_net_new_arr,
opp_start_dte
from "DEV"."SALES"."SALESFORCE_AGG_OPPORTUNITY"
),

go_live_changes as (
select distinct
sao.account_id,
sao.account_name,
ch.opp_id,
sao.opp_name,
sao.opp_arr,
ch.updated_dte,
ch.field,
ch.old_value,
ch.new_value
from change_history as ch
left join opp as sao on (ch.opp_id = sao.opp_id) 
order by updated_dte desc
)

select * from go_live_changes