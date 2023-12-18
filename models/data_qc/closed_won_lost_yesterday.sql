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
where field in ('StageName')
and new_value in ('Closed Lost','Closed Won')
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

opp_new_closed_won_lost as (
select distinct
ch.updated_dte,
sao.account_id,
sao.account_name,
ch.opp_id,
sao.opp_name,
sao.opp_arr,
sao.opp_net_new_arr,
sao.opp_start_dte,
ch.old_value,
ch.new_value
from change_history as ch
left join opp as sao on (ch.opp_id = sao.opp_id) 
)

select * from opp_new_closed_won_lost