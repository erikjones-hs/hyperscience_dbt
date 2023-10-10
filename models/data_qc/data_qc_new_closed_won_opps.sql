{{ config
(
    materialized='table',
    database = 'PROD',
    schema = 'DATA_QC'
)
}}

with change_history as (
select distinct
updated_dte,
opp_id,
field,
old_value,
new_value 
from {{ref('closed_lost_won')}}
where new_value = 'Closed Won'    
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

opp_new_closed_won_int as (
select distinct
sao.account_id,
sao.account_name,
ch.opp_id,
sao.opp_name,
sao.opp_arr,
sao.opp_net_new_arr,
sao.opp_start_dte,
ch.field,
ch.old_value,
ch.new_value
from change_history as ch
left join opp as sao on (ch.opp_id = sao.opp_id) 
),

current_closed_won as (
select distinct 
opp_id
from {{ref('fct_arr_opp')}}     
),

opp_new_closed_won as (
select distinct 
account_id,
account_name,
opp_id,
opp_name,
opp_arr,
opp_net_new_arr,
opp_start_dte,
field,
old_value,
new_value
from opp_new_closed_won_int
where opp_id not in (select * from current_closed_won)    
)

select * from opp_new_closed_won