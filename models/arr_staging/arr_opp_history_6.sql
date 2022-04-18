{{ config
(
    materialized='table',
    database = 'PROD',
    schema = 'ARR_STAGING'
)
}}

with raw_data_hist as (
select * from {{ref('arr_opp_history_1')}}
),

raw_data_inc as (
select * from {{ref('arr_opp_history_2')}}    
),

raw_data_inc_2 as (
select * from {{ref('arr_opp_history_3')}}    
),

raw_data_inc_3 as (
select * from {{ref('arr_opp_history_4')}}    
),

raw_data_inc_4 as (
select * from {{ref('arr_opp_history_5')}}    
),

/* Bringing in 5th round of incremental closed won ops since the 12/17/2021 static view */
/* date of this view is 1/3/2022 */
raw_data_inc_5 as (
select 
account_id,
account_name,
opp_id,
opp_name,
opp_revenue_type,
opp_is_marketing_influenced_flag,
to_date(opp_start_dte) as start_dte_raw,
to_date(opp_renewal_dte) as end_dte_raw,
to_date(opp_closed_won_dte) as closed_won_dte,
opp_arr,
opp_net_new_arr
from "DEV"."ERIKJONES"."SALESFORCE_AGG_OPPORTUNITY_v5"
where (opp_stage_name = 'Closed Won')
and opp_id not in (select opp_id from raw_data_hist)
and opp_id not in (select opp_id from raw_data_inc)
and opp_id not in (select opp_id from raw_data_inc_2) 
and opp_id not in (select opp_id from raw_data_inc_3)
and opp_id not in (select opp_id from raw_data_inc_4)  
and opp_arr > 0
order by account_id, start_dte_raw asc
)

SELECT * FROM raw_data_inc_5