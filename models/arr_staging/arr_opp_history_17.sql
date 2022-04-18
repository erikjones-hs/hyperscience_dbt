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

raw_data_inc_5 as (
select * from {{ref('arr_opp_history_6')}}    
),

raw_data_inc_6 as (
select * from {{ref('arr_opp_history_7')}}    
),

raw_data_inc_7 as (
select * from {{ref('arr_opp_history_8')}}    
),

raw_data_inc_8 as (
select * from {{ref('arr_opp_history_9')}}    
),

raw_data_inc_9 as (
select * from {{ref('arr_opp_history_10')}}    
),

raw_data_inc_10 as (
select * from {{ref('arr_opp_history_11')}}    
),

raw_data_inc_11 as (
select * from {{ref('arr_opp_history_12')}}    
),

raw_data_inc_12 as (
select * from {{ref('arr_opp_history_13')}}    
),

raw_data_inc_13 as (
select * from {{ref('arr_opp_history_14')}}    
),

raw_data_inc_14 as (
select * from {{ref('arr_opp_history_15')}}    
),

raw_data_inc_15 as (
select * from {{ref('arr_opp_history_16')}}    
),

raw_data_inc_16 as (
select distinct 
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
from "DEV"."ERIKJONES"."SALESFORCE_AGG_OPPORTUNITY_v16"
where (opp_stage_name = 'Closed Won')
and opp_id not in (select opp_id from raw_data_hist)
and opp_id not in (select opp_id from raw_data_inc)
and opp_id not in (select opp_id from raw_data_inc_2) 
and opp_id not in (select opp_id from raw_data_inc_3)
and opp_id not in (select opp_id from raw_data_inc_4) 
and opp_id not in (select opp_id from raw_data_inc_5) 
and opp_id not in (select opp_id from raw_data_inc_6)
and opp_id not in (select opp_id from raw_data_inc_7) 
and opp_id not in (select opp_id from raw_data_inc_8)
and opp_id not in (select opp_id from raw_data_inc_9)  
and opp_id not in (select opp_id from raw_data_inc_10)
and opp_id not in (select opp_id from raw_data_inc_11)  
and opp_id not in (select opp_id from raw_data_inc_12)
and opp_id not in (select opp_id from raw_data_inc_13)
and opp_id not in (select opp_id from raw_data_inc_14)
and opp_id not in (select opp_id from raw_data_inc_15)
and opp_arr > 0
order by account_id, start_dte_raw asc
)

select * from raw_data_inc_16