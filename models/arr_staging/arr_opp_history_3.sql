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

/* Bringing in 2nd round of incremental closed won ops since the 12/7/2021 static view */
/* date of this view is 12/16/2021 */
raw_data_inc_2 as (
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
from "DEV"."ERIKJONES"."SALESFORCE_AGG_OPPORTUNITY_v2"
where (opp_stage_name = 'Closed Won')
and opp_id not in (select opp_id from raw_data_hist)
and opp_id not in (select opp_id from raw_data_inc)
and opp_arr > 0
order by account_id, start_dte_raw asc
)

select * from raw_data_inc_2