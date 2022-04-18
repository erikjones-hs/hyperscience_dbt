{{ config
(
    materialized='table',
    database = 'PROD',
    schema = 'ARR_STAGING'
)
}}

/* pulling in raw SFDC data from our incremental model view */
/* This has correct history matching Finance ARR Google Sheet */
/* date of this static view = 11/28/2021 */
with raw_data_hist as (
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
CASE WHEN opp_id = '0061R000010O65hQAC' then 1000000 /* Adjusting First American ARR per FP&A */
     else opp_arr end as opp_arr,
CASE WHEN opp_id = '0061R000013f0rkQAA' then 154286  /* Adjusting State of CO per FP&A */
     WHEN opp_id = '0061R000010O65hQAC' then 1000000 /* Adjusting First American Net New ARR per FP&A */
     else opp_net_new_arr end as opp_net_new_arr
from "DEV"."ERIKJONES"."SALESFORCE_AGG_OPPORTUNITY"
where (opp_stage_name = 'Closed Won' or opp_id = '0061R000013f0rkQAA') /* adding in this opp_id per Finance */
and opp_arr > 0
order by account_id, start_dte_raw asc
)

select * from raw_data_hist