{{ config
(
    materialized='table',
    database = 'PROD',
    schema = 'MONTHLY_KPIS'
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
),

/* Bringing in incremental closed won ops since the 11/28/2021 static view */
/* date of this view is 12/7/2021 */
raw_data_inc as (
select 
account_id,
account_name,
opp_id,
opp_name,
opp_revenue_type,
to_date(opp_start_dte) as start_dte_raw,
to_date(opp_renewal_dte) as end_dte_raw,
opp_arr,
opp_net_new_arr
from "DEV"."ERIKJONES"."SALESFORCE_AGG_OPPORTUNITY_v1"
where (opp_stage_name = 'Closed Won')
and opp_id not in (select opp_id from raw_data_hist)
and opp_arr > 0
order by account_id, start_dte_raw asc
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
to_date(opp_start_dte) as start_dte_raw,
to_date(opp_renewal_dte) as end_dte_raw,
opp_arr,
opp_net_new_arr
from "DEV"."ERIKJONES"."SALESFORCE_AGG_OPPORTUNITY_v2"
where (opp_stage_name = 'Closed Won')
and opp_id not in (select opp_id from raw_data_hist)
and opp_id not in (select opp_id from raw_data_inc)
and opp_arr > 0
order by account_id, start_dte_raw asc
),

/* Bringing in 3rd round of incremental closed won ops since the 12/16/2021 static view */
/* date of this view is 12/17/2021 */
raw_data_inc_3 as (
select 
account_id,
account_name,
opp_id,
opp_name,
opp_revenue_type,
to_date(opp_start_dte) as start_dte_raw,
to_date(opp_renewal_dte) as end_dte_raw,
opp_arr,
opp_net_new_arr
from "DEV"."ERIKJONES"."SALESFORCE_AGG_OPPORTUNITY_v3"
where (opp_stage_name = 'Closed Won')
and opp_id not in (select opp_id from raw_data_hist)
and opp_id not in (select opp_id from raw_data_inc)
and opp_id not in (select opp_id from raw_data_inc_2)  
and opp_arr > 0
order by account_id, start_dte_raw asc
),

/* Bringing in 4th round of incremental closed won ops since the 12/17/2021 static view */
/* date of this view is 1/11/2021 */
raw_data_inc_4 as (
select 
account_id,
account_name,
opp_id,
opp_name,
opp_revenue_type,
to_date(opp_start_dte) as start_dte_raw,
to_date(opp_renewal_dte) as end_dte_raw,
opp_arr,
opp_net_new_arr
from "DEV"."ERIKJONES"."SALESFORCE_AGG_OPPORTUNITY_v4"
where (opp_stage_name = 'Closed Won')
and opp_id not in (select opp_id from raw_data_hist)
and opp_id not in (select opp_id from raw_data_inc)
and opp_id not in (select opp_id from raw_data_inc_2) 
and opp_id not in (select opp_id from raw_data_inc_3) 
and opp_arr > 0
order by account_id, start_dte_raw asc
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
to_date(opp_start_dte) as start_dte_raw,
to_date(opp_renewal_dte) as end_dte_raw,
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
),

/* Bringing in 6th round of incremental closed won ops since the 1/3/2022 static view */
/* date of this view is 1/31/2022 */
raw_data_inc_6 as (
select 
account_id,
account_name,
opp_id,
opp_name,
opp_revenue_type,
to_date(opp_start_dte) as start_dte_raw,
to_date(opp_renewal_dte) as end_dte_raw,
opp_arr,
opp_net_new_arr
from "DEV"."ERIKJONES"."SALESFORCE_AGG_OPPORTUNITY_v6"
where (opp_stage_name = 'Closed Won')
and opp_id not in (select opp_id from raw_data_hist)
and opp_id not in (select opp_id from raw_data_inc)
and opp_id not in (select opp_id from raw_data_inc_2) 
and opp_id not in (select opp_id from raw_data_inc_3)
and opp_id not in (select opp_id from raw_data_inc_4) 
and opp_id not in (select opp_id from raw_data_inc_5) 
and opp_arr > 0
order by account_id, start_dte_raw asc
),

/* Bringing in 7th round of incremental closed won ops since the 1/31/2022 static view */
/* date of this view is 2/3/2022 */
raw_data_inc_7 as (
select 
account_id,
account_name,
opp_id,
opp_name,
opp_revenue_type,
to_date(opp_start_dte) as start_dte_raw,
to_date(opp_renewal_dte) as end_dte_raw,
opp_arr,
opp_net_new_arr
from "DEV"."ERIKJONES"."SALESFORCE_AGG_OPPORTUNITY_v7"
where (opp_stage_name = 'Closed Won')
and opp_id not in (select opp_id from raw_data_hist)
and opp_id not in (select opp_id from raw_data_inc)
and opp_id not in (select opp_id from raw_data_inc_2) 
and opp_id not in (select opp_id from raw_data_inc_3)
and opp_id not in (select opp_id from raw_data_inc_4) 
and opp_id not in (select opp_id from raw_data_inc_5) 
and opp_id not in (select opp_id from raw_data_inc_6) 
and opp_arr > 0
order by account_id, start_dte_raw asc
),

/* Bringing in 8th round of incremental closed won ops since the 2/3/2022 static view */
/* date of this view is 2/9/2022 */
raw_data_inc_8 as (
select distinct 
account_id,
account_name,
opp_id,
opp_name,
opp_revenue_type,
to_date(opp_start_dte) as start_dte_raw,
to_date(opp_renewal_dte) as end_dte_raw,
opp_arr,
opp_net_new_arr
from "DEV"."ERIKJONES"."SALESFORCE_AGG_OPPORTUNITY_v8"
where (opp_stage_name = 'Closed Won')
and opp_id not in (select opp_id from raw_data_hist)
and opp_id not in (select opp_id from raw_data_inc)
and opp_id not in (select opp_id from raw_data_inc_2) 
and opp_id not in (select opp_id from raw_data_inc_3)
and opp_id not in (select opp_id from raw_data_inc_4) 
and opp_id not in (select opp_id from raw_data_inc_5) 
and opp_id not in (select opp_id from raw_data_inc_6)
and opp_id not in (select opp_id from raw_data_inc_7) 
and opp_arr > 0
order by account_id, start_dte_raw asc
),

raw_data_inc_9 as (
select distinct 
account_id,
account_name,
opp_id,
opp_name,
opp_revenue_type,
to_date(opp_start_dte) as start_dte_raw,
to_date(opp_renewal_dte) as end_dte_raw,
opp_arr,
opp_net_new_arr
from "DEV"."ERIKJONES"."SALESFORCE_AGG_OPPORTUNITY_v9"
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
and opp_arr > 0
order by account_id, start_dte_raw asc
),

raw_data_inc_10 as (
select distinct 
account_id,
account_name,
opp_id,
opp_name,
opp_revenue_type,
to_date(opp_start_dte) as start_dte_raw,
to_date(opp_renewal_dte) as end_dte_raw,
opp_arr,
opp_net_new_arr
from "DEV"."ERIKJONES"."SALESFORCE_AGG_OPPORTUNITY_v10"
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
and opp_arr > 0
order by account_id, start_dte_raw asc
),

raw_data_inc_11 as (
select distinct 
account_id,
account_name,
opp_id,
opp_name,
opp_revenue_type,
to_date(opp_start_dte) as start_dte_raw,
to_date(opp_renewal_dte) as end_dte_raw,
opp_arr,
opp_net_new_arr
from "DEV"."ERIKJONES"."SALESFORCE_AGG_OPPORTUNITY_v11"
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
and opp_arr > 0
order by account_id, start_dte_raw asc
),

raw_data_inc_12 as (
select distinct 
account_id,
account_name,
opp_id,
opp_name,
opp_revenue_type,
to_date(opp_start_dte) as start_dte_raw,
to_date(opp_renewal_dte) as end_dte_raw,
opp_arr,
opp_net_new_arr
from "DEV"."ERIKJONES"."SALESFORCE_AGG_OPPORTUNITY_v12"
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
and opp_arr > 0
order by account_id, start_dte_raw asc
),

raw_data_inc_13 as (
select distinct 
account_id,
account_name,
opp_id,
opp_name,
opp_revenue_type,
to_date(opp_start_dte) as start_dte_raw,
to_date(opp_renewal_dte) as end_dte_raw,
opp_arr,
opp_net_new_arr
from "DEV"."ERIKJONES"."SALESFORCE_AGG_OPPORTUNITY_v13"
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
and opp_arr > 0
order by account_id, start_dte_raw asc
),

raw_data_inc_14 as (
select distinct 
account_id,
account_name,
opp_id,
opp_name,
opp_revenue_type,
to_date(opp_start_dte) as start_dte_raw,
to_date(opp_renewal_dte) as end_dte_raw,
opp_arr,
opp_net_new_arr
from "DEV"."ERIKJONES"."SALESFORCE_AGG_OPPORTUNITY_v14"
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
and opp_arr > 0
order by account_id, start_dte_raw asc
),

/* Merging historical data set with incrementa closed won opps */
/* This is now the full closed won opportunity data set that will be transformed */ 
raw_data as (
select * from raw_data_hist
UNION 
select * from raw_data_inc
UNION 
select * from raw_data_inc_2 
UNION 
select * from raw_data_inc_3 
UNION 
select * from raw_data_inc_4
UNION 
select * from raw_data_inc_5
UNION 
select * from raw_data_inc_6
UNION 
select * from raw_data_inc_7
UNION 
select * from raw_data_inc_8
UNION 
select * from raw_data_inc_9 
UNION 
select * from raw_data_inc_10
UNION 
select * from raw_data_inc_11  
UNION 
select * from raw_data_inc_12
UNION 
select * from raw_data_inc_13
UNION 
select * from raw_data_inc_14
order by account_id, start_dte_raw asc
)

select * from raw_data