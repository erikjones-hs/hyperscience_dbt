{{ config
(
    materialized='table',
    database = 'PROD',
    schema = 'MONTHLY_KPIS'
)
}}

with lead_source as (
select distinct 
opp_id,
opp_lead_source
from "DBT"."DBT_EJONES"."AGG_OPPORTUNITY_INCREMENTAL"
where to_date(date_ran) = dateadd(day,-1,(to_date(current_date)))
),

/* pulling in raw SFDC data from our incremental model view */
/* This has correct history matching Finance ARR Google Sheet */
/* date of this static view = 11/28/2021 */
raw_data_hist as (
select 
account_id,
account_name,
opp_id,
opp_name,
opp_revenue_type,
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
to_date(opp_closed_won_dte) as closed_won_dte,
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
to_date(opp_closed_won_dte) as closed_won_dte,
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
to_date(opp_closed_won_dte) as closed_won_dte,
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
to_date(opp_closed_won_dte) as closed_won_dte,
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
to_date(opp_closed_won_dte) as closed_won_dte,
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
to_date(opp_closed_won_dte) as closed_won_dte,
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
to_date(opp_closed_won_dte) as closed_won_dte,
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
to_date(opp_closed_won_dte) as closed_won_dte,
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
to_date(opp_closed_won_dte) as closed_won_dte,
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
to_date(opp_closed_won_dte) as closed_won_dte,
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
to_date(opp_closed_won_dte) as closed_won_dte,
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
to_date(opp_closed_won_dte) as closed_won_dte,
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
to_date(opp_closed_won_dte) as closed_won_dte,
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
),

/* transforming start dates to start months */
/* adjusting start and end dates to match historical finance data from ARR Google Sheet */
/* adjusting arr to match historical finance data from ARR Google Sheet */
raw_data_transformed as (
select 
CASE WHEN account_id = '0011R000026iP6rQAE' then '0013600001iRke2AAC' else account_id end as account_id,
CASE WHEN account_name = 'TD Ameritrade' then 'Charles Schwab' else account_name end as account_name,
opp_id,
opp_name,
opp_revenue_type,
CASE WHEN opp_id = '0063600000X36zWAAR' then to_date('2020-07-01')
     WHEN opp_id = '0061R00000m11f1QAA' then to_date('2020-07-01')
     WHEN opp_id = '0061R00000tE0SIQA0' then to_date('2021-06-01')
     WHEN opp_id = '0063600000eMMZeAAO' then to_date('2019-12-01')
     WHEN opp_id = '0061R00000pmValQAE' then to_date('2020-06-17')
     WHEN opp_id = '0063600000FJwn2AAD' then to_date('2020-07-31')
     WHEN opp_id = '0063600000iS5wbAAC' then to_date('2020-01-01')
     WHEN opp_id = '0061R00000r6r1iQAA' then to_date('2023-11-15') /* Adjusting the end date for CRL because they are still a customer, but with no committed revenue */
     WHEN OPP_ID = '0061R00000r83epQAA' THEN TO_DATE('2020-09-15')
     WHEN opp_id = '0061R00000wKxG5QAK' THEN to_date('2020-08-15')
     WHEN opp_id = '0061R00000uJr07QAC' THEN to_date('2021-08-15')
     WHEN opp_id = '0061R000010tCbNQAU' THEN to_date('2020-10-15')
     WHEN opp_id = '0061R000010tCZvQAM' THEN to_date('2021-10-15')
     WHEN opp_id = '0061R00000oERITQA4' THEN to_date('2022-10-15')
     WHEN opp_id = '0063600000M73LuAAJ' then to_date('2020-02-15')
     WHEN opp_id = '0063600000dsPyXAAU' then to_date('2019-10-15')
     WHEN opp_id = '0061R00000kRNPDQA4' then to_date('2020-10-15')
     WHEN opp_id = '0063600000dsPsjAAE' then to_date('2020-08-15')
     WHEN opp_id = '0061R00000zAM8wQAG' then to_date('2021-08-15')
     WHEN opp_id = '0061R00000tFLB3QAO' then to_date('2021-11-15')
     WHEN opp_id = '0061R0000137jsqQAA' then to_date('2022-08-30')
     WHEN opp_id = '0061R0000137jqkQAA' then to_date('2022-08-19')
     WHEN opp_id = '0061R0000137ijiQAA' then to_date('2022-06-09')
     WHEN opp_id = '0061R00000uL8ylQAC' then to_date('2023-11-15') /* Adjusting the end date for PMP because they are still a customer, but with a 1 year free contract period */
     WHEN opp_id = '0061R00000zAjoeQAC' then to_date('2021-10-15')
     WHEN opp_id = '0061R0000137hOKQAY' then to_date('2022-08-20')
     WHEN opp_id = '0061R000010PVABQA4' then to_date('2021-10-15')
     WHEN opp_id = '0061R00000zBqNRQA0' then to_date('2021-11-15')
     WHEN opp_id = '0061R000013fGLrQAM' then to_date('2022-12-18')
     WHEN opp_id = '0061R000013fGTbQAM' then to_date('2022-11-23') /* Updated end date because it is incorect in SFDC. divvyDose 180k */
     WHEN opp_id = '0061R00000zD2sTQAS' then to_date('2021-12-15') /* End date adjusted per Kristen and Finance ARR Google Sheet. Conduent 280k */ 
     WHEN opp_id = '0061R000010t71kQAA' then to_date('2022-01-15') /* Customer no longer is paying. Close this out in Jan. per FP&A. Sience SAS 41.65k */
     when opp_id = '0061R000014uXZrQAM' then to_date('2023-01-25') /* Updated MPOWER end date because it is incorrect in SFDC */
     when opp_id = '0061R00000yElHXQA0' then to_date('2022-02-15') /* Customer Churned in Feb, per FP&A. Department of Treasury 87.5k */
     when opp_id = '0061R000010O65hQAC' then to_date('2022-08-15') /* End date adjustment because of open negotiations. First American Financial 1M */
     when opp_id = '0061R00000tG3b2QAC' then to_date('2022-05-15') /* End date adjustment because of open negotiations. CI Financial 150k */
     when opp_id = '0061R00000zAlU8QAK' then to_date('2022-03-15') /* Opportunity churned in March, per Kristen. AMEX 323k */
     when opp_id = '0061R0000136hnzQAA' then to_date('2022-02-15') /* Customer churned in Feb. per Kristen. AXA Churn. 35k */ 
     when opp_id = '0061R000014vAD7QAM' then to_date('2023-02-15') /* Adjusting end date because it is incorrect in SFDC */
     when opp_id = '0061R00000r7xPhQAI' then to_date('2022-02-15') /* Customer churned. Close this out in Feb. per FP&A. DISA 64.3k */ 
     when opp_Id = '0061R0000137tYlQAI' then to_date('2022-03-15') /* Customer churned. Close this out in Mar. per FP&A. Record Connect 239k */
     when opp_id = '0061R0000137kNxQAI' then to_date('2022-04-15') /* Customer Churned in April per FP&A. State of Texas 402.5k total. 17.5k opp */
     when opp_id = '0061R00000zD2sxQAC' then to_date('2022-05-15') /* End date adjustment because of open negotiations. Conduent 1.98M */
     ELSE end_dte_raw end as end_dte,
CASE WHEN opp_id = '0061R00000uINyXQAW' then to_date('2020-08-01')
     WHEN opp_id = '0061R00000uIehuQAC' then to_date('2020-01-01')
     WHEN opp_id = '0061R00000zD2sxQAC' then to_date('2020-12-01')
     WHEN opp_id = '0063600000FJwn2AAD' then to_date('2019-07-01')
     WHEN opp_id = '0061R00000pmValQAE' then to_date('2019-06-15')
     WHEN opp_id = '0061R00000tF1MSQA0' then to_date('2020-10-15')
     WHEN opp_id = '0061R00000zAM8wQAG' then to_date('2020-09-15')
     WHEN opp_id = '0061R00000zDAxAQAW' then to_date('2020-02-01')
     WHEN opp_id = '0063600000X36zWAAR' then to_date('2018-04-01')
     WHEN opp_id = '0063600000kQAyCAAW' then to_date('2018-12-15')
     WHEN opp_id = '0061R00000pkoODQAY' then to_date('2019-08-15')
     WHEN opp_id = '0063600000eMMZeAAO' then to_date('2018-12-15')
     WHEN opp_id = '0063600000dsPsjAAE' then to_date('2018-06-15')
     WHEN opp_id = '0063600000M73LuAAJ' then to_date('2019-02-15')
     WHEN opp_id = '0063600000dsPyXAAU' then to_date('2018-10-15')
     WHEN opp_id = '0061R00000kRNPDQA4' then to_date('2019-10-15')
     WHEN opp_id = '0061R0000137ijiQAA' then to_date('2020-06-30')
     WHEN opp_id = '0061R000013flkIQAQ' then to_date('2021-10-15')
     WHEN opp_id = '0061R000014xeQwQAI' then to_date('2022-01-15')
     WHEN opp_id = '0061R0000135gO1QAI' then to_date('2021-12-15')
     WHEN opp_id = '0061R0000137hXuQAI' then to_date('2022-02-15')
     WHEN opp_id = '0061R0000136ZbBQAU' then to_date('2022-03-15')
     ELSE start_dte_raw end as start_dte,
closed_won_dte,
date_trunc('month',to_date(start_dte)) as start_dte_month,
date_trunc('month',to_date(end_dte)) as end_dte_month,
date_trunc('month',to_date(closed_won_dte)) as closed_won_dte_month,
CASE WHEN opp_id = '0063600000M73LuAAJ' then 200000
     WHEN opp_id = '0063600000dsPyXAAU' then 150000
     WHEN opp_id = '0061R00000kRNPDQA4' then 400000
     WHEN opp_id = '0063600000X3OBrAAN' then 480000
     WHEN opp_id = '0061R0000137ijiQAA' then 25000
     WHEN opp_id = '0063600000dsPsjAAE' then 600000
     WHEN opp_id = '0061R00000uJr07QAC' then 100000
     WHEN opp_id = '0061R0000135gO1QAI' then 89040
     when opp_id = '0061R000014xeQwQAI' then 13269
     ELSE opp_arr end as opp_arr,
CASE WHEN opp_id = '0061R0000135gO1QAI' then 5040 
     WHEN opp_id = '0061R000014xeQwQAI' then 13269
     ELSE opp_net_new_arr end as opp_net_new_arr
from raw_data
where opp_id not in ('00636000003gG2qAAE','0063600000W0NhNAAV','0063600000SKDdAAAX','0061R00000m1g4KQAQ','0063600000X36vUAAR') /*removing these ops per FP&A */
),

fct_sourced_int as (
select distinct
rdt.account_id,
rdt.account_name,
rdt.opp_id,
rdt.opp_name,
rdt.opp_revenue_type,
to_timestamp(rdt.end_dte) as end_dte,
to_timestamp(rdt.start_dte) as start_dte,  
to_timestamp(rdt.closed_won_dte) as closed_won_dte,
to_timestamp(rdt.start_dte_month) as start_dte_month,
to_timestamp(rdt.end_dte_month) as end_dte_month,
to_timestamp(rdt.closed_won_dte_month) as closed_won_dte_month,
rdt.opp_arr,
rdt.opp_net_new_arr,
ls.opp_lead_source
from raw_data_transformed as rdt
left join lead_source as ls on (rdt.opp_id = ls.opp_id)
order by rdt.account_id, start_dte asc
),

fy_dates as (
select distinct
dte,
month,
day_of_year,
day_of_qtr,
fy_quarter,
fy_year
from "DEV"."MARTS"."FY_CALENDAR"
where to_date(dte) >= '2016-01-01'
),

fct_sourced as (
select distinct 
account_id,
account_name,
opp_id,
opp_name,
opp_revenue_type,
end_dte,
start_dte,  
closed_won_dte,
start_dte_month,
end_dte_month,
closed_won_dte_month,
opp_arr,
opp_net_new_arr,
opp_lead_source,
fy.dte,
fy.month,
fy.day_of_qtr,
fy.fy_quarter,
fy.fy_year
from fct_sourced_int
right join fy_dates as fy on (to_date(closed_won_dte) = to_date(fy.dte))
order by fy.dte asc
),

fy_agg_int as (
select distinct
fy_year, 
dte,
opp_lead_source,
sum(opp_net_new_arr) as net_new_arr,
sum(opp_arr) as total_arr,
count(distinct opp_id) as num_opps
from fct_sourced
group by opp_lead_source, dte, fy_year
order by dte asc
),

fy_agg as (
select 
fy_year,
to_timestamp(dte) as dte,
opp_lead_source,
ZEROIFNULL(sum(net_new_arr) over (partition by fy_year, opp_lead_source order by dte asc rows between unbounded preceding and current row)) as net_new_arr,
ZEROIFNULL(sum(total_arr) over (partition by fy_year, opp_lead_source order by dte asc rows between unbounded preceding and current row)) as total_arr,
ZEROIFNULL(sum(num_opps) over (partition by fy_year, opp_lead_source order by dte asc rows between unbounded preceding and current row)) as num_opps
from fy_agg_int 
where to_date(dte) <= to_date(current_date())
order by fy_year asc, dte asc, opp_lead_source
)

select * from fy_agg
