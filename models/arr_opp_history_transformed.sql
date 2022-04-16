{{ config
(
    materialized='table',
    database = 'PROD',
    schema = 'MONTHLY_KPIS'
)
}}

/* Pulling in raw SFDC data from arr_opp_history model*/
with raw_data as (
select * from {{ref('arr_opp_history')}}
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
     when opp_id = '0061R00000zAlU8QAK' then to_date('2022-03-15') /* Opportunity churned in March, per Kristen. AMEX 323k */
     when opp_id = '0061R0000136hnzQAA' then to_date('2022-02-15') /* Customer churned in Feb. per Kristen. AXA Churn. 35k */ 
     when opp_id = '0061R000014vAD7QAM' then to_date('2023-02-15') /* Adjusting end date because it is incorrect in SFDC */
     when opp_id = '0061R00000r7xPhQAI' then to_date('2022-02-15') /* Customer churned. Close this out in Feb. per FP&A. DISA 64.3k */ 
     when opp_Id = '0061R0000137tYlQAI' then to_date('2022-03-15') /* Customer churned. Close this out in Mar. per FP&A. Record Connect 239k */
     when opp_id = '0061R0000137kNxQAI' then to_date('2022-04-15') /* Customer Churned in April per FP&A. State of Texas 402.5k total. 17.5k opp */
     when opp_id = '0061R00000zD2sxQAC' then to_date('2022-05-15') /* End date adjustment because of open negotiations. Conduent 1.98M */
     when opp_id = '0061R0000137hQzQAI' then to_date('2022-10-15') /* Adjusting end date by 1 month per FP&A. Allstate 15k */
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
     ELSE opp_net_new_arr end as opp_net_new_arr,
opp_is_marketing_influenced_flag
from raw_data
where opp_id not in ('00636000003gG2qAAE','0063600000W0NhNAAV','0063600000SKDdAAAX','0061R00000m1g4KQAQ','0063600000X36vUAAR') /*removing these ops per FP&A */
)

select * from raw_data_transformed