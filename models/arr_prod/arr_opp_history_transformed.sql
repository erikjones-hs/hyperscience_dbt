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
CASE WHEN account_id = '0011R000026iP6rQAE' then '0013600001iRke2AAC' 
     else account_id end as account_id,
CASE WHEN account_name = 'TD Ameritrade' then 'Charles Schwab' 
     WHEN account_name = '8053580156557' then 'Department of Justice' 
     WHEN account_name = '8780895197581' then 'Mathematica, Inc.'
     WHEN account_name = 'Tokio Marine HCC' then 'Philadelphia Insurance Companies'
     WHEN account_name = 'Great American Insurance Group' then 'Great American Insurance Company'
     else account_name end as account_name,
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
     WHEN opp_id = '0061R00000yFonNQAS' then to_date('2023-11-15') /* Adjusting the end date for MetaSource because they are still a customer, but with no committed revenue */
     WHEN opp_id = '0061R00000zAI8KQAW' then to_date('2023-11-15') /* Adjusting the end date for Virginia DMV because they are still a customer, but with no committed revenue */
     WHEN opp_id = '0061R00000yEQVgQAO' then to_date('2023-11-15') /* Adjusting the end date for GDIT-VA because they are still a customer, but with no committed revenue */
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
     WHEN opp_id = '0061R00000uL8ylQAC' then to_date('2023-11-15') /* Adjusting the end date for PMP because they are still a customer, but with a 1 year free contract period */
     WHEN opp_id = '0061R00000zAjoeQAC' then to_date('2021-10-15')
     WHEN opp_id = '0061R000010PVABQA4' then to_date('2021-10-15')
     WHEN opp_id = '0061R00000zBqNRQA0' then to_date('2021-11-15')
     WHEN opp_id = '0061R000013fGLrQAM' then to_date('2022-12-18')
     WHEN opp_id = '0061R00000wJkMuQAK' then to_date('2021-12-15')
     when opp_id = '0061R0000137saUQAQ' then to_date('2022-06-15') 
     WHEN opp_id = '0061R000013fGTbQAM' then to_date('2022-11-23') /* Updated end date because it is incorect in SFDC. divvyDose 180k */
     WHEN opp_id = '0061R00000zD2sTQAS' then to_date('2021-12-15') /* End date adjusted per Kristen and Finance ARR Google Sheet. Conduent 280k */ 
     WHEN opp_id = '0061R000010t71kQAA' then to_date('2022-01-15') /* Customer no longer is paying. Close this out in Jan. per FP&A. Sience SAS 41.65k */
     when opp_id = '0061R000014uXZrQAM' then to_date('2023-01-25') /* Updated MPOWER end date because it is incorrect in SFDC */
     when opp_id = '0061R00000yElHXQA0' then to_date('2022-02-15') /* Customer Churned in Feb, per FP&A. Department of Treasury 87.5k */
     when opp_id = '0061R00000zAlU8QAK' then to_date('2022-03-15') /* Opportunity churned in March, per Kristen. AMEX 323k */
     when opp_id = '0061R0000136hnzQAA' then to_date('2022-02-15') /* Customer churned in Feb. per Kristen. AXA Churn. 35k */ 
     when opp_id = '0061R000014vAD7QAM' then to_date('2023-02-15') /* Adjusting end date because it is incorrect in SFDC */
     when opp_id = '0061R00000r7xPhQAI' then to_date('2022-02-15') /* Customer churned. Close this out in Feb. per FP&A. DISA 64.3k */ 
     when opp_Id = '0061R0000137tYlQAI' then to_date('2022-03-15') /* Customer churned. Close this out in Mar. per FP&A. Record Connect 239k */
     when opp_id = '0061R0000137kNxQAI' then to_date('2022-04-15') /* Customer Churned in April per FP&A. State of Texas 402.5k total. 17.5k opp */
     when opp_id = '0061R000010ujZ5QAI' then to_date('2022-06-15') /* Adjusting end date because of new contract with expansion that starts in June */
     when opp_id = '0061R00000zD2sxQAC' then to_date('2024-03-15') /* End date adjustment because renewal date is incorrect in SFDC. Conduent 1.98M */
     when opp_id = '0061R0000137ijiQAA' then to_date('2022-06-29') /* End date adjustment due to negotiated end of contract. Johnson Law Group 25k */
     when opp_id = '0061R0000137jqkQAA' then to_date('2022-08-19') /* Adjusting end date because wrong in Salesforce. QAI 35k */
     when opp_id = '0061R00000zDCt9QAG' then to_date('2024-08-24') /* End date adjustment because renewal date was wrong in snapshot */
     when opp_id = '0061R000010QadCQAS' then to_date('2027-03-15') /* End date adjustment to account for amended contract. Philadelphia Insureance Company 300k */
     when opp_id = '0061R0000137jsqQAA' then to_date('2022-08-15') /* Adjusting End Date for historical accuracy. Pac Life 330k */
     when opp_id = '0061R000010O65hQAC' then to_date('2022-08-15') /* Adjusting End Date for historical accuracy. First American Financial 1M */
     when opp_id = '0061R0000137hQzQAI' then to_date('2022-10-15') /* End date adjustment for historical accuracy. Allstate 15k */
     when opp_id = '0061R00001A4pwsQAB' then to_date('2023-10-29') /* End date adjustment because it is wrong in SFDC. Ascensus 216k */
     when opp_id = '0061R000010OgSrQAK' then to_date('2022-11-15') /* End date adjustment for historical accuracy. GAIG 180k */
     when opp_id = '0061R000013fHgQQAU' then to_date('2022-10-15') /* End date adjustment for historical accuracy. IRS phase 2 */
     when opp_id = '0061R0000137hOKQAY' then to_date('2022-12-15') /* End date adjustment because of open negotiations. SSA DeDupe 1.9M */
     when opp_id = '0061R000013flkIQAQ' then to_date('2022-12-15') /* End date adjustment because of open negotiations. VBA IBM 2.3M */
     when opp_id = '0061R0000137by9QAA' then to_date('2022-12-15') /* End date adjustment because of open negotiations. AIG 555k */
     when opp_id = '0061R000010tH9RQAU' then to_date('2022-12-15') /* End date adjustment because of open negotiations. VA VICCS 1.2M */
     when opp_id = '0061R0000137kdCQAQ' then to_date('2022-12-15') /* End date adjustment because of open negotiations. Unum 625k */
     when opp_id = '0061R0000135gO1QAI' then to_date('2022-12-15') /* End date adjustment because of open negotiations. Accerta 89.4k */
     when opp_id = '0061R000014wIeUQAU' then to_date('2022-12-15') /* End date adjustment because of open negotiations. SSA W2 950k */
     ELSE end_dte_raw end as end_dte,
end_dte_raw,
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
     when opp_id = '0061R000014wI4qQAE' then to_date('2022-06-01')
     when opp_id = '0061R000016my8LQAQ' then to_date('2022-06-15')
     when opp_id = '0061R000016jsHbQAI' then to_date('2022-07-15')
     when opp_id = '0061R00001A3ujGQAR' then to_date('2022-03-15')
     ELSE start_dte_raw end as start_dte,
closed_won_dte,
date_trunc('month',to_date(start_dte)) as start_dte_month,
date_trunc('month',to_date(end_dte)) as end_dte_month,
date_trunc('month',to_date(end_dte_raw)) as end_dte_raw_month,
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
     when opp_id = '0061R00001A6F76QAF' then 15000
     when opp_id = '0061R00001BAPkAQAX' then 330000
     ELSE opp_arr end as opp_arr,
CASE WHEN opp_id = '0061R0000135gO1QAI' then 5040 
     WHEN opp_id = '0061R000014xeQwQAI' then 13269
     WHEN opp_id = '0061R00001A6F76QAF' then 15000
     when opp_id = '0061R00001BAPkAQAX' then 150000
     ELSE opp_net_new_arr end as opp_net_new_arr,
opp_is_marketing_influenced_flag
from raw_data
where opp_id not in ('00636000003gG2qAAE','0063600000W0NhNAAV','0063600000SKDdAAAX','0061R00000m1g4KQAQ','0063600000X36vUAAR') /*removing these ops per FP&A */
)

select * from raw_data_transformed where opp_id not in 
(
'0061R000016my8LQAQ', /* Tokio Marine Deal 420k. Removed due to out clause */
'0061R000016jsHbQAI', /* Utilize Core 54k. Removed due to out clause */
'0061R000010QadCQAS', /* Original Tokio Marine Deal with Philly Insuarnce (replaced by amended opp for Philly Insurance) */
'0061R000014wNsNQAU', /* Data Dimensions 640k. Removing because they opted out of their auto-renewal */
'0061R00001A3TIAQA3' /* Vida Capital 1.6k. Was an NRR Deal. Should have not been in here as ARR */
)