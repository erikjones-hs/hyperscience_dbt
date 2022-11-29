{{ config
(
    materialized='table',
    database = 'PROD',
    schema = 'RENEWALS'
)
}}

with renewals as (
select *,
CASE WHEN opp_category = 'churn' and customer_category = 'active' then 1 else 0 end as renewal_month,
max(renewal_month) over (partition by account_id) as has_renewed,
sum(renewal_month) over (partition by account_id) as num_renewals
from {{ ref('fct_arr_opp') }} 
order by account_id, date_month asc
), 

churn as (
select distinct
date_month,
account_id,
account_name,
opp_id,
opp_name
from {{ ref('fct_arr_opp') }} 
where customer_category = 'churn'
and to_date(date_month) <= date_trunc(month,to_date(current_date()))
order by date_month asc
),

renewal_opps as (
select distinct
date_month,
account_id,
account_name,
opp_id,
opp_name,
CASE WHEN opp_id = '0061R000016kGCyQAM' then -90000
     WHEN opp_id = '0061R000014xeQwQAI' then -183115.38
     WHEN opp_id = '0061R00000r6r1iQAA' then -375000
     WHEN opp_id = '0061R00000zAI8KQAW' then -43225
     WHEN opp_id = '0061R000014wNrpQAE' then -35000.001
     else mrr_change end as mrr_change,
CASE WHEN opp_id = '0061R0000137hOKQAY' then to_date('2022-09-30') /* Adjusting end date because it is wrong in SFDC. SSA DEDupe 19.M */
     WHEN opp_id = '0061R000013fGTbQAM' then to_date('2022-11-23') /* Adjusting end date because it is wrong in SFDC. DivvyDose 180k */ 
     WHEN opp_id = '0061R00000r6r1iQAA' then to_date('2023-11-15') /* Adjusting the end date for CRL because they are still a customer, but with no committed revenue */
     WHEN opp_id = '0061R00000yFonNQAS' then to_date('2023-11-15') /* Adjusting the end date for MetaSource because they are still a customer, but with no committed revenue */
     WHEN opp_id = '0061R00000zAI8KQAW' then to_date('2023-11-15') /* Adjusting the end date for Virginia DMV because they are still a customer, but with no committed revenue */
     WHEN opp_id = '0061R00000yEQVgQAO' then to_date('2023-11-15') /* Adjusting the end date for GDIT-VA because they are still a customer, but with no committed revenue */
     WHEN opp_id = '0061R00000uL8ylQAC' then to_date('2023-11-15') /* Adjusting the end date for PMP because they are still a customer, but with a 1 year free contract period */
     When opp_id = '0061R000014uXZrQAM' then to_date('2023-01-25') /* Updated MPOWER end date because it is incorrect in SFDC */
     when opp_id = '0061R00000zD2sxQAC' then to_date('2024-03-15') /* End date adjustment because renewal date is incorrect in SFDC. Conduent 1.98M */
     when opp_id = '0061R00000zDCt9QAG' then to_date('2024-08-24') /* End date adjustment because renewal date was wrong in snapshot */
     when opp_id = '0061R000010QadCQAS' then to_date('2027-03-15') /* End date adjustment to account for amended contract. Philadelphia Insureance Company 300k */
     when opp_id = '0061R00001A4pwsQAB' then to_date('2023-10-29') /* End date adjustment because it is wrong in SFDC. Ascensus 216k */
     when opp_id = '0061R000010OgSrQAK' then to_date('2022-11-15') /* End date adjustment for historical accuracy. GAIG 180k */
     when opp_id = '0061R000013fHgQQAU' then to_date('2022-10-15') /* End date adjustment for historical accuracy. IRS phase 2 */
     when opp_id = '0061R0000137hOKQAY' then to_date('2022-09-15') /* End date adjustment for historical accuracy. SSA DeDupe 1.9M */
     when opp_id = '0061R000013flkIQAQ' then to_date('2022-10-15') /* End date adjustment for historical accuracy. VBA IBM 2.3M */
     when opp_id = '0061R000010tH9RQAU' then to_date('2022-10-15') /* End date adjustment for historical accuracy. VA VICCS 1.2M */
     when opp_id = '0061R00001A4pwYQAR' then to_date('2023-10-29') /* End date adjustment because end date is incorrect in SFDC. Unum Group 690k */
     when opp_id = '0061R000013gijQQAQ' then to_date('2022-11-15') /* End Date Adjustment per FP&A. Not paying. MindMap 150k */
     when opp_id = '0061R000016mzrWQAQ' then to_date('2022-11-15') /* End Date Adjustment per FP&A. Not paying. Featsystems 60k */
     when opp_id = '0061R0000137scfQAA' then to_date('2022-11-15') /* End Date Adjustment per FP&A. Not Paying. Cogent 95k */ 
     when opp_id = '0061R000014wIeUQAU' then to_date('2022-10-15') /* End date adjustment for historical accuracy. SSA W2 950k */
     when opp_id = '0061R00001A4rKQQAZ' then to_date('2023-11-30') /* End date adjustment because it is wrong in SFDC. Kovack 35k */
     ELSE end_dte_raw end as end_dte
from {{ ref('fct_arr_opp_renewals') }}
where opp_category = 'churn'
and to_date(date_month) >= date_trunc('month',to_date(current_date()))
and to_date(date_month) <= '2024-02-01'
and opp_id not in (select opp_id from renewals where renewal_month = 1 and to_date(date_month) <= date_trunc(month,to_date(current_date())))
and opp_id not in (select opp_id from churn)
and opp_id not in (
'0061R00000zAuShQAK', /* Removing extra BenefitMall opp. Relying instead on Upsell opp */ 
'0061R000013fFwbQAE', /* Early Renewal. Federated Mutual Insurance */
'0061R000010OgSrQAK', /* Early Renewal Great American Insurance */
'0061R000014v6D3QAI', /* Early Renewal for Aviso Wealth */
'0061R000013fGTbQAM', /* Early Renewal for divvyDOSE */
'0061R000014uygDQAQ'. /* Early Renewal for Kovack */
)
order by end_dte asc
),

open_opps_int as (
select distinct
opp_id,
opp_name,
account_id,
account_name,
CASE WHEN opp_id = '0061R00001BAugdQAD' then 0.00000000001
     WHEN opp_id = '0061R000014wNrVQAU' then 250000
     WHEN opp_id = '0061R000014wNrmQAE' then 0.00000000001
     WHEN opp_id = '0061R00001A644eQAB' then 375000.003
     WHEN opp_id = '0061R00001BAPkAQAX' then 330000
     else opp_arr end as opp_arr,
CASE WHEN opp_id = '0061R00001A6iWpQAJ' then 0 
     WHEN opp_id = '0061R000014wI4cQAE' then 0
     else opp_net_new_arr end as opp_net_new_arr,
opp_stage_name,
opp_commit_status,
CASE WHEN opp_id = '0061R00000yGqH3QAK' then '0061R000014wIeUQAU' 
     WHEN opp_id = '0061R00001A6iWpQAJ' then '0061R000016kGCyQAM'
     when opp_id = '0061R000014wNrVQAU' then '0061R00000yFci4QAC'
     when opp_id = '0061R00001BAugdQAD' then '0061R00000yFonNQAS'
     else prior_opp_id end as prior_opp_id,
opp_close_dte
from "DEV"."SALES"."SALESFORCE_AGG_OPPORTUNITY"
where opp_stage_name not in ('Closed Won','Opp DQed','Closed Lost')
and is_deleted = false
),

open_opps as (
select * 
from open_opps_int
where prior_opp_id in (select distinct opp_id from renewal_opps) 
OR prior_opp_id = '0061R000013eo6oQAA' 
and opp_id not in ('0061R000014wNrwQAE')
),

combined_opps as (
select distinct
ro.date_month as renewal_month,
ro.account_id,
ro.account_name,
ro.opp_id as existing_opp_id,
ro.opp_name as existing_opp_name,
ro.mrr_change as potential_churn_amount,
ro.end_dte as existing_opp_renewal_date,
oo.opp_id as open_opp_id,
oo.opp_name as open_opp_name,
oo.opp_arr as open_opp_arr,
oo.opp_net_new_arr as open_opp_net_new_arr_raw,
oo.opp_close_dte as open_opp_close_dte,
oo.opp_stage_name,
oo.opp_commit_status as open_opp_commit_status,
CASE WHEN open_opp_id IS NULL then potential_churn_amount else (potential_churn_amount + open_opp_arr) end as net_new_arr,
CASE WHEN net_new_arr > 0 then 'expansion'
     WHEN net_new_arr < 0 and open_opp_id IS NULL then 'no open opp'
     WHEN net_new_arr < 0 and open_opp_id IS NOT NULL then 'churn'
     WHEN net_new_arr = 0 then 'flat'
     ELSE 'other' end as renewal_type
from renewal_opps as ro
left join open_opps as oo on (ro.opp_id = oo.prior_opp_id)
order by date_month asc
)

select * from combined_opps