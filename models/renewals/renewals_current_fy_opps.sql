{{ config
(
    materialized='table',
    database = 'PROD',
    schema = 'RENEWALS'
)
}}

with renewal_opps as (
select * from {{ref('fct_renewals')}}
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
and opp_id not in ('006Dm0000046tabIAA')
and is_deleted = false
),

open_opps as (
select * 
from open_opps_int
where prior_opp_id in (select distinct existing_opp_id from renewal_opps) 
OR prior_opp_id = '0061R000013eo6oQAA' 
and opp_id not in ('0061R000014wNrwQAE','006Dm0000046tabIAA')
),

combined_opps as (
select distinct
ro.renewal_month,
ro.account_id,
ro.account_name,
ro.existing_opp_id,
ro.existing_opp_name,
CASE WHEN ro.existing_opp_id in ('0061R00000zAI8KQAW','0061R00000r6r1iQAA') then 0 
     WHEN ro.existing_opp_id = '0061R000014wI4hQAE' then 20000 /* adjusting for peerstreet arr */
     else ro.potential_churn_amount end as potential_churn_amount,
ro.existing_opp_renewal_date,
ro.has_churned_flag,
ro.outstanding_renewal_flag,
ro.upcoming_renewal_flag,
oo.opp_id as open_opp_id,
oo.opp_name as open_opp_name,
oo.opp_arr as open_opp_arr,
oo.opp_net_new_arr as open_opp_net_new_arr_raw,
oo.opp_close_dte as open_opp_close_dte,
oo.opp_stage_name,
oo.opp_commit_status as open_opp_commit_status,
CASE WHEN ro.existing_opp_id in ('0061R00000zAI8KQAW','0061R00000r6r1iQAA') then 0
     WHEN ro.existing_opp_id = '0061R000014wI4hQAE' then 20000 /* adjusting for peerstreet arr */
     WHEN open_opp_id IS NULL then (-1)*potential_churn_amount 
     else ((-1)*potential_churn_amount + open_opp_arr) end as net_new_arr,
CASE WHEN net_new_arr > 0 then 'expansion'
     WHEN net_new_arr < 0 and open_opp_id IS NULL then 'no open opp'
     WHEN net_new_arr < 0 and open_opp_id IS NOT NULL then 'churn'
     WHEN net_new_arr = 0 then 'flat'
     ELSE 'other' end as renewal_type
from renewal_opps as ro
left join open_opps as oo on (ro.existing_opp_id = oo.prior_opp_id)
where existing_opp_id not in ('0061R00000yEQVgQAO', /* Removing BenefitMall because open opp associated with usell opp */
                              '0061R00000zAuShQAK', /* Removing GDVIT - VA because renewal bucketd with larger new opp */
                              '0061R00000uINyXQAW') /* Can remove this opp in July, once Fidelity upsell goes live */
order by renewal_month asc
)

select * from combined_opps